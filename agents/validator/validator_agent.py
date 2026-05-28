"""ValidatorAgent — מאמת:
- Response: status code + JSONPath schema_assertions
- Kafka: מסר תקין נמצא ב-topic
- Elastic: log entry קיים, אין ERROR-level
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

from config.logging_config import get_logger
from models.executable_test_case import ExecutableTestCase
from models.test_case import TestCase, TestCaseResult, TestStatus
from models.test_run import ValidationResult

# Validator pohל-מקבל TestCase או ExecutableTestCase — שניהם חולקים את אותם שדות assertions
TestCaseLike = Union[TestCase, ExecutableTestCase]

log = get_logger(__name__)

try:
    from jsonpath_ng.ext import parse as jsonpath_parse  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover
    jsonpath_parse = None  # type: ignore[assignment]


class ValidatorAgent:
    async def validate_all(
        self,
        results: List[Tuple[TestCaseLike, TestCaseResult]],
    ) -> List[ValidationResult]:
        out: List[ValidationResult] = []
        for tc, res in results:
            out.append(self.validate_one(tc, res))
        return out

    def validate_one(self, tc: TestCaseLike, res: TestCaseResult) -> ValidationResult:
        failures: List[str] = []
        response_check = self._check_response(tc, res)
        if response_check and not response_check.get("ok", False):
            failures.append(response_check.get("reason", "response check failed"))

        kafka_check = self._check_kafka(tc, res)
        if kafka_check and not kafka_check.get("ok", True):
            failures.append(kafka_check.get("reason", "kafka check failed"))

        elastic_check = self._check_elastic(tc, res)
        if elastic_check and not elastic_check.get("ok", True):
            failures.append(elastic_check.get("reason", "elastic check failed"))

        if res.status == TestStatus.BLOCKED:
            failures.insert(0, "Blocked: " + ((res.api_response or {}).get("error") or "unknown"))
            overall = TestStatus.BLOCKED
        elif failures:
            overall = TestStatus.FAILED
        else:
            overall = TestStatus.PASSED

        return ValidationResult(
            test_case_id=tc.test_case_id,
            ado_test_case_id=tc.ado_test_case_id,
            overall_status=overall,
            response_check=response_check,
            kafka_check=kafka_check,
            elastic_check=elastic_check,
            failure_reasons=failures,
        )

    def _check_response(self, tc: TestCaseLike, res: TestCaseResult) -> Optional[Dict[str, Any]]:
        if not tc.expected_response or not res.api_response:
            return None
        actual_status = res.api_response.get("status")
        expected_status = tc.expected_response.status
        ok = actual_status == expected_status
        check: Dict[str, Any] = {
            "ok": ok,
            "expected_status": expected_status,
            "actual_status": actual_status,
            "assertions": [],
        }
        if not ok:
            check["reason"] = f"Status code {actual_status} ≠ {expected_status}"

        body = res.api_response.get("body")
        for path, expected in (tc.expected_response.schema_assertions or {}).items():
            assertion_result = self._eval_jsonpath(body, path, expected)
            check["assertions"].append(assertion_result)
            if not assertion_result["ok"]:
                ok = False
                check["reason"] = check.get("reason") or f"JSONPath {path} כשל: {assertion_result.get('reason')}"

        check["ok"] = ok
        return check

    @staticmethod
    def _eval_jsonpath(body: Any, path: str, expected: Any) -> Dict[str, Any]:
        if jsonpath_parse is None:
            return {"path": path, "ok": False, "reason": "jsonpath-ng לא מותקן"}
        try:
            expr = jsonpath_parse(path)
            matches = [m.value for m in expr.find(body)]
        except Exception as e:
            return {"path": path, "ok": False, "reason": f"שגיאת parsing: {e}"}
        if not matches:
            return {"path": path, "ok": False, "reason": "לא נמצא"}
        actual = matches[0]
        # התאמה: אם expected זה dict עם 'type' / 'value' — בדיקה ייחודית
        if isinstance(expected, dict):
            if "type" in expected:
                expected_type = expected["type"]
                ok = _matches_type(actual, expected_type)
                return {
                    "path": path,
                    "ok": ok,
                    "actual": actual,
                    "expected_type": expected_type,
                    "reason": None if ok else f"סוג {type(actual).__name__} ≠ {expected_type}",
                }
            if "value" in expected:
                ok = actual == expected["value"]
                return {"path": path, "ok": ok, "actual": actual, "expected": expected["value"]}
        # פשוט — השוואה ישירה
        if isinstance(expected, str) and expected.lower() in {"string", "number", "boolean", "object", "array"}:
            ok = _matches_type(actual, expected.lower())
            return {
                "path": path,
                "ok": ok,
                "actual": actual,
                "expected_type": expected,
                "reason": None if ok else f"סוג {type(actual).__name__} ≠ {expected}",
            }
        ok = actual == expected
        return {"path": path, "ok": ok, "actual": actual, "expected": expected}

    def _check_kafka(self, tc: TestCaseLike, res: TestCaseResult) -> Optional[Dict[str, Any]]:
        if not tc.kafka_assertion:
            return None
        if not res.kafka_result:
            return {"ok": False, "reason": "אימות Kafka לא רץ"}
        if res.kafka_result.get("skipped"):
            return {"ok": True, "skipped": True}
        found = bool(res.kafka_result.get("found"))
        return {
            "ok": found,
            "topic": tc.kafka_assertion.topic,
            "search_term": tc.kafka_assertion.search_term,
            "found": found,
            "reason": None if found else "מסר לא נמצא ב-Kafka",
        }

    def _check_elastic(self, tc: TestCaseLike, res: TestCaseResult) -> Optional[Dict[str, Any]]:
        if not tc.elastic_assertion:
            return None
        if not res.elastic_result:
            return {"ok": False, "reason": "אימות Elastic לא רץ"}
        if res.elastic_result.get("skipped"):
            return {"ok": True, "skipped": True}
        hits = res.elastic_result.get("hits", 0)
        errors = res.elastic_result.get("errors", 0)
        ok = hits > 0 and errors == 0
        reason = None
        if hits == 0:
            reason = "לא נמצאו לוגים"
        elif errors > 0:
            reason = f"נמצאו {errors} לוגי ERROR"
        return {
            "ok": ok,
            "index": tc.elastic_assertion.index,
            "hits": hits,
            "errors": errors,
            "reason": reason,
        }


def _matches_type(value: Any, type_name: str) -> bool:
    type_name = type_name.lower()
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "array":
        return isinstance(value, list)
    if type_name == "object":
        return isinstance(value, dict)
    return False
