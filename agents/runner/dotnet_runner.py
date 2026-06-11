"""DotNet Runner — מבצע DotNetExecutableTestCase: Kafka publish/wait + Couchbase wait.

תלוי ב-confluent-kafka + couchbase Python SDK. אם הם לא מותקנים (JFrog blocked),
ה-runner מחזיר BLOCKED עם הסבר ברור — לא קורס את הפייפליין.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, Dict, List, Optional

from config.logging_config import get_logger
from config.settings import settings
from models.dotnet_test_case import (
    CouchbaseWaitAction,
    DotNetExecutableTestCase,
    KafkaPublishAction,
    KafkaWaitAction,
)
from models.test_case import StepResult, TestCaseResult, TestStatus

log = get_logger(__name__)


class DotNetRunner:
    name = "dotnet"

    async def execute(self, executable: DotNetExecutableTestCase) -> TestCaseResult:
        """מבצע את רצף ה-actions אחד אחרי השני. status סופי הוא AND של כולם."""
        if not executable.actions:
            return TestCaseResult(
                test_case_id=executable.test_case_id,
                ado_test_case_id=executable.ado_test_case_id,
                status=TestStatus.BLOCKED,
                step_results=[],
                duration_seconds=0.0,
                api_response={"error": executable.compiler_notes or "no actions to execute"},
            )

        started = time.perf_counter()
        step_results: List[StepResult] = []
        observations: List[Dict[str, Any]] = []
        overall_status = TestStatus.PASSED
        error_message: Optional[str] = None

        for action in executable.actions:
            try:
                if isinstance(action, KafkaPublishAction):
                    step, obs = await self._run_kafka_publish(action)
                elif isinstance(action, KafkaWaitAction):
                    step, obs = await self._run_kafka_wait(action)
                elif isinstance(action, CouchbaseWaitAction):
                    step, obs = await self._run_couchbase_wait(action)
                else:
                    step = StepResult(
                        step=f"unknown action: {getattr(action, 'kind', '?')}",
                        expected_result="known action",
                        actual_result="skipped",
                        status=TestStatus.BLOCKED,
                        error_message="unknown action kind",
                    )
                    obs = {"skipped": True}
            except Exception as e:
                log.warning("dotnet_action_exception", kind=getattr(action, "kind", "?"), error=str(e))
                step = StepResult(
                    step=f"{getattr(action, 'kind', '?')}",
                    expected_result="action runs",
                    actual_result=f"Exception: {str(e)[:200]}",
                    status=TestStatus.BLOCKED,
                    error_message=str(e),
                )
                obs = {"error": str(e), "kind": getattr(action, "kind", "?")}

            step_results.append(step)
            observations.append({"action": action.model_dump(), "observation": obs})

            if step.status == TestStatus.FAILED:
                overall_status = TestStatus.FAILED
                error_message = error_message or step.error_message
            elif step.status == TestStatus.BLOCKED and overall_status != TestStatus.FAILED:
                overall_status = TestStatus.BLOCKED
                error_message = error_message or step.error_message

        duration = time.perf_counter() - started

        # api_response בפורמט generic — UI יודע לקרוא {actions, observations}
        api_response: Dict[str, Any] = {
            "status": 200 if overall_status == TestStatus.PASSED else 0,
            "kind": "dotnet",
            "observations": observations,
            "duration_ms": int(duration * 1000),
        }
        if error_message:
            api_response["error"] = error_message

        return TestCaseResult(
            test_case_id=executable.test_case_id,
            ado_test_case_id=executable.ado_test_case_id,
            status=overall_status,
            step_results=step_results,
            duration_seconds=duration,
            api_response=api_response,
        )

    # ============================================================
    # Kafka publish
    # ============================================================

    async def _run_kafka_publish(self, action: KafkaPublishAction):
        if not settings.kafka_enabled:
            return self._blocked_step(
                f"PUBLISH topic={action.topic}",
                "Kafka not configured (KAFKA_BOOTSTRAP_SERVERS empty)",
            )

        try:
            from confluent_kafka import Producer  # type: ignore[import-not-found]
        except ImportError:
            return self._blocked_step(
                f"PUBLISH topic={action.topic}",
                "confluent-kafka package not installed",
            )

        conf = self._kafka_conf()
        producer = Producer(conf)
        value_bytes = self._encode_value(action.value)
        key_bytes = action.key.encode("utf-8") if action.key else None
        headers = (
            [(k, v.encode("utf-8")) for k, v in action.headers.items()]
            if action.headers
            else None
        )

        delivery_result: Dict[str, Any] = {}

        def _on_delivery(err, msg):
            if err is not None:
                delivery_result["error"] = str(err)
            else:
                delivery_result["topic"] = msg.topic()
                delivery_result["partition"] = msg.partition()
                delivery_result["offset"] = msg.offset()

        producer.produce(
            topic=action.topic,
            value=value_bytes,
            key=key_bytes,
            headers=headers,
            on_delivery=_on_delivery,
        )
        # flush בעטיפת asyncio.to_thread כדי לא לחסום את ה-event loop
        await asyncio.to_thread(producer.flush, 10)

        if "error" in delivery_result:
            step = StepResult(
                step=f"PUBLISH topic={action.topic}",
                expected_result="delivered",
                actual_result=f"error: {delivery_result['error']}",
                status=TestStatus.FAILED,
                error_message=delivery_result["error"],
            )
            return step, delivery_result

        step = StepResult(
            step=f"PUBLISH topic={action.topic}",
            expected_result="delivered",
            actual_result=f"offset={delivery_result.get('offset')}",
            status=TestStatus.PASSED,
            response_dump=delivery_result,
        )
        return step, delivery_result

    # ============================================================
    # Kafka wait
    # ============================================================

    async def _run_kafka_wait(self, action: KafkaWaitAction):
        if not settings.kafka_enabled:
            return self._blocked_step(
                f"WAIT topic={action.topic}",
                "Kafka not configured (KAFKA_BOOTSTRAP_SERVERS empty)",
            )

        try:
            from confluent_kafka import Consumer  # type: ignore[import-not-found]
        except ImportError:
            return self._blocked_step(
                f"WAIT topic={action.topic}",
                "confluent-kafka package not installed",
            )

        group_id = f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}-{uuid.uuid4().hex[:8]}"
        conf = self._kafka_conf()
        conf.update({
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })

        observed = await asyncio.to_thread(
            self._consume_until_match,
            Consumer, conf, action.topic, action.match, action.timeout_seconds,
        )

        if observed is None:
            step = StepResult(
                step=f"WAIT topic={action.topic} (timeout {action.timeout_seconds}s)",
                expected_result="message arrived matching " + json.dumps(action.match, ensure_ascii=False),
                actual_result="timeout — no matching message",
                status=TestStatus.FAILED,
                error_message="message did not arrive within timeout",
            )
            return step, {"timeout": True, "match": action.match}

        # אסרשנים על שדות צפויים
        missing = _check_expected_fields(observed.get("value_parsed") or {}, action.expected_fields)
        if missing:
            step = StepResult(
                step=f"WAIT topic={action.topic}",
                expected_result=json.dumps(action.expected_fields, ensure_ascii=False),
                actual_result=json.dumps(observed.get("value_parsed") or {}, ensure_ascii=False)[:300],
                status=TestStatus.FAILED,
                error_message="missing/mismatched fields: " + ", ".join(missing),
                response_dump=observed,
            )
            return step, observed

        step = StepResult(
            step=f"WAIT topic={action.topic}",
            expected_result="message matched + fields ok",
            actual_result=f"offset={observed.get('offset')} fields_ok",
            status=TestStatus.PASSED,
            response_dump=observed,
        )
        return step, observed

    @staticmethod
    def _consume_until_match(
        Consumer,
        conf: Dict[str, Any],
        topic: str,
        match: Dict[str, Any],
        timeout_seconds: int,
    ) -> Optional[Dict[str, Any]]:
        """סינכרוני — רץ ב-thread. polling עד שמתאים או timeout."""
        consumer = Consumer(conf)
        try:
            consumer.subscribe([topic])
            deadline = time.monotonic() + timeout_seconds
            while time.monotonic() < deadline:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    continue
                raw_value = msg.value()
                try:
                    parsed = json.loads(raw_value.decode("utf-8")) if raw_value else None
                except Exception:
                    parsed = None
                if not _matches(parsed, match):
                    continue
                return {
                    "offset": msg.offset(),
                    "partition": msg.partition(),
                    "topic": msg.topic(),
                    "value_parsed": parsed,
                    "value_raw": raw_value.decode("utf-8", errors="replace") if raw_value else None,
                    "timestamp": msg.timestamp(),
                }
            return None
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    # ============================================================
    # Couchbase wait
    # ============================================================

    async def _run_couchbase_wait(self, action: CouchbaseWaitAction):
        if not settings.couchbase_enabled:
            return self._blocked_step(
                f"COUCHBASE bucket={action.bucket}",
                "Couchbase not configured (COUCHBASE_CONNECTION_STRING empty)",
            )

        try:
            from couchbase.auth import PasswordAuthenticator  # type: ignore[import-not-found]
            from couchbase.cluster import Cluster  # type: ignore[import-not-found]
            from couchbase.options import ClusterOptions  # type: ignore[import-not-found]
        except ImportError:
            return self._blocked_step(
                f"COUCHBASE bucket={action.bucket}",
                "couchbase package not installed",
            )

        observed = await asyncio.to_thread(
            self._poll_couchbase,
            Cluster, ClusterOptions, PasswordAuthenticator, action,
        )

        if observed is None:
            step = StepResult(
                step=f"COUCHBASE bucket={action.bucket} key={action.key} (timeout {action.timeout_seconds}s)",
                expected_result="document exists",
                actual_result="timeout — no document",
                status=TestStatus.FAILED,
                error_message="document did not appear within timeout",
            )
            return step, {"timeout": True}

        missing = _check_expected_fields(observed.get("doc") or {}, action.expected_fields)
        if missing:
            step = StepResult(
                step=f"COUCHBASE bucket={action.bucket} key={action.key}",
                expected_result=json.dumps(action.expected_fields, ensure_ascii=False),
                actual_result=json.dumps(observed.get("doc") or {}, ensure_ascii=False)[:300],
                status=TestStatus.FAILED,
                error_message="missing/mismatched fields: " + ", ".join(missing),
                response_dump=observed,
            )
            return step, observed

        step = StepResult(
            step=f"COUCHBASE bucket={action.bucket} key={action.key}",
            expected_result="doc exists + fields ok",
            actual_result="ok",
            status=TestStatus.PASSED,
            response_dump=observed,
        )
        return step, observed

    @staticmethod
    def _poll_couchbase(Cluster, ClusterOptions, PasswordAuthenticator, action: CouchbaseWaitAction):
        """סינכרוני — רץ ב-thread."""
        auth = PasswordAuthenticator(settings.COUCHBASE_USERNAME or "", settings.COUCHBASE_PASSWORD or "")
        cluster = Cluster(settings.COUCHBASE_CONNECTION_STRING, ClusterOptions(auth))
        try:
            bucket = cluster.bucket(action.bucket)
            if action.scope and action.collection:
                coll = bucket.scope(action.scope).collection(action.collection)
            else:
                coll = bucket.default_collection()
            deadline = time.monotonic() + action.timeout_seconds
            last_error = None
            while time.monotonic() < deadline:
                if action.key:
                    try:
                        result = coll.get(action.key)
                        doc = result.content_as[dict]
                        return {"key": action.key, "doc": doc}
                    except Exception as e:
                        last_error = str(e)
                        time.sleep(1.0)
                        continue
                if action.query:
                    try:
                        rows = list(cluster.query(action.query))
                        if rows:
                            return {"query": action.query, "doc": rows[0]}
                    except Exception as e:
                        last_error = str(e)
                    time.sleep(1.0)
            return None
        finally:
            try:
                cluster.close()
            except Exception:
                pass

    # ============================================================
    # Helpers
    # ============================================================

    @staticmethod
    def _kafka_conf() -> Dict[str, Any]:
        conf: Dict[str, Any] = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "security.protocol": settings.KAFKA_SECURITY_PROTOCOL,
        }
        if settings.KAFKA_SECURITY_PROTOCOL.startswith("SASL"):
            conf["sasl.mechanism"] = settings.KAFKA_SASL_MECHANISM
            if settings.KAFKA_SASL_USERNAME:
                conf["sasl.username"] = settings.KAFKA_SASL_USERNAME
            if settings.KAFKA_SASL_PASSWORD:
                conf["sasl.password"] = settings.KAFKA_SASL_PASSWORD
        return conf

    @staticmethod
    def _encode_value(value: Any) -> bytes:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False).encode("utf-8")
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")

    @staticmethod
    def _blocked_step(label: str, reason: str):
        step = StepResult(
            step=label,
            expected_result="action runs",
            actual_result=f"BLOCKED: {reason}",
            status=TestStatus.BLOCKED,
            error_message=reason,
        )
        return step, {"blocked": True, "reason": reason}

    # ============================================================
    # Verify (no-op — Kafka/Couchbase verified inside actions)
    # ============================================================
    async def verify_kafka(self, executable) -> Dict[str, Any]:
        return {"skipped": True, "reason": "verification embedded in actions"}

    async def verify_elastic(self, executable) -> Dict[str, Any]:
        return {"skipped": True, "reason": "verification embedded in actions"}


# ============================================================
# Pure helpers (testable without confluent-kafka)
# ============================================================

def _matches(value: Optional[Dict[str, Any]], match: Dict[str, Any]) -> bool:
    """True אם value מכיל את כל ה-key:value-pairs ב-match."""
    if not match:
        return True
    if not isinstance(value, dict):
        return False
    for k, expected in match.items():
        if k not in value:
            return False
        if value[k] != expected:
            return False
    return True


def _check_expected_fields(value: Dict[str, Any], expected: Dict[str, Any]) -> List[str]:
    """מחזיר רשימה של שדות שחסרים / לא תואמים. ריק = הכל בסדר."""
    issues: List[str] = []
    if not expected:
        return issues
    if not isinstance(value, dict):
        return list(expected.keys())
    for k, want in expected.items():
        if k not in value:
            issues.append(f"{k} (missing)")
            continue
        actual = value[k]
        if str(actual) != str(want):
            issues.append(f"{k}={actual!r}≠{want!r}")
    return issues
