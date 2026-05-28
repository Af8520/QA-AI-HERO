"""MockRunner — מדמה ריצה ללא רשת מכבי. מקבל ExecutableTestCase כמו ESBRunner."""

from __future__ import annotations

import random
from typing import Any, Dict

from models.executable_test_case import ExecutableTestCase
from models.test_case import (
    StepResult,
    TestCaseResult,
    TestStatus,
)


class MockRunner:
    name = "mock"

    async def execute(self, executable: ExecutableTestCase) -> TestCaseResult:
        passed = random.random() > 0.3
        status = TestStatus.PASSED if passed else TestStatus.FAILED
        # מחזיר תשובה שתואמת את ה-expected_status אם passed, אחרת 500
        api_status = executable.expected_response.status if passed else 500

        step_results = [
            StepResult(
                step=f"{executable.request.method} {executable.request.url}",
                expected_result=f"HTTP {executable.expected_response.status}",
                actual_result=f"HTTP {api_status}" if passed else "Connection refused",
                status=status,
                error_message=None if passed else "Mock failure",
            )
        ]

        api_response: Dict[str, Any] = {
            "status": api_status,
            "headers": {"Content-Type": "application/json"},
            "body": {"mock": True, "tc": executable.test_case_id, "ok": passed},
            "body_text": '{"mock": true}',
            "duration_ms": random.randint(50, 800),
            "url": executable.request.url,
            "method": executable.request.method,
        }

        return TestCaseResult(
            test_case_id=executable.test_case_id,
            ado_test_case_id=executable.ado_test_case_id,
            status=status,
            step_results=step_results,
            duration_seconds=random.uniform(0.5, 2.5),
            api_response=api_response,
        )

    async def verify_kafka(self, executable: ExecutableTestCase) -> Dict[str, Any]:
        if not executable.kafka_assertion:
            return {"skipped": True}
        return {
            "found": random.random() > 0.2,
            "topic": executable.kafka_assertion.topic,
            "mock": True,
        }

    async def verify_elastic(self, executable: ExecutableTestCase) -> Dict[str, Any]:
        if not executable.elastic_assertion:
            return {"skipped": True}
        ok = random.random() > 0.2
        return {
            "hits": 5 if ok else 0,
            "errors": 0 if ok else 1,
            "mock": True,
        }
