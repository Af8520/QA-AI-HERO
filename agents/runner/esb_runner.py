"""ESB Runner — מבצע ExecutableTestCase מלא + מאמת Kafka/Elastic."""

from __future__ import annotations

import re
import time
from typing import Any, Dict

import httpx

from config.logging_config import get_logger
from config.settings import settings
from models.executable_test_case import ExecutableTestCase
from models.test_case import (
    StepResult,
    TestCaseResult,
    TestStatus,
)

log = get_logger(__name__)

# ============================================================
# Constant headers שמוזרקים לכל קריאת ESB.
# מטרה: לזהות בלוגים של Elastic איזה תסריט הריץ איזה API call.
# ============================================================
_CONST_USER_NAME = "qa-ai-hero"

_TC_ID_PATTERN = re.compile(r"(TC[\s\-_]*\d+)", re.IGNORECASE)


def _extract_tc_short_id(title: str) -> str:
    """מחלץ 'TC-XX' מ-title שעשוי להיות ארוך כמו 'TC-01: הקמת אורח...'.
    אם אין pattern מזוהה — מחזיר את ה-title חתוך ל-32 תווים.
    """
    if not title:
        return "unknown"
    m = _TC_ID_PATTERN.search(title)
    if m:
        return re.sub(r"[\s_]", "-", m.group(1))
    return title[:32]


def _inject_constant_headers(headers: Dict[str, str], test_case_id: str) -> Dict[str, str]:
    """מחזיר עותק של headers עם mac_user_name + mac_user_id מוזרקים.
    אנחנו מזריקים תמיד — גם אם ה-LLM/regex הוסיף מה-spec.
    """
    merged: Dict[str, str] = dict(headers or {})
    merged["mac_user_name"] = _CONST_USER_NAME
    merged["mac_user_id"] = _extract_tc_short_id(test_case_id)
    return merged


class ESBRunner:
    name = "esb"

    async def execute(self, executable: ExecutableTestCase) -> TestCaseResult:
        """מבצע request HTTP מלא לפי ה-ExecutableTestCase. אין mutations."""
        req = executable.request
        if req.url == "about:blank":
            return TestCaseResult(
                test_case_id=executable.test_case_id,
                ado_test_case_id=executable.ado_test_case_id,
                status=TestStatus.BLOCKED,
                step_results=[],
                duration_seconds=0.0,
                api_response={"error": executable.compiler_notes or "no executable request"},
            )

        # ★ הזרקת constant headers — נשמרת לכל הקריאות
        req.headers = _inject_constant_headers(req.headers or {}, executable.test_case_id)

        started = time.perf_counter()
        api_response = await _execute_http(req, settings.HTTP_TIMEOUT_SECONDS, settings.VERIFY_SSL)
        duration = time.perf_counter() - started

        # סטטוס ראשוני — ה-Validator יקבע סופית
        status = TestStatus.PASSED
        error_message = None
        if api_response.get("status") == 0:
            status = TestStatus.BLOCKED
            error_message = api_response.get("error")

        step_results = [
            StepResult(
                step=f"{req.method} {req.url}",
                expected_result=f"HTTP {executable.expected_response.status}",
                actual_result=f"HTTP {api_response.get('status')}",
                status=status,
                error_message=error_message,
                response_dump={
                    "status": api_response.get("status"),
                    "duration_ms": api_response.get("duration_ms"),
                    "url": api_response.get("url"),
                },
            )
        ]

        return TestCaseResult(
            test_case_id=executable.test_case_id,
            ado_test_case_id=executable.ado_test_case_id,
            status=status,
            step_results=step_results,
            duration_seconds=duration,
            api_response=api_response,
        )

    async def verify_kafka(self, executable: ExecutableTestCase) -> Dict[str, Any]:
        if not executable.kafka_assertion:
            return {"skipped": True}
        from agents.runner.web_consoles.confluent import verify_kafka_message

        return await verify_kafka_message(
            topic=executable.kafka_assertion.topic,
            search_term=executable.kafka_assertion.search_term,
            expected_value=executable.kafka_assertion.expected_value,
            headless=settings.PLAYWRIGHT_HEADLESS,
        )

    async def verify_elastic(self, executable: ExecutableTestCase) -> Dict[str, Any]:
        if not executable.elastic_assertion:
            return {"skipped": True}
        from agents.runner.web_consoles.kibana import verify_log_entry

        return await verify_log_entry(
            index=executable.elastic_assertion.index,
            query=executable.elastic_assertion.query,
            must_not_contain_level=executable.elastic_assertion.must_not_contain_level,
            headless=settings.PLAYWRIGHT_HEADLESS,
        )


async def _execute_http(
    req,
    timeout_seconds: int,
    verify_ssl: bool,
) -> Dict[str, Any]:
    """מבצע HTTP call לפי HttpRequestSpec ומחזיר dict סטנדרטי."""
    method = req.method.upper()
    url = req.url
    headers = req.headers or {}
    body = req.body

    log.info(
        "esb_request_start",
        method=method,
        url=url,
        header_keys=list(headers.keys()),
        has_body=body is not None,
    )
    started = time.perf_counter()

    async with httpx.AsyncClient(timeout=timeout_seconds, verify=verify_ssl) as client:
        kwargs: Dict[str, Any] = {"headers": headers}
        ct = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
        if isinstance(body, str):
            kwargs["content"] = body.encode("utf-8") if "json" in ct else body
        elif isinstance(body, dict):
            if "json" in ct or not ct:
                kwargs["json"] = body
            else:
                kwargs["data"] = body
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.HTTPError as e:
            duration_ms = int((time.perf_counter() - started) * 1000)
            log.warning("esb_request_failed", url=url, error=str(e))
            return {
                "status": 0,
                "headers": {},
                "body": None,
                "body_text": None,
                "duration_ms": duration_ms,
                "url": url,
                "method": method,
                "error": str(e),
            }

    duration_ms = int((time.perf_counter() - started) * 1000)
    body_text = response.text
    parsed_body: Any
    try:
        parsed_body = response.json()
    except Exception:
        parsed_body = None

    log.info("esb_request_done", method=method, url=url, status=response.status_code, duration_ms=duration_ms)
    return {
        "status": response.status_code,
        "headers": dict(response.headers),
        "body": parsed_body,
        "body_text": body_text,
        "duration_ms": duration_ms,
        "url": url,
        "method": method,
    }
