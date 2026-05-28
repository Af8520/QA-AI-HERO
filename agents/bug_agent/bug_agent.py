"""BugAgent — מנתח כשלים, מציע bug reports עם Azure OpenAI (אופציונלי)."""

from __future__ import annotations

import json
from typing import List, Optional

from config.logging_config import get_logger
from config.settings import settings
from models.bug import BugReport
from models.test_case import TestCase, TestCaseResult
from models.test_run import ValidationResult

log = get_logger(__name__)

BUG_SYSTEM_PROMPT = """אתה QA bug analyst של מכבי.
בהינתן test case שנכשל + תוצאות הריצה, צור bug report בעברית.

החזר JSON בלבד:
{
  "title": "כותרת קצרה (עד 100 תווים)",
  "severity": "Critical|High|Medium|Low",
  "description": "פירוט הכשל",
  "repro_steps": ["צעד 1", "צעד 2"],
  "suggested_fix": "הצעה לתיקון"
}

severity:
- Critical: API מחזיר 5xx, אובדן נתונים, אבטחה
- High: ולידציה כושלת, שדה חובה חסר, Kafka לא רושם
- Medium: log חסר באלסטיק, אזהרות
- Low: cosmetic / format
"""


class BugAgent:
    async def analyze(
        self,
        failures: List[tuple[TestCase, TestCaseResult, ValidationResult]],
    ) -> List[BugReport]:
        out: List[BugReport] = []
        for tc, res, val in failures:
            bug = await self._analyze_one(tc, res, val)
            if bug:
                out.append(bug)
        return out

    async def _analyze_one(
        self,
        tc: TestCase,
        res: TestCaseResult,
        val: ValidationResult,
    ) -> Optional[BugReport]:
        # אם אין Azure OpenAI — fallback פשוט
        if not settings.azure_openai_enabled:
            return self._fallback_bug(tc, val)
        try:
            from openai import AsyncAzureOpenAI  # type: ignore[import-not-found]
            import httpx
        except ImportError:
            return self._fallback_bug(tc, val)

        http_client = httpx.AsyncClient(verify=settings.VERIFY_SSL, timeout=60.0)
        client = AsyncAzureOpenAI(
            api_key=settings.AZURE_OPENAI_KEY,
            api_version=settings.AZURE_OPENAI_API_VERSION,
            azure_endpoint=settings.AZURE_OPENAI_ENDPOINT,
            http_client=http_client,
        )
        payload = {
            "test_case_id": tc.test_case_id,
            "ado_test_case_id": tc.ado_test_case_id,
            "steps": _extract_steps(tc),
            "api_response": _trim(res.api_response),
            "kafka_check": val.kafka_check,
            "elastic_check": val.elastic_check,
            "response_check": val.response_check,
            "failure_reasons": val.failure_reasons,
        }
        try:
            resp = await client.chat.completions.create(
                model=settings.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": BUG_SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
                ],
                response_format={"type": "json_object"},
                temperature=0,
            )
            data = json.loads(resp.choices[0].message.content or "{}")
        except Exception as e:
            log.warning("bug_llm_failed", error=str(e), tc=tc.test_case_id)
            return self._fallback_bug(tc, val)

        return BugReport(
            title=data.get("title") or f"כשל ב-{tc.test_case_id}",
            severity=data.get("severity") or "Medium",
            description=data.get("description") or "; ".join(val.failure_reasons),
            test_case_id=tc.test_case_id,
            ado_test_case_id=tc.ado_test_case_id,
            failure_reasons=val.failure_reasons,
            repro_steps=data.get("repro_steps") or [s.step for s in tc.steps],
            suggested_fix=data.get("suggested_fix"),
        )

    @staticmethod
    def _fallback_bug(tc, val: ValidationResult) -> BugReport:
        return BugReport(
            title=f"כשל ב-{tc.test_case_id}: {(val.failure_reasons or ['unknown'])[0][:80]}",
            severity="High" if any("5" in (r or "") for r in val.failure_reasons) else "Medium",
            description="; ".join(val.failure_reasons) or "כשל לא מזוהה",
            test_case_id=tc.test_case_id,
            ado_test_case_id=tc.ado_test_case_id,
            failure_reasons=val.failure_reasons,
            repro_steps=_extract_steps(tc),
        )


def _extract_steps(tc) -> List[str]:
    """תומך גם ב-TestCase (יש .steps) וגם ב-ExecutableTestCase (יש .request + .source_text)."""
    steps = getattr(tc, "steps", None)
    if steps:
        return [s.step if hasattr(s, "step") else str(s) for s in steps]
    # ExecutableTestCase: בנה צעדים מה-request
    request = getattr(tc, "request", None)
    source_text = getattr(tc, "source_text", None)
    out: List[str] = []
    if request:
        out.append(f"{request.method} {request.url}")
        if request.body is not None:
            try:
                body_str = json.dumps(request.body, ensure_ascii=False, default=str)[:300]
            except Exception:
                body_str = str(request.body)[:300]
            out.append(f"body: {body_str}")
    if source_text:
        out.append(f"תרחיש: {source_text[:200]}")
    return out or ["(אין צעדים)"]


def _trim(d):
    if d is None:
        return None
    body = d.get("body")
    if body and isinstance(body, (dict, list)):
        s = json.dumps(body, ensure_ascii=False, default=str)
        if len(s) > 1500:
            return {**d, "body": s[:1500] + "...(truncated)"}
    return d
