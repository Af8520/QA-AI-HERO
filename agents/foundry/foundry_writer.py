"""FoundryTestCaseWriter — מסלול חלופי לכתיבת test cases דרך Azure AI Foundry.

עוקף את Copilot Studio (שתקוע על הרשאות). הסוכן ב-Foundry מקבל מסמך אפיון
ומחזיר רשימת test cases כ-JSON.

הזדהות: DefaultAzureCredential (az login / Visual Studio Enterprise).
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, List, Optional

from config.logging_config import get_logger
from config.settings import settings

log = get_logger(__name__)


PROMPT_TEMPLATE = """אנא צור test cases עבור US-{us_number}.

תוכן מסמך האפיון:
{spec_content}

החזר JSON בלבד בפורמט:
[
  {{
    "test_case_id": "תיאור קצר בעברית",
    "steps": [
      {{"step": "string", "expected_result": "string"}}
    ]
  }}
]
"""


class FoundryTestCaseWriter:
    """ממשק סינכרוני-חיצוני, פנימית רץ ב-thread כי ה-SDK של Foundry סינכרוני."""

    def __init__(self) -> None:
        if not settings.foundry_enabled:
            raise RuntimeError("Foundry לא מוגדר ב-.env (AZURE_FOUNDRY_ENDPOINT + FOUNDRY_WRITER_AGENT_ID)")
        self._endpoint = settings.AZURE_FOUNDRY_ENDPOINT
        self._agent_id = settings.FOUNDRY_WRITER_AGENT_ID
        self._project = None  # lazy

    def _get_client(self):
        if self._project is not None:
            return self._project
        try:
            from azure.ai.agents import AgentsClient  # type: ignore[import-not-found]
        except ImportError as e:  # pragma: no cover
            raise RuntimeError(
                "azure-ai-agents לא מותקן. הרץ: pip install azure-ai-agents azure-identity"
            ) from e
        credential = _build_credential(settings.FOUNDRY_AUTH_MODE)
        self._project = AgentsClient(endpoint=self._endpoint, credential=credential)
        log.info(
            "foundry_client_initialized",
            endpoint=self._endpoint,
            agent_id=self._agent_id,
            auth=settings.FOUNDRY_AUTH_MODE,
        )
        return self._project

    async def generate_test_cases(
        self,
        spec_content: str,
        us_number: str,
    ) -> List[Dict[str, Any]]:
        """מחזיר רשימת test cases כ-list של dicts: {test_case_id, steps:[{step, expected_result}]}."""
        return await asyncio.to_thread(self._generate_sync, spec_content, us_number)

    def _generate_sync(self, spec_content: str, us_number: str) -> List[Dict[str, Any]]:
        """פונה לסוכן הקיים ב-Foundry דרך azure-ai-agents SDK עם handshake אוטומטי.

        הסוכן ב-Foundry מקונפג כ-multi-turn: spec → טבלה → "תקין" → US → JSON.
        אנחנו מדמים את ה-flow אוטומטית: בודקים אחרי כל הודעה אם יש JSON,
        ואם לא — שולחים את הצעד הבא (אישור / US / בקשה ישירה ל-JSON).
        """
        client = self._get_client()
        thread = client.threads.create()
        log.info("foundry_thread_created", thread_id=thread.id)

        # תור הודעות אוטומטיות לפי הסדר. אחרי כל אחת — נבדוק אם יש JSON.
        turns = [
            ("send_spec", PROMPT_TEMPLATE.format(us_number=us_number, spec_content=spec_content)),
            ("approve", "תקין. אשר את הטבלה הזו והפק JSON של התסריטים."),
            ("send_us", f"מספר US: {us_number}. אנא החזר עכשיו את התסריטים בפורמט JSON."),
            ("ask_json", "החזר את התסריטים בפורמט JSON בלבד, ללא טקסט נלווה, לפי המבנה שביקשתי."),
        ]

        last_text = ""
        for turn_idx, (turn_name, message) in enumerate(turns, 1):
            log.info("foundry_turn_start", turn=turn_idx, name=turn_name)
            last_text = self._send_and_wait(client, thread.id, message)
            log.info("foundry_turn_response", turn=turn_idx, chars=len(last_text), preview=last_text[:120])
            test_cases = _extract_json_array(last_text)
            if test_cases:
                log.info("foundry_test_cases_generated", count=len(test_cases), turn=turn_idx)
                return test_cases

        log.warning("foundry_no_json_after_all_turns", preview=last_text[:300])
        raise RuntimeError(
            f"Foundry agent לא החזיר JSON אחרי {len(turns)} ניסיונות. "
            f"תשובה אחרונה: {last_text[:300]}"
        )

    def _send_and_wait(self, client, thread_id: str, message: str) -> str:
        """שולח הודעה, מריץ את הסוכן, מחכה, מחזיר את תשובת ה-assistant האחרונה."""
        client.messages.create(thread_id=thread_id, role="user", content=message)
        run = client.runs.create_and_process(thread_id=thread_id, agent_id=self._agent_id)
        if run.status != "completed":
            last_err = getattr(run, "last_error", None)
            log.warning("foundry_run_not_completed", status=run.status, error=last_err)
            raise RuntimeError(f"Foundry run failed: status={run.status}, error={last_err}")
        try:
            from azure.ai.agents.models import ListSortOrder  # type: ignore[import-not-found]
            messages_iter = client.messages.list(thread_id=thread_id, order=ListSortOrder.ASCENDING)
        except ImportError:
            messages_iter = client.messages.list(thread_id=thread_id)
        return _extract_last_assistant_text(messages_iter)


def _build_credential(mode: str):
    """בונה Azure TokenCredential לפי mode. ברירת מחדל = browser (לא דורש az login).

    מצבים:
    - browser: InteractiveBrowserCredential — פותח דפדפן בריצה ראשונה. הכי טוב לפיתוח דסקטופ.
    - device_code: DeviceCodeCredential — מציג code, פתח https://microsoft.com/devicelogin.
    - default: DefaultAzureCredential עם interactive browser מופעל — שרשרת מלאה.
    """
    try:
        from azure.identity import (  # type: ignore[import-not-found]
            DefaultAzureCredential,
            DeviceCodeCredential,
            InteractiveBrowserCredential,
        )
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("azure-identity לא מותקן. הרץ: pip install azure-identity") from e

    mode = (mode or "browser").lower()
    if mode == "device_code":
        log.info("foundry_auth_device_code")
        return DeviceCodeCredential()
    if mode == "default":
        log.info("foundry_auth_default_chain")
        # exclude_interactive_browser_credential=False — מאפשר fallback לדפדפן אם אין az login
        return DefaultAzureCredential(exclude_interactive_browser_credential=False)
    # default: browser
    log.info("foundry_auth_interactive_browser")
    return InteractiveBrowserCredential()


def _extract_last_assistant_text(messages_iter) -> str:
    """שולף טקסט מההודעה האחרונה של ה-assistant.

    OpenAI Assistants API מחזיר Message objects עם content = list של blocks.
    כל block של text מכיל .text.value עם הטקסט.
    """
    last_assistant = None
    for msg in messages_iter:
        role = getattr(msg, "role", None) or (msg.get("role") if isinstance(msg, dict) else None)
        if role == "assistant":
            last_assistant = msg

    if last_assistant is None:
        return ""

    content = getattr(last_assistant, "content", None) or (
        last_assistant.get("content") if isinstance(last_assistant, dict) else None
    )
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return str(content) if content else ""

    parts: List[str] = []
    for block in content:
        # OpenAI Assistant text block: block.type == "text", block.text.value == "..."
        block_type = getattr(block, "type", None) or (block.get("type") if isinstance(block, dict) else None)
        if block_type and block_type != "text":
            continue
        text_obj = getattr(block, "text", None) or (block.get("text") if isinstance(block, dict) else None)
        if text_obj is None:
            continue
        if isinstance(text_obj, str):
            parts.append(text_obj)
            continue
        value = getattr(text_obj, "value", None) or (
            text_obj.get("value") if isinstance(text_obj, dict) else None
        )
        if isinstance(value, str):
            parts.append(value)
    return "\n".join(parts)


def _extract_json_array(text: str) -> Optional[List[Dict[str, Any]]]:
    """מחפש JSON array בתוך הטקסט. תומך ב-fenced code block ו-raw."""
    if not text:
        return None
    # 1. fenced ```json [...] ```
    m = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", text, re.DOTALL)
    if m:
        try:
            parsed = json.loads(m.group(1))
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
    # 2. raw array — מחפש את התחילה של [
    start = text.find("[")
    end = text.rfind("]")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
    return None


def foundry_to_raw_cases(foundry_test_cases: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """ממיר test cases מפורמט Foundry לפורמט raw_ado_test_case שה-SmartCompiler מצפה לו."""
    raw = []
    for idx, tc in enumerate(foundry_test_cases, 1):
        title = tc.get("test_case_id") or f"TC-{idx}"
        steps = tc.get("steps") or []
        text_parts = [title, ""]
        for s in steps:
            step = s.get("step", "")
            expected = s.get("expected_result", "")
            text_parts.append(f"- {step}")
            if expected:
                text_parts.append(f"  צפוי: {expected}")
        raw.append({"id": idx, "title": title, "text": "\n".join(text_parts)})
    return raw
