"""Copilot Bridge — proxy לסוכן Copilot Studio.

Phase A: Python = proxy טהור.
- המשתמש מעלה מסמך → נשלח כהודעה ראשונה לסוכן
- כל הודעה הלוך וחזור → proxy שקוף
- בסיום הסוכן מחזיר הודעת הצלחה עם suite_id → trigger Phase B

מבוסס על microsoft-agents-copilotstudio-client SDK (ConnectionSettings + CopilotClient).
"""

from __future__ import annotations

import abc
import asyncio
import re
from typing import Any, Dict, Optional

from config.logging_config import get_logger
from config.settings import settings
from models.pipeline import CompletionInfo

log = get_logger(__name__)

# הודעת הצלחה מהסוכן בדרך כלל מכילה "הועלה" + מספר ADO suite/folder.
COMPLETION_PATTERNS = [
    re.compile(r"(?:suite|ADO|תיקיי?ה|התיקייה)\D{0,40}?(\d{3,})", re.IGNORECASE),
    re.compile(r"הועלו? בהצלחה.*?(\d{3,})", re.IGNORECASE),
]
COMPLETION_KEYWORDS = ("הועלה בהצלחה", "הועלו בהצלחה", "uploaded successfully")


class CopilotBridgeBase(abc.ABC):
    @abc.abstractmethod
    async def start_session(self, session_id: str) -> str: ...

    @abc.abstractmethod
    async def send(self, session_id: str, user_message: str) -> str: ...

    @abc.abstractmethod
    async def send_document(self, session_id: str, doc_text: str, filename: Optional[str] = None) -> str: ...

    async def end_session(self, session_id: str) -> None:
        return None

    def is_completion_message(self, agent_message: str) -> Optional[CompletionInfo]:
        if not agent_message:
            return None
        msg = agent_message.strip()
        keyword_hit = any(k in msg for k in COMPLETION_KEYWORDS)
        for pattern in COMPLETION_PATTERNS:
            m = pattern.search(msg)
            if m and (keyword_hit or "suite" in msg.lower() or "ADO" in msg):
                try:
                    return CompletionInfo(suite_id=int(m.group(1)), raw_message=msg)
                except ValueError:
                    continue
        return None


# ----------------------------- Mock -----------------------------

class CopilotBridgeMock(CopilotBridgeBase):
    """Mock — multi-turn דמה לפיתוח ללא Copilot Studio."""

    def __init__(self) -> None:
        self._states: Dict[str, str] = {}

    async def start_session(self, session_id: str) -> str:
        self._states[session_id] = "init"
        return (
            "שלום! אני הסוכן של QA-AI-Hero. "
            "אנא העלה מסמך אפיון (Word/PDF) או הדבק תוכן כדי שאצור עבורך תסריטי בדיקה."
        )

    async def send_document(self, session_id: str, doc_text: str, filename: Optional[str] = None) -> str:
        self._states[session_id] = "awaiting_approval"
        preview = (doc_text or "")[:80].replace("\n", " ")
        return (
            f"קיבלתי את המסמך ({filename or 'ללא שם'}, {len(doc_text)} תווים).\n\n"
            f"להלן טבלת תסריטים מוצעת:\n"
            f"| # | תרחיש |\n|---|---|\n"
            f"| 1 | בדיקת flow תקין |\n| 2 | בדיקת payload לא תקין |\n\n"
            f"(תקציר אפיון: {preview}...)\n\nהאם זה תקין?"
        )

    async def send(self, session_id: str, user_message: str) -> str:
        state = self._states.get(session_id, "init")
        msg = (user_message or "").strip().lower()
        if state == "init":
            return "אנא העלה מסמך אפיון תחילה."
        if state == "awaiting_approval":
            if msg in {"תקין", "ok", "אישור", "מאושר", "כן", "מצוין"}:
                self._states[session_id] = "awaiting_us"
                return "מצוין. אנא תן לי מספר US בן 6 ספרות."
            return "עדכנתי לפי בקשתך. האם זה תקין?"
        if state == "awaiting_us":
            us_match = re.search(r"\b(\d{6})\b", user_message or "")
            if not us_match:
                return "אנא תן לי מספר US תקני בן 6 ספרות."
            us = us_match.group(1)
            self._states[session_id] = "done"
            return f"מעולה! התסריטים עבור US-{us} הועלו בהצלחה ל-ADO suite 999."
        return "השיחה הסתיימה."


# ----------------------------- Real -----------------------------

class CopilotBridgeReal(CopilotBridgeBase):
    """Proxy מלא ל-Copilot Studio Agent דרך microsoft-agents-copilotstudio-client.

    כל session_id ממופה ל-CopilotClient + conversation_id משלו.
    הודעות הסוכן באות כ-AsyncIterable[Activity] — אנחנו מאגדים את כל הטקסט.
    """

    def __init__(self) -> None:
        self._sessions: Dict[str, "_CopilotConversation"] = {}
        self._lock = asyncio.Lock()

    def _build_settings(self):
        from microsoft_agents.copilotstudio.client import (  # type: ignore[import-not-found]
            AgentType,
            ConnectionSettings,
            PowerPlatformCloud,
        )

        try:
            cloud = PowerPlatformCloud[settings.COPILOT_CLOUD.upper()]
        except KeyError:
            cloud = PowerPlatformCloud.PROD
        try:
            agent_type = AgentType[settings.COPILOT_AGENT_TYPE.upper()]
        except KeyError:
            agent_type = AgentType.PUBLISHED

        return ConnectionSettings(
            environment_id=settings.COPILOT_ENVIRONMENT_ID,
            agent_identifier=settings.COPILOT_AGENT_IDENTIFIER,
            cloud=cloud,
            copilot_agent_type=agent_type,
            direct_connect_url=settings.COPILOT_DIRECT_CONNECT_URL,
        )

    def _build_client(self):
        from microsoft_agents.copilotstudio.client import CopilotClient  # type: ignore[import-not-found]

        from agents.copilot_bridge.msal_auth import get_access_token

        conn = self._build_settings()
        scope = CopilotClient.scope_from_settings(conn)
        token = get_access_token(scope)
        return CopilotClient(conn, token)

    async def _get_or_create_conversation(self, session_id: str) -> "_CopilotConversation":
        async with self._lock:
            conv = self._sessions.get(session_id)
            if conv is not None:
                return conv
            client = self._build_client()
            conv = _CopilotConversation(client=client)
            await conv.start()
            self._sessions[session_id] = conv
            log.info(
                "copilot_conversation_started",
                session_id=session_id,
                conversation_id=conv.conversation_id,
            )
            return conv

    async def start_session(self, session_id: str) -> str:
        conv = await self._get_or_create_conversation(session_id)
        # ב-start_conversation הסוכן בדרך כלל שולח greeting; conv.start() כבר אסף אותם.
        return conv.consume_buffered() or "השיחה עם הסוכן החלה. אנא העלה מסמך אפיון."

    async def send(self, session_id: str, user_message: str) -> str:
        conv = await self._get_or_create_conversation(session_id)
        return await conv.ask(user_message)

    async def send_document(self, session_id: str, doc_text: str, filename: Optional[str] = None) -> str:
        prefix = f"מסמך אפיון מצורף ({filename}):\n\n" if filename else "מסמך אפיון מצורף:\n\n"
        return await self.send(session_id, prefix + (doc_text or ""))

    async def end_session(self, session_id: str) -> None:
        async with self._lock:
            self._sessions.pop(session_id, None)


class _CopilotConversation:
    """עוטף שיחה אחת ב-CopilotClient: start_conversation + ask_question.

    מאגד טקסט מכל ה-activities מסוג message שמתקבלים.
    """

    def __init__(self, client) -> None:
        self._client = client
        self._buffer: list[str] = []
        self.conversation_id: Optional[str] = None

    async def start(self) -> None:
        async for act in self._client.start_conversation(emit_start_conversation_event=True):
            self._handle_activity(act)
        # ה-CopilotClient שומר conversation_id באוטומט; ננסה לחלץ.
        self.conversation_id = getattr(self._client, "_current_conversation_id", None)

    async def ask(self, user_message: str) -> str:
        # ניקוי buffer לפני הבקשה הבאה
        self._buffer.clear()
        async for act in self._client.ask_question(user_message, conversation_id=self.conversation_id):
            self._handle_activity(act)
        # ייתכן שה-conversation_id התעדכן
        new_id = getattr(self._client, "_current_conversation_id", None)
        if new_id:
            self.conversation_id = new_id
        return self.consume_buffered()

    def _handle_activity(self, activity: Any) -> None:
        atype = getattr(activity, "type", None) or (activity.get("type") if isinstance(activity, dict) else None)
        if atype != "message":
            # event / typing / endOfConversation וכו' — לא מכניסים לטקסט
            log.debug("copilot_non_message_activity", type=atype)
            return
        text = (
            getattr(activity, "text", None)
            or (activity.get("text") if isinstance(activity, dict) else None)
            or ""
        )
        if text:
            self._buffer.append(text.strip())

    def consume_buffered(self) -> str:
        if not self._buffer:
            return ""
        joined = "\n\n".join(self._buffer)
        self._buffer.clear()
        return joined


# ----------------------------- Factory -----------------------------

def get_copilot_bridge() -> CopilotBridgeBase:
    """Factory: real אם 4 השדות מלאים, אחרת mock. ללא Foundry."""
    if settings.copilot_real_enabled:
        log.info(
            "copilot_bridge_selected",
            mode="real",
            agent=settings.COPILOT_AGENT_IDENTIFIER,
            env=settings.COPILOT_ENVIRONMENT_ID,
            auth=settings.COPILOT_AUTH_MODE,
        )
        return CopilotBridgeReal()
    log.info("copilot_bridge_selected", mode="mock")
    return CopilotBridgeMock()
