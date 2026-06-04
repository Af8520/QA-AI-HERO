"""ניהול state per chat session: phase, suite_id, postman, bugs."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

from models.bug import BugReport
from models.postman import PostmanCollection

Phase = Literal["A_copilot", "B_pipeline", "done"]


@dataclass
class ChatSession:
    session_id: str
    phase: Phase = "A_copilot"
    spec_text: Optional[str] = None
    spec_filename: Optional[str] = None
    postman_collection: Optional[PostmanCollection] = None
    suite_id: Optional[int] = None
    # Foundry mode: test_cases מועברים ישירות (ללא ADO)
    direct_test_cases: Optional[List[Dict[str, Any]]] = None
    # Phase A raw JSON (כפי שהגיע מהסוכן / הודבק) — לדיבוג; שמור גם לדיסק ב-logs/phase_a/
    phase_a_raw_json: Optional[List[Dict[str, Any]]] = None
    phase_a_json_file: Optional[str] = None
    pending_bugs: List[BugReport] = field(default_factory=list)
    pipeline_task: Optional[asyncio.Task] = None
    event_queue: "asyncio.Queue[Dict[str, Any]]" = field(default_factory=asyncio.Queue)
    last_activity_ts: float = field(default_factory=time.time)
    pipeline_result: Optional[Dict[str, Any]] = None
    bugs_decision: Optional[asyncio.Future] = None  # human approval — set ע"י /approve-bugs

    def touch(self) -> None:
        self.last_activity_ts = time.time()

    async def emit(self, event_type: str, data: Any) -> None:
        await self.event_queue.put({"type": event_type, "data": data, "ts": time.time()})


class SessionStore:
    def __init__(self) -> None:
        self._sessions: Dict[str, ChatSession] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(self, session_id: Optional[str]) -> ChatSession:
        async with self._lock:
            if session_id and session_id in self._sessions:
                self._sessions[session_id].touch()
                return self._sessions[session_id]
            new_id = session_id or str(uuid.uuid4())
            s = ChatSession(session_id=new_id)
            self._sessions[new_id] = s
            return s

    async def get(self, session_id: str) -> Optional[ChatSession]:
        async with self._lock:
            return self._sessions.get(session_id)

    async def delete(self, session_id: str) -> None:
        async with self._lock:
            self._sessions.pop(session_id, None)


store = SessionStore()
