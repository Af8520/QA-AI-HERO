"""FastAPI routes — Chat UI proxy + pipeline trigger.

Phase A: כל הודעת chat מועברת לסוכן Copilot Studio (proxy).
Phase B: pipeline אוטומטי, התקדמות נשלחת ב-SSE.
"""

from __future__ import annotations

import asyncio
import io
import json
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse

from agents.copilot_bridge import get_copilot_bridge
from agents.postman.postman_loader import load_collection_from_dict
from config.logging_config import get_logger
from config.settings import settings
from server.chat_session import ChatSession, store

log = get_logger(__name__)
router = APIRouter()

# Singleton bridge (in-memory state).
_bridge = get_copilot_bridge()


def _resolve_session_id(request: Request, body_session_id: Optional[str]) -> Optional[str]:
    return body_session_id or request.headers.get("X-Session-ID")


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.post("/session/start")
async def start_session(request: Request):
    session = await store.get_or_create(None)
    if settings.copilot_canvas_mode:
        # ★ Custom Canvas mode — UI מטמיע WebChat עם file upload + auto JSON detection
        return {
            "session_id": session.session_id,
            "phase": session.phase,
            "canvas_mode": True,
            "embed_mode": False,
            "token_endpoint": settings.COPILOT_TOKEN_ENDPOINT,
            "webchat_url": None,
            "foundry_enabled": settings.foundry_enabled,
            "agent_message": None,
            # WebSocket / polling — תומך סטרימינג מילה-מילה כש-WebSocket אפשרי
            "use_websocket": settings.COPILOT_USE_WEBSOCKET,
            "polling_interval_ms": settings.COPILOT_POLLING_INTERVAL_MS,
        }
    if settings.copilot_embed_mode:
        # Embed mode (fallback) — iframe חיצוני, cross-origin, JSON paste ידני
        return {
            "session_id": session.session_id,
            "phase": session.phase,
            "canvas_mode": False,
            "embed_mode": True,
            "token_endpoint": None,
            "webchat_url": settings.COPILOT_WEBCHAT_URL,
            "foundry_enabled": settings.foundry_enabled,
            "agent_message": None,
        }
    greeting = await _bridge.start_session(session.session_id)
    return {
        "session_id": session.session_id,
        "phase": session.phase,
        "canvas_mode": False,
        "embed_mode": False,
        "token_endpoint": None,
        "webchat_url": None,
        "foundry_enabled": settings.foundry_enabled,
        "agent_message": greeting,
    }


@router.post("/foundry/generate-and-run")
async def foundry_generate_and_run(
    request: Request,
    file: UploadFile = File(...),
    session_id: Optional[str] = Form(None),
    us_number: str = Form("000000"),
):
    """מסלול Foundry: מקבל מסמך אפיון → Foundry מייצר tcs → Phase B רץ ישירות (ללא ADO)."""
    if not settings.foundry_enabled:
        raise HTTPException(400, "Foundry לא מוגדר ב-.env (AZURE_FOUNDRY_ENDPOINT + FOUNDRY_WRITER_AGENT_ID)")
    sid = _resolve_session_id(request, session_id)
    session = await store.get_or_create(sid)

    raw = await file.read()
    spec_text = _extract_text(file.filename or "doc", raw)
    session.spec_text = spec_text
    session.spec_filename = file.filename
    session.touch()

    # 1. Foundry → test cases
    from agents.foundry import FoundryTestCaseWriter
    from agents.foundry.foundry_writer import foundry_to_raw_cases

    try:
        writer = FoundryTestCaseWriter()
        foundry_cases = await writer.generate_test_cases(spec_text, us_number)
    except Exception as e:
        log.error("foundry_failed", error=str(e), exc_info=True)
        raise HTTPException(500, f"Foundry כשל: {e}")

    raw_cases = foundry_to_raw_cases(foundry_cases)
    session.direct_test_cases = raw_cases
    session.suite_id = 0  # סימון שלא משתמש ב-ADO

    # 2. הפעל את Phase B ברקע
    await _trigger_phase_b(session, suite_id=0)
    return {
        "session_id": session.session_id,
        "phase": session.phase,
        "test_cases_generated": len(raw_cases),
        "test_case_ids": [r["title"] for r in raw_cases],
    }


@router.post("/complete-phase-a")
async def complete_phase_a(request: Request, payload: dict):
    """Embed mode: היוזר מסיים את השיחה ב-iframe ומזין ידנית את suite_id."""
    sid = _resolve_session_id(request, payload.get("session_id"))
    if not sid:
        raise HTTPException(400, "session_id חסר")
    session = await store.get_or_create(sid)
    suite_id = payload.get("suite_id")
    try:
        suite_id_int = int(suite_id)
    except (TypeError, ValueError):
        raise HTTPException(400, "suite_id חייב להיות מספר")
    if suite_id_int <= 0:
        raise HTTPException(400, "suite_id חייב להיות חיובי")
    await _trigger_phase_b(session, suite_id_int)
    return {"session_id": sid, "phase": session.phase, "suite_id": suite_id_int}


@router.post("/extract-spec")
async def extract_spec(
    request: Request,
    file: UploadFile = File(...),
    session_id: Optional[str] = Form(None),
):
    """Helper ל-embed mode: מחלץ טקסט ממסמך כדי שהיוזר יוכל להעתיק ולהדביק ב-iframe.

    לא שולח לסוכן — רק מחלץ ומחזיר.
    """
    sid = _resolve_session_id(request, session_id)
    session = await store.get_or_create(sid)
    raw = await file.read()
    text = _extract_text(file.filename or "doc", raw)
    session.spec_text = text
    session.spec_filename = file.filename
    session.touch()
    log.info("spec_extracted", session_id=sid, filename=file.filename, chars=len(text))
    return {
        "session_id": session.session_id,
        "filename": file.filename,
        "chars": len(text),
        "text": text,
    }


@router.post("/direct-json")
async def direct_json(request: Request, payload: dict):
    """Cross-origin workaround: היוזר מעתיק JSON של test cases מה-iframe ומדביק כאן.

    מצופה payload: {"session_id": "...", "json_text": "..." או "test_cases": [...]}.
    מאכלס session.direct_test_cases ומטריג Phase B (כמו /foundry/generate-and-run).
    """
    sid = _resolve_session_id(request, payload.get("session_id"))
    session = await store.get_or_create(sid)

    raw_payload = payload.get("test_cases") or payload.get("json_text") or ""
    test_cases: list

    if isinstance(raw_payload, list):
        test_cases = raw_payload
    else:
        text = str(raw_payload).strip()
        if not text:
            raise HTTPException(400, "test_cases / json_text ריקים")
        # תמיכה ב-fenced ```json [...] ``` או raw [...]
        from agents.foundry.foundry_writer import _extract_json_array
        parsed = _extract_json_array(text)
        if parsed is None:
            raise HTTPException(400, "לא נמצא JSON array תקני בטקסט המודבק")
        test_cases = parsed

    if not isinstance(test_cases, list) or not test_cases:
        raise HTTPException(400, "test_cases חייב להיות רשימה לא ריקה")

    # ולידציה בסיסית: כל פריט הוא dict עם test_case_id ו-steps (פורמט הסוכן)
    for i, tc in enumerate(test_cases):
        if not isinstance(tc, dict):
            raise HTTPException(400, f"test case #{i} אינו object")
        if "test_case_id" not in tc:
            raise HTTPException(400, f"test case #{i} חסר test_case_id")

    from agents.foundry.foundry_writer import foundry_to_raw_cases
    raw_cases = foundry_to_raw_cases(test_cases)
    session.direct_test_cases = raw_cases
    session.phase_a_raw_json = test_cases
    session.suite_id = 0

    # ★ שמירה לדיסק לדיבוג — היוזר ביקש לראות מה הסוכן באמת החזיר
    json_file = _persist_phase_a_json(sid or session.session_id, test_cases)
    session.phase_a_json_file = json_file
    session.touch()

    # הדפסה לטרמינל — הסוכן הוא JSON של 20-35 cases ולכן זה ראדבל
    print(f"\n========== PHASE A JSON SAVED — {len(test_cases)} test cases ==========")
    print(f"File: {json_file}")
    try:
        print(json.dumps(test_cases, ensure_ascii=False, indent=2))
    except Exception:
        print(repr(test_cases))
    print("=" * 70 + "\n", flush=True)

    log.info("direct_json_loaded", session_id=sid, count=len(raw_cases), saved_to=json_file)

    await _trigger_phase_b(session, suite_id=0)
    return {
        "session_id": session.session_id,
        "phase": session.phase,
        "test_cases_loaded": len(raw_cases),
        "test_case_ids": [r["title"] for r in raw_cases],
        "phase_a_json_file": json_file,
    }


def _persist_phase_a_json(session_id: str, test_cases: list) -> str:
    """שומר את ה-JSON שהגיע מהסוכן ל-logs/phase_a/<timestamp>_<session>.json.
    מחזיר נתיב מלא — נשלח גם ל-UI כדי שייצור קישור.
    """
    import datetime
    from pathlib import Path

    logs_dir = Path("logs") / "phase_a"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_sid = (session_id or "anon")[:12]
    fpath = logs_dir / f"{ts}_{safe_sid}.json"
    try:
        fpath.write_text(json.dumps(test_cases, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log.warning("phase_a_persist_failed", error=str(e), path=str(fpath))
    return str(fpath)


@router.get("/session/{session_id}/phase-a-json")
async def get_phase_a_json(session_id: str):
    """מחזיר את ה-JSON שהתקבל מהסוכן ב-Phase A (אם זמין)."""
    session = await store.get(session_id)
    if not session or not session.phase_a_raw_json:
        raise HTTPException(404, "אין JSON של Phase A ל-session זה")
    return {
        "session_id": session_id,
        "count": len(session.phase_a_raw_json),
        "test_cases": session.phase_a_raw_json,
        "file": session.phase_a_json_file,
    }


@router.post("/upload-document")
async def upload_document(
    request: Request,
    file: UploadFile = File(...),
    session_id: Optional[str] = Form(None),
):
    sid = _resolve_session_id(request, session_id)
    session = await store.get_or_create(sid)
    if session.phase != "A_copilot":
        raise HTTPException(400, "המסמך מועלה רק בשלב A (שיחה עם הסוכן).")

    raw = await file.read()
    text = _extract_text(file.filename or "doc", raw)
    session.spec_text = text
    session.spec_filename = file.filename
    session.touch()

    agent_response = await _bridge.send_document(session.session_id, text, filename=file.filename)
    completion = _bridge.is_completion_message(agent_response)
    if completion:
        await _trigger_phase_b(session, completion.suite_id)
    return {
        "session_id": session.session_id,
        "phase": session.phase,
        "agent_message": agent_response,
        "completion": completion.dict() if completion else None,
        "doc_chars": len(text),
    }


@router.post("/upload-postman")
async def upload_postman(
    request: Request,
    file: UploadFile = File(...),
    session_id: Optional[str] = Form(None),
):
    sid = _resolve_session_id(request, session_id)
    session = await store.get_or_create(sid)
    raw = await file.read()
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise HTTPException(400, f"קובץ Postman לא תקין: {e}")
    try:
        collection = load_collection_from_dict(data)
    except Exception as e:
        raise HTTPException(400, f"כשל בטעינת collection: {e}")
    session.postman_collection = collection
    session.touch()
    return {
        "session_id": session.session_id,
        "phase": session.phase,
        "collection_name": collection.name,
        "request_count": len(collection.requests),
        "request_names": collection.request_names(),
    }


@router.post("/chat")
async def chat(request: Request, payload: dict):
    sid = _resolve_session_id(request, payload.get("session_id"))
    session = await store.get_or_create(sid)
    user_message = (payload.get("message") or "").strip()

    if not user_message:
        raise HTTPException(400, "הודעה ריקה")

    if session.phase == "done":
        return {
            "session_id": session.session_id,
            "phase": "done",
            "agent_message": "הריצה הסתיימה. ניתן לפתוח session חדש.",
        }

    if session.phase == "B_pipeline":
        return {
            "session_id": session.session_id,
            "phase": "B_pipeline",
            "agent_message": "ה-pipeline רץ ברקע. עקוב אחרי ההתקדמות בצ'אט.",
        }

    # Phase A — proxy לסוכן.
    agent_response = await _bridge.send(session.session_id, user_message)
    session.touch()
    completion = _bridge.is_completion_message(agent_response)

    response = {
        "session_id": session.session_id,
        "phase": session.phase,
        "agent_message": agent_response,
        "completion": completion.dict() if completion else None,
    }

    if completion:
        await _trigger_phase_b(session, completion.suite_id)
        response["phase"] = session.phase

    return response


@router.post("/approve-bugs")
async def approve_bugs(request: Request, payload: dict):
    sid = _resolve_session_id(request, payload.get("session_id"))
    if not sid:
        raise HTTPException(400, "session_id חסר")
    session = await store.get(sid)
    if not session:
        raise HTTPException(404, "session לא נמצא")
    if not session.bugs_decision or session.bugs_decision.done():
        raise HTTPException(400, "אין בקשת אישור פתוחה")
    approved = bool(payload.get("approved", False))
    session.bugs_decision.set_result(approved)
    return {"session_id": sid, "approved": approved}


@router.get("/events/{session_id}")
async def events(session_id: str, request: Request):
    session = await store.get(session_id)
    if not session:
        raise HTTPException(404, "session לא נמצא")

    async def event_generator():
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(session.event_queue.get(), timeout=15.0)
            except asyncio.TimeoutError:
                yield {"event": "ping", "data": "{}"}
                continue
            yield {"event": event["type"], "data": json.dumps(event["data"], ensure_ascii=False)}
            if event["type"] == "pipeline_done":
                break

    return EventSourceResponse(event_generator())


async def _trigger_phase_b(session: ChatSession, suite_id: int) -> None:
    """מעבר Phase A -> Phase B: הפעלת ה-pipeline ב-background."""
    if session.phase != "A_copilot":
        return
    if not session.postman_collection:
        await session.emit("warning", {"text": "לא הועלה Postman Collection — נריץ ב-mock mode."})
    session.suite_id = suite_id
    session.phase = "B_pipeline"
    log.info("phase_transition", session_id=session.session_id, suite_id=suite_id)

    from pipeline.esb_pipeline import run_esb_pipeline  # late import כדי להימנע מ-circular

    async def runner():
        try:
            result = await run_esb_pipeline(session)
            session.pipeline_result = result.dict() if hasattr(result, "dict") else dict(result)
            session.phase = "done"
            await session.emit("pipeline_done", session.pipeline_result)
        except Exception as e:
            log.error("pipeline_error", error=str(e), exc_info=True)
            await session.emit("error", {"text": f"שגיאה ב-pipeline: {e}"})
            session.phase = "done"
            await session.emit("pipeline_done", {"error": str(e)})

    session.pipeline_task = asyncio.create_task(runner())


def _extract_text(filename: str, content: bytes) -> str:
    name = (filename or "").lower()
    if name.endswith(".docx"):
        from docx import Document  # type: ignore[import-not-found]

        doc = Document(io.BytesIO(content))
        return "\n".join(p.text for p in doc.paragraphs if p.text)
    if name.endswith(".pdf"):
        from PyPDF2 import PdfReader  # type: ignore[import-not-found]

        reader = PdfReader(io.BytesIO(content))
        return "\n".join((p.extract_text() or "") for p in reader.pages)
    # ברירת מחדל — UTF-8 / Latin-1
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        return content.decode("latin-1", errors="replace")


@router.post("/debug-log")
async def debug_log(request: Request):
    """לוג ניפוי שגיאות מהדפדפן → terminal של ה-server.

    ה-WebChat רץ ב-browser, אז הודעות מ-Copilot Studio לא עוברות דרך Python.
    ה-endpoint הזה מאפשר ל-UI לשלוח כל אירוע ל-server log כדי שניתן יהיה
    לראות בטרמינל מה הסוכן באמת החזיר (במיוחד כאשר JSON לא זוהה).
    """
    try:
        data = await request.json()
    except Exception:
        data = {"raw": "<non-json body>"}
    # NOTE: structlog משתמש ב-'event' כשם השדה של ה-message — לכן אנחנו לא יכולים
    # להעביר event=... כ-kwarg. אנחנו מקודדים את ה-event אל תוך ה-log message.
    browser_event = data.get("event", "unknown") if isinstance(data, dict) else "unknown"
    payload = data.get("data") if isinstance(data, dict) else None
    sid = data.get("session_id") if isinstance(data, dict) else None
    if isinstance(payload, dict) and "text_full" in payload:
        log.info(
            f"browser_debug.{browser_event}",
            session_id=sid,
            text_len=payload.get("text_len"),
            preview=payload.get("text_preview"),
        )
        # ה-text המלא ב-line נפרד כדי שיתפוס שורה שלמה ויהיה קל לקרוא
        full = payload.get("text_full") or ""
        if full:
            print(f"\n===== AGENT MESSAGE ({browser_event}) — {payload.get('text_len', 0)} chars =====")
            print(full)
            print("=" * 70 + "\n", flush=True)
    else:
        log.info(f"browser_debug.{browser_event}", session_id=sid, data=payload)
    return {"ok": True}


@router.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    from pathlib import Path

    html_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


# Helper לשימוש ב-tests/main.
__all__ = ["router"]
