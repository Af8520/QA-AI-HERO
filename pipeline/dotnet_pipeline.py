"""DotNet Pipeline — Phase B עבור מחלקת .NET (Kafka + Couchbase Worker tests).

מקביל ל-esb_pipeline אבל:
- שלב 3 משתמש ב-DotNetCompiler במקום SmartCompiler
- שלב 4 משתמש ב-DotNetRunner; ה-tc_detail מכיל {actions, observations} במקום request/response
- שלב 5 מסומן skipped (verification מובנה ב-actions)
- שאר השלבים זהים — Validator + BugAgent + Reporter עובדים על TestCaseResult generic
"""

from __future__ import annotations

import asyncio
import json
from typing import List, Optional, Tuple

from agents.bug_agent.ado_client import ADOClient
from agents.bug_agent.bug_agent import BugAgent
from agents.compiler.dotnet_compiler import DotNetCompiler
from agents.payload_builder import PayloadBuilderBridge
from agents.payload_builder.payload_builder_bridge import PayloadBuilderError
from agents.reporter.reporter_agent import ReporterAgent
from agents.runner.dotnet_runner import DotNetRunner
from agents.validator.validator_agent import ValidatorAgent
from config.logging_config import get_logger
from config.settings import settings
from models.dotnet_test_case import DotNetExecutableTestCase
from models.executable_test_case import ExecutableTestCase, HttpRequestSpec
from models.pipeline import PipelineResult
from models.test_case import ResponseAssertion, StepResult, TestCaseResult, TestStatus
from models.test_run import ValidationResult
from server.chat_session import ChatSession

log = get_logger(__name__)

TOTAL_STAGES = 7


async def run_dotnet_pipeline(session: ChatSession) -> PipelineResult:
    import datetime
    import uuid as _uuid
    from pathlib import Path

    # ★ run_id + run log — לוג מובנה פר-ריצה, נשלח ב-SSE + נשמר לדיסק
    run_id = _uuid.uuid4().hex[:12]
    session.run_id = run_id
    _runs_dir = Path("logs") / "runs"
    try:
        _runs_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    _log_file = _runs_dir / f"{run_id}.jsonl"

    async def run_log(message: str, status: str = "info", tc_id: str = "", action: str = "",
                      ts: str = "") -> None:
        entry = {
            "ts": ts or datetime.datetime.now().strftime("%H:%M:%S"),
            "run_id": run_id,
            "tc_id": tc_id,
            "action": action,
            "status": status,   # info | success | warn | error
            "message": message,
        }
        await session.emit("log_line", entry)
        try:
            with _log_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass

    async def emit(text: str) -> None:
        await session.emit("progress", {"text": text})

    async def tl_step(step: int, status: str, label: str = "") -> None:
        await session.emit("tl_step", {"step": step, "status": status, "label": label})
        await run_log(f"שלב {step}: {label} [{status}]", status="info" if status != "failed" else "error",
                      action="step")

    async def tl_tc(idx: int, total: int, name: str, status: str, http_status: int = 0) -> None:
        await session.emit("tl_tc", {
            "idx": idx, "total": total, "name": name,
            "status": status, "http_status": http_status,
        })

    async def emit_tc_detail(tc_id: str, request: dict, response: dict) -> None:
        """ב-.NET, request = {kind: 'dotnet', actions: [...]}, response = {observations: [...]}.
        ה-UI יודע לקרוא את ה-kind ולהציג מסך שונה.
        """
        await session.emit("tc_detail", {
            "test_case_id": tc_id,
            "request": request,
            "response": response,
        })

    await session.emit("tl_phase", {"phase": "B", "status": "active"})
    await run_log(f"▶ תחילת ריצה (.NET) — run_id={run_id}", status="info", action="run_start")

    suite_id = session.suite_id or 0
    ado = ADOClient()

    # שלב 1 — מקור תסריטים
    await tl_step(1, "active", "Pull test cases")
    if session.direct_test_cases:
        await emit(f"שלב 1/{TOTAL_STAGES} — משתמש ב-{len(session.direct_test_cases)} תסריטים מהסוכן .NET...")
        raw_cases = session.direct_test_cases
    else:
        await emit(f"שלב 1/{TOTAL_STAGES} — אין תסריטים זמינים (ADO suite #{suite_id})")
        raw_cases = []

    if not raw_cases:
        await tl_step(1, "failed", "No test cases")
        await emit("⚠ אין תסריטים — מסיים")
        await session.emit("tl_phase", {"phase": "B", "status": "failed"})
        return PipelineResult(error="No test cases found")

    await tl_step(1, "done", f"Pull test cases ({len(raw_cases)})")

    # שלב 2 — ★ Payload Builder (חדש)
    # שולח את ה-spec לסוכן Copilot Studio שני (Payload Builder) שמחזיר templates + field_catalog.
    # אם DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT ריק או spec_text לא זמין → דילוג עם warning.
    await tl_step(2, "active", "Payload Builder")
    spec_md = session.spec_text
    payload_templates = await _build_payloads(session, spec_md, emit)
    if payload_templates:
        tmpl_count = len(payload_templates.get("templates") or {})
        await tl_step(2, "done", f"Payloads ({tmpl_count} templates)")
    else:
        await tl_step(2, "skipped", "No payloads — using regex-only")

    # שלב 3 — DotNet Compiler
    await tl_step(3, "active", f"Compile (0/{len(raw_cases)})")
    await emit(f"שלב 3/{TOTAL_STAGES} — מהדר {len(raw_cases)} תסריטים ל-actions...")
    # ★ מסרי-דוגמה אמיתיים מהמקור (אם היוזר העלה) → בסיס publish format-agnostic
    sample_messages = getattr(session, "sample_source_messages", None)
    compiler = DotNetCompiler(spec_md=spec_md, payload_templates=payload_templates,
                              sample_messages=sample_messages)
    # ★ key_built_from (נתיבי-מקור של ה-target KEY) → unique-id format-agnostic ב-runner
    key_built_from = _extract_key_built_from(payload_templates)
    # ★ נתיב-המקור שהופך ל-KEY/entity_id verbatim (מ-transformations) — להזרקת ה-uid לשדה ה-KEY
    key_source_path = _extract_key_source_path(payload_templates)
    executables: List[DotNetExecutableTestCase] = []
    compile_failures = 0
    for raw in raw_cases:
        tc_label = raw.get("title") or f"TC-{raw.get('id')}"
        try:
            ex = await compiler.compile(raw)
            ex.key_built_from = key_built_from
            ex.key_source_path = key_source_path
            # ★ הגנתי: גם אם ה-compiler לא חתם source_sample (regex-only / נתיב ישן) — אם היוזר
            # העלה מסר-דוגמה, נשתמש בו כבסיס publish דטרמיניסטי ברנר (format-agnostic).
            if sample_messages and not ex.source_sample:
                ex.source_sample = sample_messages[0]
            executables.append(ex)
            kinds = [a.kind for a in ex.actions] or ["(empty)"]
            await emit(f"  ✓ {ex.test_case_id} → actions: {', '.join(kinds)}")
        except Exception as e:
            compile_failures += 1
            log.warning("dotnet_compile_failed", tc=tc_label, error=str(e))
            executables.append(DotNetExecutableTestCase(
                test_case_id=tc_label,
                ado_test_case_id=raw.get("id"),
                actions=[],
                source_text=raw.get("text") or "",
                compiler_notes=f"Compile failed: {str(e)[:200]}",
            ))
            await emit(f"  ✗ {tc_label} → שגיאת קומפילציה: {str(e)[:100]}")

    compile_status = "done" if compile_failures == 0 else (
        "failed" if compile_failures == len(raw_cases) else "done"
    )
    await tl_step(3, compile_status, f"Compile ({len(raw_cases) - compile_failures}/{len(raw_cases)} OK)")

    # שלב 4 — Execute (Publish + Observe)
    await tl_step(4, "active", f"Publish+Observe (0/{len(executables)})")
    await emit(f"שלב 4/{TOTAL_STAGES} — מבצע {len(executables)} תסריטי actions...")
    runner = DotNetRunner()
    results: List[TestCaseResult] = []
    http_shaped_for_validator: List[ExecutableTestCase] = []
    # ★ Early abort: אם נתקלים בשגיאת תשתית fatal (ACL, auth) — לא מריצים את שאר ה-TCs
    infra_failure: Optional[str] = None
    for i, ex in enumerate(executables, 1):
        # ★ אם זוהתה שגיאת תשתית בקריאה קודמת — מסמנים את שאר ה-TCs כ-BLOCKED בלי להריץ
        if infra_failure:
            results.append(TestCaseResult(
                test_case_id=ex.test_case_id,
                ado_test_case_id=ex.ado_test_case_id,
                status=TestStatus.BLOCKED,
                step_results=[StepResult(
                    step="(skipped due to prior infrastructure failure)",
                    expected_result="(n/a)",
                    actual_result=infra_failure,
                    status=TestStatus.BLOCKED,
                    error_message=infra_failure,
                )],
                duration_seconds=0.0,
                api_response={"error": infra_failure, "skipped_after_infra_failure": True},
            ))
            await tl_tc(i, len(executables), ex.test_case_id, "blocked")
            http_shaped_for_validator.append(_to_http_shape(ex))
            continue
        if not ex.actions:
            results.append(TestCaseResult(
                test_case_id=ex.test_case_id,
                ado_test_case_id=ex.ado_test_case_id,
                status=TestStatus.BLOCKED,
                step_results=[StepResult(
                    step="(compile produced no actions)",
                    expected_result="(n/a)",
                    actual_result=ex.compiler_notes or "no actions",
                    status=TestStatus.BLOCKED,
                    error_message=ex.compiler_notes,
                )],
                duration_seconds=0.0,
                api_response={"error": ex.compiler_notes or "no actions"},
            ))
            await emit(f"  ⊘ ({i}/{len(executables)}) {ex.test_case_id} — דילוג (compile failed)")
            await tl_tc(i, len(executables), ex.test_case_id, "skipped")
            http_shaped_for_validator.append(_to_http_shape(ex))
            continue
        try:
            await run_log(f"מתחיל TC: {ex.test_case_id}", status="info", tc_id=ex.test_case_id, action="tc_start")
            r = await runner.execute(ex)
            results.append(r)
            # ★ שידור ה-log שצבר ה-runner פר action (PUBLISH/CONSUME/candidates/MATCH/ASSERT)
            # שומרים את ה-ts המקורי של ה-runner (זמן הפעולה האמיתי, לא זמן ה-replay)
            for le in (r.api_response or {}).get("log", []) or []:
                await run_log(le.get("message", ""), status=le.get("status", "info"),
                              tc_id=ex.test_case_id, action=le.get("action", ""),
                              ts=le.get("ts", ""))
            await run_log(f"TC {ex.test_case_id} → {r.status.value}",
                          status=("success" if r.status == TestStatus.PASSED
                                  else "error" if r.status == TestStatus.FAILED else "warn"),
                          tc_id=ex.test_case_id, action="tc_done")
            # ★ זיהוי שגיאת תשתית — שאר ה-TCs לא יורצו
            infra = _detect_infra_failure(r)
            if infra and not infra_failure:
                infra_failure = infra
                await emit(f"  🛑 זוהתה שגיאת תשתית: {infra[:200]}")
                await emit(f"  ⊘ {len(executables) - i} ה-TCs הבאים יסומנו BLOCKED ללא הרצה.")
            tc_status = "done" if r.status == TestStatus.PASSED else (
                "failed" if r.status == TestStatus.FAILED else "blocked"
            )
            await emit(f"  → ({i}/{len(executables)}) {ex.test_case_id} → {r.status.value}")
            await tl_tc(i, len(executables), ex.test_case_id, tc_status,
                        int((r.api_response or {}).get("status", 0) or 0))
            await emit_tc_detail(ex.test_case_id, {
                "kind": "dotnet",
                "actions": [a.model_dump() for a in ex.actions],
                "expected_status": ex.expected_status,
            }, {
                "status": r.status.value,
                "kind": "dotnet",
                # ★ steps לצ'אט — מה עבר/נכשל פר step
                "steps": [
                    {"label": s.step, "status": s.status.value,
                     "detail": s.actual_result, "error": s.error_message}
                    for s in (r.step_results or [])
                ],
                "observations": (r.api_response or {}).get("observations") or [],
                "duration_ms": (r.api_response or {}).get("duration_ms"),
                "error": (r.api_response or {}).get("error"),
            })
            await tl_step(4, "active", f"Publish+Observe ({i}/{len(executables)})")
        except Exception as e:
            log.warning("dotnet_execute_failed", tc=ex.test_case_id, error=str(e))
            results.append(TestCaseResult(
                test_case_id=ex.test_case_id,
                ado_test_case_id=ex.ado_test_case_id,
                status=TestStatus.BLOCKED,
                step_results=[StepResult(
                    step="execute",
                    expected_result="actions complete",
                    actual_result=f"Exception: {str(e)[:200]}",
                    status=TestStatus.BLOCKED,
                    error_message=str(e),
                )],
                duration_seconds=0.0,
                api_response={"error": str(e)},
            ))
            await emit(f"  ✗ ({i}/{len(executables)}) {ex.test_case_id} → שגיאת ביצוע: {str(e)[:80]}")
            await tl_tc(i, len(executables), ex.test_case_id, "failed")
            await emit_tc_detail(ex.test_case_id, {
                "kind": "dotnet",
                "actions": [a.model_dump() for a in ex.actions],
            }, {"error": str(e), "kind": "dotnet"})

        http_shaped_for_validator.append(_to_http_shape(ex))

    await tl_step(4, "done", f"Publish+Observe ({len(executables)} done)")

    # שלב 5 — Verify (skipped — verification embedded in actions)
    await tl_step(5, "skipped", "Verify (embedded in actions)")
    await emit(f"שלב 5/{TOTAL_STAGES} — verify מובנה ב-actions, מדלגים על שלב נפרד.")

    # שלב 6 — Validation + bug analysis + human approval
    await tl_step(6, "active", "Validate + Bugs")
    await emit(f"שלב 6/{TOTAL_STAGES} — מאמת תוצאות ופותח bugs...")
    validator = ValidatorAgent()
    validations: List[ValidationResult] = await validator.validate_all(
        list(zip(http_shaped_for_validator, results))
    )

    failures: List[Tuple[ExecutableTestCase, TestCaseResult, ValidationResult]] = [
        (ex, r, v)
        for ex, r, v in zip(http_shaped_for_validator, results, validations)
        if v.overall_status in (TestStatus.FAILED, TestStatus.BLOCKED)
    ]

    opened_bugs_models = []
    if failures:
        bug_agent = BugAgent()
        bugs = await bug_agent.analyze(failures)
        await emit(f"זוהו {len(bugs)} באגים פוטנציאליים — ממתין לאישור...")
        await session.emit("bugs_for_approval", {"bugs": [_bug_summary(b) for b in bugs]})
        approved = await _await_human_approval(session, timeout_seconds=300)
        if approved and ado.enabled:
            await emit("פותח bugs ב-ADO...")
            await ado.create_bugs(bugs)
            opened_bugs_models = bugs
        elif approved:
            opened_bugs_models = bugs
            await emit("ADO לא מוגדר — לא נפתחו bugs בפועל.")
        else:
            await emit("דחיית אישור — לא נפתחו bugs.")
    else:
        await emit("אין כשלים — אין צורך ב-bugs.")
    await tl_step(6, "done", f"Validate ({len(failures)} failures, {len(opened_bugs_models)} bugs)")

    # שלב 7 — Reporter
    await tl_step(7, "active", "Reporter")
    await emit(f"שלב 7/{TOTAL_STAGES} — מכין סיכום...")
    reporter = ReporterAgent()
    report = await reporter.generate(
        suite_id=suite_id,
        us_number=None,
        test_cases=http_shaped_for_validator,
        results=results,
        validations=validations,
        opened_bugs=opened_bugs_models,
    )
    await tl_step(7, "done", "Reporter")
    await session.emit("tl_phase", {"phase": "B", "status": "done"})
    await emit("הסתיים.")
    return report


def _detect_infra_failure(result: TestCaseResult) -> Optional[str]:
    """מחזיר תקציר ההסבר אם ה-TC נכשל בגלל בעיית תשתית (ACL/auth/topic).
    תוצאה לא ריקה → הפייפליין יפסיק להריץ TCs ויסמן את השאר BLOCKED.
    """
    api = result.api_response or {}
    # observations מכיל את ה-actions ואת ה-classified error אם היה
    for obs in api.get("observations") or []:
        observation = (obs or {}).get("observation") or {}
        classified = observation.get("classified")
        if isinstance(classified, dict) and classified.get("is_fatal_infra"):
            friendly = classified.get("friendly") or "infrastructure error"
            recommendation = classified.get("recommendation") or ""
            return f"{friendly}\n→ {recommendation}".strip()
    return None


def _to_http_shape(ex: DotNetExecutableTestCase) -> ExecutableTestCase:
    """ה-Validator + Reporter + BugAgent עובדים על ExecutableTestCase (HTTP shape).
    כדי לשמור אותם בלי שינוי, אנחנו ממירים את ה-DotNetExecutableTestCase ל-shape תואם.
    """
    summary_url = "dotnet://" + ",".join(a.kind for a in ex.actions) if ex.actions else "about:blank"
    body = {"actions": [a.model_dump() for a in ex.actions]}
    return ExecutableTestCase(
        test_case_id=ex.test_case_id,
        ado_test_case_id=ex.ado_test_case_id,
        request=HttpRequestSpec(method="DOTNET", url=summary_url, body=body),
        expected_response=ResponseAssertion(status=ex.expected_status),
        source_text=ex.source_text,
        compiler_notes=ex.compiler_notes,
    )


async def _build_payloads(session: ChatSession, spec_text, emit):
    """שולח את ה-spec_text לסוכן Payload Builder ומקבל templates + field_catalog.

    מחזיר את ה-dict שהסוכן החזיר, או None אם:
      - DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT ריק
      - spec_text ריק
      - שגיאה / timeout — מודיע בלוג ב-emit + structlog
    """
    if not settings.DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT:
        await emit("שלב 2/" + str(TOTAL_STAGES) + " — Payload Builder לא מוגדר (DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT ריק). דילוג.")
        log.warning("payload_builder_skipped_no_endpoint")
        return None
    if not spec_text or not spec_text.strip():
        await emit("שלב 2/" + str(TOTAL_STAGES) + " — אין spec_text על ה-session. דילוג על Payload Builder.")
        log.warning("payload_builder_skipped_no_spec")
        return None

    bridge = PayloadBuilderBridge()
    # ★ העדפה: לשלוח את הקובץ המקורי (בייטים) ולא את הטקסט. הסוכן מקבל קובץ → flow מלא;
    # קבלת טקסט גולמי → flow מצומצם שמחזיר רק חלקים מהשדות (עם MISSING placeholders).
    has_bytes = bool(getattr(session, "spec_bytes", None))
    if has_bytes:
        await emit(f"שלב 2/{TOTAL_STAGES} — שולח קובץ ({len(session.spec_bytes):,} bytes, "
                   f"{session.spec_filename}) לסוכן Payload Builder...")
    else:
        await emit(f"שלב 2/{TOTAL_STAGES} — שולח spec_text ({len(spec_text)} תווים) לסוכן Payload Builder "
                   f"⚠ (אין קובץ מקורי — תוצאה עשויה להיות מצומצמת)")
    try:
        result = await bridge.generate(
            spec_text=spec_text,
            spec_bytes=getattr(session, "spec_bytes", None),
            spec_filename=getattr(session, "spec_filename", None),
            spec_content_type=getattr(session, "spec_content_type", None),
        )
    except PayloadBuilderError as e:
        await emit(f"  ⚠ Payload Builder failed: {str(e)[:200]}")
        log.warning("payload_builder_failed", error=str(e))
        return None
    except Exception as e:
        await emit(f"  ✗ Payload Builder exception: {str(e)[:200]}")
        log.exception("payload_builder_exception")
        return None

    tmpl_keys = list((result.get("templates") or {}).keys())
    target_keys = list((result.get("target_templates") or {}).keys())
    n_transforms = len(result.get("transformations") or {})
    await emit(f"  ✓ קיבל {len(tmpl_keys)} source templates ({', '.join(tmpl_keys) or '—'}), "
               f"{len(target_keys)} target templates, {n_transforms} transformations, "
               f"target_entity_type={result.get('target_entity_type') or '—'}")
    # שמירה ל-session + דיסק (לדיבוג)
    session.payload_templates = result
    session.payload_templates_file = _persist_payloads(session.session_id, result)
    return result


def _extract_key_built_from(payload_templates):
    """מחלץ key_built_from (נתיבי-מקור שה-target KEY בנוי מהם) מתשובת ה-Payload Builder.
    מחפש ב-target_templates[<action>].key_built_from, או key_built_from ברמה העליונה. None אם אין.
    משמש ל-unique-id format-agnostic ב-runner (לא קשיח ל-member_id)."""
    if not isinstance(payload_templates, dict):
        return None
    top = payload_templates.get("key_built_from")
    if isinstance(top, list) and top:
        return top
    tt = payload_templates.get("target_templates") or {}
    if isinstance(tt, dict):
        for tmpl in tt.values():
            if isinstance(tmpl, dict):
                kbf = tmpl.get("key_built_from")
                if isinstance(kbf, list) and kbf:
                    return kbf
    return None


_KEY_TARGET_FIELDS = {"entity_id", "scc_message_id", "_data.scc_message_id",
                      "root.entity_id", "_data.entity_id"}


def _extract_key_source_path(payload_templates):
    """מחלץ את נתיב-המקור (לוגי) שהופך ל-target KEY/entity_id/scc_message_id **verbatim**, מתוך
    ה-transformations של ה-Payload Builder. למשל {"MessageHeader.id": {"target_field_path":
    "_data.scc_message_id"}} → "MessageHeader.id". זה השדה שצריך להזריק בו ערך ייחודי כדי שה-KEY
    ביעד יהיה ייחודי (להבדיל מ-member_id שעובר טרנספורמציה). None אם לא נמצא."""
    if not isinstance(payload_templates, dict):
        return None
    tfs = payload_templates.get("transformations") or {}
    if isinstance(tfs, dict):
        for src_path, spec in tfs.items():
            if isinstance(spec, dict) and spec.get("target_field_path") in _KEY_TARGET_FIELDS:
                return str(src_path)
    return None


def _persist_payloads(session_id, payloads):
    """שומר את התשובה של Payload Builder ל-logs/payload_builder/<ts>_<sid>.json לדיבוג."""
    import datetime
    import json as _json
    from pathlib import Path

    logs_dir = Path("logs") / "payload_builder"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_sid = (session_id or "anon")[:12]
    fpath = logs_dir / f"{ts}_{safe_sid}.json"
    try:
        fpath.write_text(_json.dumps(payloads, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log.warning("payload_persist_failed", error=str(e))
    return str(fpath)


async def _await_human_approval(session: ChatSession, timeout_seconds: int) -> bool:
    fut: asyncio.Future = asyncio.get_running_loop().create_future()
    session.bugs_decision = fut
    try:
        return await asyncio.wait_for(fut, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        return False
    finally:
        session.bugs_decision = None


def _bug_summary(b) -> dict:
    return {
        "title": b.title,
        "severity": b.severity,
        "test_case_id": b.test_case_id,
        "ado_test_case_id": b.ado_test_case_id,
        "failure_reasons": b.failure_reasons,
    }
