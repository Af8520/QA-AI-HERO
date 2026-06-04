"""ESB Pipeline — Phase B (7 שלבים) אחרי שסוכן Copilot Studio סיים והעלה ל-ADO.

השלבים:
1. Pull test cases מ-ADO suite
2. Pull spec MD attachment מ-ADO suite
3. Smart Compiler — ממיר tcs ל-ExecutableTestCase מלאים (LLM call פר tc)
4. Execute requests (httpx)
5. Verify Kafka + Elastic (Playwright web consoles)
6. Validate responses + Bug analysis + human approval
7. Reporter — סיכום עברית
"""

from __future__ import annotations

import asyncio
import json
from typing import List, Tuple

from agents.bug_agent.ado_client import ADOClient
from agents.bug_agent.bug_agent import BugAgent
from agents.compiler.smart_compiler import SmartCompiler
from agents.reporter.reporter_agent import ReporterAgent
from agents.runner import get_runner
from agents.validator.validator_agent import ValidatorAgent
from config.logging_config import get_logger
from models.executable_test_case import ExecutableTestCase
from models.pipeline import PipelineResult
from models.test_case import TestCaseResult, TestStatus
from models.test_run import ValidationResult
from server.chat_session import ChatSession

log = get_logger(__name__)

TOTAL_STAGES = 7


async def run_esb_pipeline(session: ChatSession) -> PipelineResult:
    async def emit(text: str) -> None:
        await session.emit("progress", {"text": text})

    async def tl_step(step: int, status: str, label: str = "") -> None:
        """Timeline update for a Phase B step (1-7). status: pending|active|done|skipped|failed"""
        await session.emit("tl_step", {"step": step, "status": status, "label": label})

    async def tl_tc(idx: int, total: int, name: str, status: str, http_status: int = 0) -> None:
        """Timeline update for a single test case in step 4."""
        await session.emit("tl_tc", {
            "idx": idx, "total": total, "name": name,
            "status": status, "http_status": http_status,
        })

    async def emit_tc_detail(tc_id: str, request: dict, response: dict) -> None:
        """Send full request + response for UI display."""
        await session.emit("tc_detail", {
            "test_case_id": tc_id,
            "request": request,
            "response": response,
        })

    # Phase A done, Phase B starting
    await session.emit("tl_phase", {"phase": "B", "status": "active"})

    suite_id = session.suite_id or 0
    collection = session.postman_collection
    ado = ADOClient()

    # שלב 1 — מקור תסריטים: Foundry (in-memory) או ADO
    await tl_step(1, "active", "Pull test cases")
    if session.direct_test_cases:
        await emit(f"שלב 1/{TOTAL_STAGES} — משתמש ב-{len(session.direct_test_cases)} תסריטים שנוצרו ע\"י Foundry...")
        raw_cases = session.direct_test_cases
    else:
        await emit(f"שלב 1/{TOTAL_STAGES} — מושך תסריטים מ-ADO suite #{suite_id}...")
        raw_cases = await _fetch_test_cases(ado, suite_id)
        if not raw_cases:
            await emit("⚠ לא נמצאו test cases ב-ADO. ממשיך עם דמה ל-pipeline.")
            raw_cases = _mock_raw_cases(suite_id)
        await emit(f"נמצאו {len(raw_cases)} test cases.")

    await tl_step(1, "done", f"Pull test cases ({len(raw_cases)})")

    # שלב 2 — Spec MD: ב-Foundry mode נשתמש ב-spec_text הגולמי. ב-ADO mode נמשוך attachment.
    await tl_step(2, "active", "Spec MD")
    await emit(f"שלב 2/{TOTAL_STAGES} — מושך מסמך אפיון...")
    if session.direct_test_cases and session.spec_text:
        spec_md = session.spec_text  # ה-Compiler יעבוד עם ה-spec הגולמי כמ-MD
        await emit(f"  ✓ משתמש ב-spec הגולמי ({len(spec_md)} תווים)")
        await tl_step(2, "done", f"Spec ({len(spec_md)} chars)")
    else:
        spec_md = await _fetch_spec_md(ado, suite_id)
        if spec_md:
            await emit(f"  ✓ נטען MD מ-ADO ({len(spec_md)} תווים)")
            await tl_step(2, "done", f"Spec MD ({len(spec_md)} chars)")
        else:
            await emit("  ⚠ אין MD ב-suite — Compiler ירוץ ללא הקשר ספק (דיוק יורד)")
            await tl_step(2, "skipped", "No spec MD")

    # שלב 3 — Smart Compiler
    await tl_step(3, "active", f"Compile (0/{len(raw_cases)})")
    await emit(f"שלב 3/{TOTAL_STAGES} — מהדר {len(raw_cases)} תסריטים לבקשות HTTP...")
    compiler = SmartCompiler(spec_md=spec_md, collection=collection)
    executables: List[ExecutableTestCase] = []
    compile_failures = 0
    for raw in raw_cases:
        tc_label = raw.get("title") or f"TC-{raw.get('id')}"
        try:
            ex = await compiler.compile(raw)
            executables.append(ex)
            url_display = ex.request.url if ex.request.url else "(no url)"
            await emit(f"  ✓ {ex.test_case_id} → {ex.request.method} {url_display}")
        except Exception as e:
            # אל תפיל את כל ה-pipeline על תסריט אחד בעייתי
            compile_failures += 1
            log.warning("compile_failed", tc=tc_label, error=str(e))
            from models.executable_test_case import HttpRequestSpec
            from models.test_case import ResponseAssertion
            placeholder = ExecutableTestCase(
                test_case_id=tc_label,
                ado_test_case_id=raw.get("id"),
                request=HttpRequestSpec(method="GET", url="about:blank"),
                expected_response=ResponseAssertion(status=0),
                source_text=raw.get("text") or "",
                compiler_notes=f"Compile failed: {str(e)[:200]}",
            )
            executables.append(placeholder)
            await emit(f"  ✗ {tc_label} → שגיאת קומפילציה: {str(e)[:100]}")
    if compile_failures:
        await emit(f"⚠ {compile_failures} תסריטים נכשלו בקומפילציה (יסומנו BLOCKED בריצה)")
    compile_status = "done" if compile_failures == 0 else ("failed" if compile_failures == len(raw_cases) else "done")
    await tl_step(3, compile_status, f"Compile ({len(raw_cases) - compile_failures}/{len(raw_cases)} OK)")

    # שלב 4 — execute
    await tl_step(4, "active", f"Execute (0/{len(executables)})")
    await emit(f"שלב 4/{TOTAL_STAGES} — מריץ {len(executables)} בקשות...")
    runner = get_runner()
    results: List[TestCaseResult] = []
    for i, ex in enumerate(executables, 1):
        # אל תקרא ל-API אם ה-URL הוא about:blank (placeholder של compile failure)
        if ex.request.url == "about:blank":
            from models.test_case import StepResult
            results.append(TestCaseResult(
                test_case_id=ex.test_case_id,
                ado_test_case_id=ex.ado_test_case_id,
                status=TestStatus.BLOCKED,
                step_results=[StepResult(
                    step="(compile failed)",
                    expected_result="(n/a)",
                    actual_result=ex.compiler_notes or "compile error",
                    status=TestStatus.BLOCKED,
                    error_message=ex.compiler_notes,
                )],
                duration_seconds=0.0,
                api_response={"error": ex.compiler_notes or "compile failed"},
            ))
            await emit(f"  ⊘ ({i}/{len(executables)}) {ex.test_case_id} — דילוג (compile failed)")
            await tl_tc(i, len(executables), ex.test_case_id, "skipped")
            continue
        try:
            r = await runner.execute(ex)
            results.append(r)
            actual_status = (r.api_response or {}).get("status", 0) or 0
            expected = ex.expected_response.status if ex.expected_response else 0
            tc_status = "done" if actual_status == expected else ("failed" if actual_status > 0 else "blocked")
            await emit(f"  → ({i}/{len(executables)}) {ex.test_case_id} → HTTP {actual_status}")
            await tl_tc(i, len(executables), ex.test_case_id, tc_status, int(actual_status))
            # שלח request+response למסך — להצגה אקספנדבילית
            await emit_tc_detail(ex.test_case_id, {
                "method": ex.request.method,
                "url": ex.request.url,
                "headers": ex.request.headers or {},
                "body": ex.request.body,
                "expected_status": expected,
            }, {
                "status": actual_status,
                "headers": (r.api_response or {}).get("headers") or {},
                "body": (r.api_response or {}).get("body"),
                "body_text": (r.api_response or {}).get("body_text"),
                "duration_ms": (r.api_response or {}).get("duration_ms"),
            })
            await tl_step(4, "active", f"Execute ({i}/{len(executables)})")
        except Exception as e:
            log.warning("execute_failed", tc=ex.test_case_id, error=str(e))
            from models.test_case import StepResult
            results.append(TestCaseResult(
                test_case_id=ex.test_case_id,
                ado_test_case_id=ex.ado_test_case_id,
                status=TestStatus.BLOCKED,
                step_results=[StepResult(
                    step=f"{ex.request.method} {ex.request.url}",
                    expected_result=f"HTTP {ex.expected_response.status if ex.expected_response else '?'}",
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
                "method": ex.request.method,
                "url": ex.request.url,
                "headers": ex.request.headers or {},
                "body": ex.request.body,
            }, {"error": str(e)})

    await tl_step(4, "done", f"Execute ({len(executables)} done)")

    # שלב 5 — Kafka + Elastic
    await tl_step(5, "active", "Verify Kafka + Elastic")
    await emit(f"שלב 5/{TOTAL_STAGES} — מאמת Kafka + Elastic...")
    for ex, res in zip(executables, results):
        if ex.kafka_assertion and res.status != TestStatus.BLOCKED:
            try:
                res.kafka_result = await runner.verify_kafka(ex)
            except Exception as e:
                log.warning("kafka_verify_failed", tc=ex.test_case_id, error=str(e))
                res.kafka_result = {"found": False, "error": str(e)}
        if ex.elastic_assertion and res.status != TestStatus.BLOCKED:
            try:
                res.elastic_result = await runner.verify_elastic(ex)
            except Exception as e:
                log.warning("elastic_verify_failed", tc=ex.test_case_id, error=str(e))
                res.elastic_result = {"hits": 0, "errors": 0, "error": str(e)}

    await tl_step(5, "done", "Verify Kafka + Elastic")

    # שלב 6 — validation + bug analysis + human approval
    await tl_step(6, "active", "Validate + Bugs")
    await emit(f"שלב 6/{TOTAL_STAGES} — מאמת תשובות ופותח bugs...")
    validator = ValidatorAgent()
    validations: List[ValidationResult] = await validator.validate_all(list(zip(executables, results)))

    failures: List[Tuple[ExecutableTestCase, TestCaseResult, ValidationResult]] = [
        (ex, r, v)
        for ex, r, v in zip(executables, results, validations)
        if v.overall_status in (TestStatus.FAILED, TestStatus.BLOCKED)
    ]

    opened_bugs_models = []
    if failures:
        bug_agent = BugAgent()
        bugs = await bug_agent.analyze(failures)
        await emit(f"זוהו {len(bugs)} באגים פוטנציאליים — ממתין לאישור...")
        await session.emit(
            "bugs_for_approval",
            {"bugs": [_bug_summary(b) for b in bugs]},
        )
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

    # שלב 7 — reporter
    await tl_step(7, "active", "Reporter")
    await emit(f"שלב 7/{TOTAL_STAGES} — מכין סיכום...")
    reporter = ReporterAgent()
    report = await reporter.generate(
        suite_id=suite_id,
        us_number=None,
        test_cases=executables,
        results=results,
        validations=validations,
        opened_bugs=opened_bugs_models,
    )
    await tl_step(7, "done", "Reporter")
    await session.emit("tl_phase", {"phase": "B", "status": "done"})
    await emit("הסתיים.")
    return report


async def _fetch_test_cases(ado: ADOClient, suite_id: int):
    if not ado.enabled or not suite_id:
        return []
    try:
        return await ado.get_test_cases_in_suite_by_id(suite_id)
    except Exception as e:
        log.warning("ado_fetch_failed", error=str(e))
        return []


async def _fetch_spec_md(ado: ADOClient, suite_id: int):
    if not ado.enabled or not suite_id:
        return None
    try:
        return await ado.get_suite_attachment(suite_id, name_pattern="*.md")
    except Exception as e:
        log.warning("ado_md_fetch_failed", error=str(e))
        return None


def _mock_raw_cases(suite_id: int):
    return [
        {
            "id": 1001,
            "title": "TC-001 flow תקין",
            "text": "שלח בקשת admission עם נתונים תקינים. תוצאה צפויה: 200, מסר ב-Kafka, log באלסטיק.",
        },
        {
            "id": 1002,
            "title": "TC-002 שדה חובה חסר",
            "text": "שלח admission ללא שדה patient_name (שדה חובה). תוצאה צפויה: 400.",
        },
    ]


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
