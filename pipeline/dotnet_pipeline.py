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
from typing import List, Tuple

from agents.bug_agent.ado_client import ADOClient
from agents.bug_agent.bug_agent import BugAgent
from agents.compiler.dotnet_compiler import DotNetCompiler
from agents.reporter.reporter_agent import ReporterAgent
from agents.runner.dotnet_runner import DotNetRunner
from agents.validator.validator_agent import ValidatorAgent
from config.logging_config import get_logger
from models.dotnet_test_case import DotNetExecutableTestCase
from models.executable_test_case import ExecutableTestCase, HttpRequestSpec
from models.pipeline import PipelineResult
from models.test_case import ResponseAssertion, StepResult, TestCaseResult, TestStatus
from models.test_run import ValidationResult
from server.chat_session import ChatSession

log = get_logger(__name__)

TOTAL_STAGES = 7


async def run_dotnet_pipeline(session: ChatSession) -> PipelineResult:
    async def emit(text: str) -> None:
        await session.emit("progress", {"text": text})

    async def tl_step(step: int, status: str, label: str = "") -> None:
        await session.emit("tl_step", {"step": step, "status": status, "label": label})

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

    # שלב 2 — Spec MD
    await tl_step(2, "active", "Spec MD")
    spec_md = session.spec_text
    if spec_md:
        await emit(f"שלב 2/{TOTAL_STAGES} — משתמש ב-spec ({len(spec_md)} תווים)")
        await tl_step(2, "done", f"Spec ({len(spec_md)} chars)")
    else:
        await emit(f"שלב 2/{TOTAL_STAGES} — אין spec MD")
        await tl_step(2, "skipped", "No spec MD")

    # שלב 3 — DotNet Compiler
    await tl_step(3, "active", f"Compile (0/{len(raw_cases)})")
    await emit(f"שלב 3/{TOTAL_STAGES} — מהדר {len(raw_cases)} תסריטים ל-actions...")
    compiler = DotNetCompiler(spec_md=spec_md)
    executables: List[DotNetExecutableTestCase] = []
    compile_failures = 0
    for raw in raw_cases:
        tc_label = raw.get("title") or f"TC-{raw.get('id')}"
        try:
            ex = await compiler.compile(raw)
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
    # נשמור גם רשימה מקבילה של ExecutableTestCase HTTP-shaped כדי שה-Validator הגנרי יוכל לעבוד
    http_shaped_for_validator: List[ExecutableTestCase] = []
    for i, ex in enumerate(executables, 1):
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
            r = await runner.execute(ex)
            results.append(r)
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
