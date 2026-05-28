"""ReporterAgent — סיכום עברית של הרצת הבדיקות."""

from __future__ import annotations

from typing import List

from models.bug import BugReport
from models.pipeline import PipelineResult
from models.test_case import TestCase, TestCaseResult, TestStatus
from models.test_run import ValidationResult


class ReporterAgent:
    async def generate(
        self,
        suite_id: int,
        us_number: str | None,
        test_cases: List[TestCase],
        results: List[TestCaseResult],
        validations: List[ValidationResult],
        opened_bugs: List[BugReport],
    ) -> PipelineResult:
        total = len(results)
        passed = sum(1 for v in validations if v.overall_status == TestStatus.PASSED)
        failed = sum(1 for v in validations if v.overall_status == TestStatus.FAILED)
        blocked = sum(1 for v in validations if v.overall_status == TestStatus.BLOCKED)

        bug_ids = [b.ado_bug_id for b in opened_bugs if b.ado_bug_id]

        summary_lines = [
            f"סיכום הרצה — Suite #{suite_id}" + (f" (US-{us_number})" if us_number else ""),
            f"סה\"כ תסריטים: {total} | עברו: {passed} | נכשלו: {failed} | חסומים: {blocked}",
            "",
        ]
        if failed or blocked:
            summary_lines.append("פירוט כשלים:")
            for v in validations:
                if v.overall_status in (TestStatus.FAILED, TestStatus.BLOCKED):
                    reasons = "; ".join(v.failure_reasons) or "ללא פירוט"
                    summary_lines.append(f"  • {v.test_case_id}: {reasons}")
            summary_lines.append("")
        if opened_bugs:
            summary_lines.append("Bugs שנפתחו:")
            for b in opened_bugs:
                summary_lines.append(
                    f"  • #{b.ado_bug_id or '(לא נפתח)'} [{b.severity}] {b.title}"
                )

        return PipelineResult(
            suite_id=suite_id,
            us_number=us_number,
            total=total,
            passed=passed,
            failed=failed,
            blocked=blocked,
            bugs_opened=bug_ids,
            summary_hebrew="\n".join(summary_lines),
        )
