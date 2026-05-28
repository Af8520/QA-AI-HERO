"""ADO REST client — שולף test cases מ-suite, attachments, ופותח bugs."""

from __future__ import annotations

import base64
import fnmatch
from typing import Any, Dict, List, Optional

import httpx

from config.logging_config import get_logger
from config.settings import settings
from models.bug import BugReport

log = get_logger(__name__)

API_VERSION = "7.1-preview.3"


class ADOClient:
    def __init__(
        self,
        org_url: Optional[str] = None,
        project: Optional[str] = None,
        pat: Optional[str] = None,
    ) -> None:
        self.org_url = (org_url or settings.ADO_ORG_URL or "").rstrip("/")
        self.project = project or settings.ADO_PROJECT or ""
        self.pat = pat or settings.ADO_PAT or ""
        if self.pat:
            token = base64.b64encode(f":{self.pat}".encode()).decode()
            self._headers = {"Authorization": f"Basic {token}"}
        else:
            self._headers = {}
        self.enabled = bool(self.org_url and self.project and self.pat)

    @property
    def base(self) -> str:
        return f"{self.org_url}/{self.project}/_apis"

    async def get_test_cases_in_suite(self, plan_id: int, suite_id: int) -> List[Dict[str, Any]]:
        """מחזיר רשימת test cases (work items מלאים) מתוך suite."""
        if not self.enabled:
            log.warning("ado_disabled")
            return []
        url = f"{self.base}/testplan/Plans/{plan_id}/Suites/{suite_id}/TestCase?api-version={API_VERSION}"
        async with httpx.AsyncClient(verify=settings.VERIFY_SSL, timeout=30) as client:
            r = await client.get(url, headers=self._headers)
            r.raise_for_status()
            data = r.json()
        wi_ids = [item["workItem"]["id"] for item in data.get("value", []) if item.get("workItem")]
        return await self._get_work_items(wi_ids)

    async def get_test_cases_in_suite_by_id(self, suite_id: int) -> List[Dict[str, Any]]:
        """גרסה פשוטה — מנסה לזהות plan אוטומטית או שולפת ישירות מ-Test Suite work items."""
        if not self.enabled:
            return []
        # ננסה את ה-API הישן יותר (TestPlan/Suites/{id}/testcases) שלא דורש plan_id
        url = f"{self.org_url}/{self.project}/_apis/test/Plans/0/suites/{suite_id}/testcases?api-version=5.0"
        async with httpx.AsyncClient(verify=settings.VERIFY_SSL, timeout=30) as client:
            try:
                r = await client.get(url, headers=self._headers)
                r.raise_for_status()
                data = r.json()
                wi_ids = [int(item["testCase"]["id"]) for item in data.get("value", []) if item.get("testCase")]
            except httpx.HTTPError as e:
                log.warning("ado_suite_fetch_failed", error=str(e))
                return []
        return await self._get_work_items(wi_ids)

    async def _get_work_items(self, ids: List[int]) -> List[Dict[str, Any]]:
        if not ids:
            return []
        ids_str = ",".join(str(i) for i in ids)
        url = (
            f"{self.base}/wit/workitems?ids={ids_str}"
            f"&fields=System.Id,System.Title,System.Description,Microsoft.VSTS.TCM.Steps"
            f"&api-version={API_VERSION}"
        )
        async with httpx.AsyncClient(verify=settings.VERIFY_SSL, timeout=30) as client:
            r = await client.get(url, headers=self._headers)
            r.raise_for_status()
            data = r.json()
        out: List[Dict[str, Any]] = []
        for wi in data.get("value", []):
            f = wi.get("fields", {})
            text_blob = "\n".join(
                str(v) for v in [
                    f.get("System.Title"),
                    f.get("System.Description"),
                    f.get("Microsoft.VSTS.TCM.Steps"),
                ] if v
            )
            out.append({
                "id": wi.get("id"),
                "title": f.get("System.Title", ""),
                "text": text_blob,
            })
        return out

    async def get_suite_attachment(
        self,
        suite_id: int,
        name_pattern: str = "*.md",
    ) -> Optional[str]:
        """שולף attachment של ה-Test Suite (work item) לפי pattern. מחזיר תוכן כ-text או None.

        Test Suite ב-ADO הוא work item רגיל — attachments נגישים דרך
        /wit/workitems/{suite_id}?$expand=relations
        ה-relations מסוג AttachedFile מצביעים ל-/wit/attachments/{id} עם ה-binary.
        """
        if not self.enabled:
            log.warning("ado_disabled_for_attachment")
            return None
        url = f"{self.base}/wit/workitems/{suite_id}?$expand=relations&api-version={API_VERSION}"
        async with httpx.AsyncClient(verify=settings.VERIFY_SSL, timeout=30) as client:
            try:
                r = await client.get(url, headers=self._headers)
                r.raise_for_status()
                wi = r.json()
            except httpx.HTTPError as e:
                log.warning("ado_suite_workitem_fetch_failed", suite_id=suite_id, error=str(e))
                return None

            # סנן attachments לפי name pattern
            for rel in wi.get("relations", []) or []:
                if rel.get("rel") != "AttachedFile":
                    continue
                attrs = rel.get("attributes", {}) or {}
                name = attrs.get("name", "")
                if not fnmatch.fnmatch(name.lower(), name_pattern.lower()):
                    continue
                attachment_url = rel.get("url")
                if not attachment_url:
                    continue
                try:
                    ar = await client.get(attachment_url, headers=self._headers)
                    ar.raise_for_status()
                    log.info("ado_attachment_fetched", suite_id=suite_id, name=name, bytes=len(ar.content))
                    return ar.text
                except httpx.HTTPError as e:
                    log.warning("ado_attachment_download_failed", url=attachment_url, error=str(e))
                    continue
        log.info("ado_no_matching_attachment", suite_id=suite_id, pattern=name_pattern)
        return None

    async def create_bug(self, bug: BugReport) -> Optional[int]:
        """פותח Bug חדש ב-ADO ומחזיר את ה-ID."""
        if not self.enabled:
            log.warning("ado_create_bug_skipped")
            return None
        url = f"{self.base}/wit/workitems/$Bug?api-version={API_VERSION}"
        body = [
            {"op": "add", "path": "/fields/System.Title", "value": bug.title},
            {"op": "add", "path": "/fields/System.Description", "value": _format_description(bug)},
            {"op": "add", "path": "/fields/Microsoft.VSTS.Common.Severity", "value": _severity_to_ado(bug.severity)},
        ]
        async with httpx.AsyncClient(verify=settings.VERIFY_SSL, timeout=30) as client:
            r = await client.post(
                url,
                headers={**self._headers, "Content-Type": "application/json-patch+json"},
                json=body,
            )
            r.raise_for_status()
            data = r.json()
        bug_id = data.get("id")
        log.info("ado_bug_created", bug_id=bug_id, title=bug.title)
        return bug_id

    async def create_bugs(self, bugs: List[BugReport]) -> List[int]:
        out: List[int] = []
        for b in bugs:
            try:
                bid = await self.create_bug(b)
                if bid:
                    b.ado_bug_id = bid
                    out.append(bid)
            except Exception as e:
                log.warning("ado_bug_create_failed", error=str(e), title=b.title)
        return out


def _format_description(bug: BugReport) -> str:
    parts = [
        f"<b>Test Case:</b> {bug.test_case_id} (ADO #{bug.ado_test_case_id or '-'})",
        f"<b>Severity:</b> {bug.severity}",
        "<br><b>סיבות כשל:</b>",
        "<ul>" + "".join(f"<li>{r}</li>" for r in bug.failure_reasons) + "</ul>",
    ]
    if bug.repro_steps:
        parts.append("<b>צעדי שחזור:</b>")
        parts.append("<ol>" + "".join(f"<li>{s}</li>" for s in bug.repro_steps) + "</ol>")
    if bug.suggested_fix:
        parts.append(f"<b>הצעה לתיקון:</b> {bug.suggested_fix}")
    return "<br>".join(parts)


def _severity_to_ado(sev: str) -> str:
    mapping = {"Critical": "1 - Critical", "High": "2 - High", "Medium": "3 - Medium", "Low": "4 - Low"}
    return mapping.get(sev, "3 - Medium")
