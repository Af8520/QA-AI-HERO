"""PayloadBuilderBridge — שיחה server-side עם סוכן Copilot Studio (Custom website channel).

הסוכן הזה מקבל טקסט אפיון ומחזיר JSON שמכיל templates + field_catalog.
תקשורת דרך DirectLine REST (אותו פרוטוקול שה-WebChat משתמש בו בדפדפן),
כי Custom website channel הוא no-auth — אין צורך ב-MSAL/Copilots.Invoke.

זרימה:
1. GET regionalchannelsettings → DirectLine URL
2. GET token endpoint → DirectLine token
3. POST conversations → conversationId
4. POST activity startConversation event (כמו הדפדפן)
5. POST activity message עם spec_text
6. GET activities ב-polling עד שמסר מהבוט מכיל JSON object תקין
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any, Dict, Optional

import httpx

from config.logging_config import get_logger
from config.settings import settings

log = get_logger(__name__)


class PayloadBuilderError(Exception):
    """שגיאת תקשורת/פרסור עם סוכן ה-Payload Builder."""


class PayloadBuilderBridge:
    def __init__(
        self,
        token_endpoint: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> None:
        self.token_endpoint = token_endpoint or settings.DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT
        self.timeout_seconds = timeout_seconds or settings.DOTNET_PAYLOAD_BUILDER_TIMEOUT_SECONDS
        self.enabled = bool(self.token_endpoint)

    async def generate(self, spec_text: str) -> Dict[str, Any]:
        """שולח spec_text לסוכן ומחזיר את ה-JSON המנותח.

        זורק PayloadBuilderError במקרה של כשל.
        """
        if not self.enabled:
            raise PayloadBuilderError("DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT לא מוגדר")
        if not spec_text or not spec_text.strip():
            raise PayloadBuilderError("spec_text ריק — לא ניתן לבקש templates")

        async with httpx.AsyncClient(verify=settings.VERIFY_SSL, timeout=30.0) as client:
            # 1. Regional channel settings (כמו שה-WebChat בדפדפן עושה)
            directline_url = await self._fetch_directline_url(client)
            log.info("payload_builder_directline_url", url=directline_url)

            # 2. Token
            token = await self._fetch_token(client)
            log.info("payload_builder_token_fetched", token_len=len(token))

            # 3. Conversation
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            conv_id = await self._start_conversation(client, directline_url, headers)
            log.info("payload_builder_conversation_started", conv_id=conv_id)

            # 4. startConversation event (מטריג את ה-greeting של הסוכן)
            await self._post_activity(client, directline_url, conv_id, headers, {
                "type": "event",
                "name": "startConversation",
                "from": {"id": "qa-ai-hero-server", "role": "user"},
                "channelData": {"postBack": True},
            })

            # 5. שליחת ה-spec_text כטקסט (אם הסוכן מצפה לקובץ, נשלח את התוכן כטקסט גולמי)
            await self._post_activity(client, directline_url, conv_id, headers, {
                "type": "message",
                "text": spec_text,
                "from": {"id": "qa-ai-hero-server", "role": "user"},
            })

            # 6. Polling עד שמגיע JSON object תקין מהבוט
            return await self._poll_for_json(client, directline_url, conv_id, headers)

    # ============================================================
    # Helpers
    # ============================================================

    async def _fetch_directline_url(self, client: httpx.AsyncClient) -> str:
        """מחלץ את ה-DirectLine URL מ-regional channel settings."""
        # מבנה ה-token endpoint: https://{env}.{region}.environment.api.powerplatform.com/powervirtualagents/.../directline/token?api-version=...
        env_marker = "/powervirtualagents"
        idx = self.token_endpoint.find(env_marker)
        if idx < 0:
            raise PayloadBuilderError(f"token endpoint לא בפורמט מוכר: {self.token_endpoint[:80]}")
        env_endpoint = self.token_endpoint[:idx]
        api_version_match = re.search(r"api-version=([^&]+)", self.token_endpoint)
        api_version = api_version_match.group(1) if api_version_match else "2022-03-01-preview"
        reg_url = f"{env_endpoint}/powervirtualagents/regionalchannelsettings?api-version={api_version}"
        try:
            r = await client.get(reg_url)
            r.raise_for_status()
            data = r.json()
        except httpx.HTTPError as e:
            raise PayloadBuilderError(f"regional settings fetch failed: {e}")
        url = (data.get("channelUrlsById") or {}).get("directline")
        if not url:
            raise PayloadBuilderError("DirectLine URL לא נמצא ב-regional settings")
        return url

    async def _fetch_token(self, client: httpx.AsyncClient) -> str:
        try:
            r = await client.get(self.token_endpoint)
            r.raise_for_status()
            info = r.json()
        except httpx.HTTPError as e:
            raise PayloadBuilderError(f"token fetch failed: {e}")
        token = info.get("token")
        if not token:
            raise PayloadBuilderError("token ריק בתשובה")
        return token

    async def _start_conversation(
        self,
        client: httpx.AsyncClient,
        directline_url: str,
        headers: Dict[str, str],
    ) -> str:
        url = f"{directline_url}v3/directline/conversations"
        try:
            r = await client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json()
        except httpx.HTTPError as e:
            raise PayloadBuilderError(f"conversation start failed: {e}")
        conv_id = data.get("conversationId")
        if not conv_id:
            raise PayloadBuilderError("conversationId חסר בתשובה")
        return conv_id

    async def _post_activity(
        self,
        client: httpx.AsyncClient,
        directline_url: str,
        conv_id: str,
        headers: Dict[str, str],
        activity: Dict[str, Any],
    ) -> None:
        url = f"{directline_url}v3/directline/conversations/{conv_id}/activities"
        try:
            r = await client.post(url, headers=headers, json=activity)
            r.raise_for_status()
        except httpx.HTTPError as e:
            raise PayloadBuilderError(f"post activity failed: {e}")

    async def _poll_for_json(
        self,
        client: httpx.AsyncClient,
        directline_url: str,
        conv_id: str,
        headers: Dict[str, str],
    ) -> Dict[str, Any]:
        """polling עד שאחת מהודעות הבוט מכילה JSON object שמכיל templates/source_topic.

        מצטבר את כל תוכן ההודעות לטקסט אחד כדי לתפוס JSON שמתפצל על כמה הודעות.
        """
        url = f"{directline_url}v3/directline/conversations/{conv_id}/activities"
        watermark: Optional[str] = None
        deadline = time.monotonic() + self.timeout_seconds
        accumulated = ""

        while time.monotonic() < deadline:
            params = {"watermark": watermark} if watermark else {}
            try:
                r = await client.get(url, headers=headers, params=params)
                r.raise_for_status()
                data = r.json()
            except httpx.HTTPError as e:
                log.warning("payload_builder_poll_failed", error=str(e))
                await asyncio.sleep(2.0)
                continue

            watermark = data.get("watermark") or watermark
            for activity in data.get("activities", []) or []:
                if (activity.get("from") or {}).get("role") != "bot":
                    continue
                if activity.get("type") != "message":
                    continue
                text = activity.get("text") or ""
                if text:
                    accumulated += text + "\n"
                # נסה לחלץ JSON object שמכיל את השדות שאנחנו מצפים להם
                obj = _try_extract_json_object(accumulated, required_keys=("templates", "source_topic"))
                if obj is not None:
                    log.info("payload_builder_received", text_len=len(text),
                             template_keys=list((obj.get("templates") or {}).keys()))
                    return obj
            await asyncio.sleep(2.0)

        raise PayloadBuilderError(
            f"Timeout {self.timeout_seconds}s — אין JSON בתשובה. "
            f"accumulated {len(accumulated)} chars (first 200: {accumulated[:200]!r})"
        )


# ============================================================
# Pure helpers — testable without DirectLine
# ============================================================

def _try_extract_json_object(text: str, required_keys: tuple = ()) -> Optional[Dict[str, Any]]:
    """מחזיר JSON object מתוך טקסט. עדיפות ל-fenced ```json {...}``` ואז raw.

    אם required_keys ניתנו — מחזיר רק אובייקט שמכיל לפחות אחד מהם.
    """
    if not text:
        return None

    candidates = []

    # 1. ```json {...}```
    for m in re.finditer(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text):
        candidates.append(m.group(1))

    # 2. raw — חיפוש "אובייקט" שנראה גדול. מנסים את הראשון
    if "{" in text:
        first = text.find("{")
        last = text.rfind("}")
        if first >= 0 and last > first:
            candidates.append(text[first:last + 1])

    for candidate in candidates:
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        if required_keys and not any(k in obj for k in required_keys):
            continue
        return obj

    return None
