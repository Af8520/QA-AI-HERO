"""MSAL token acquisition עבור Copilot Studio.

תומך 4 מצבים:
- interactive: פותח דפדפן (טוב לפיתוח לוקלי)
- device_code: מציג code, פתח https://microsoft.com/devicelogin (טוב לשרתים headless)
- client_secret: app-only (דורש admin consent)
- token: paste ידני (לבדיקות מהירות)
"""

from __future__ import annotations

import threading
import time
from typing import List, Optional

from config.logging_config import get_logger
from config.settings import settings

log = get_logger(__name__)

# Cache פשוט בזיכרון: token + expires_at
_cached_token: Optional[str] = None
_cached_exp: float = 0.0
_lock = threading.Lock()


def get_access_token(scope: str) -> str:
    """מחזיר Bearer token תקף ל-Copilot Studio. Cache פנימי."""
    global _cached_token, _cached_exp
    with _lock:
        now = time.time()
        if _cached_token and now < _cached_exp - 60:
            return _cached_token
        token, expires_in = _acquire_token([scope])
        _cached_token = token
        _cached_exp = now + expires_in
        log.info("msal_token_acquired", mode=settings.COPILOT_AUTH_MODE, expires_in=expires_in)
        return _cached_token


def _acquire_token(scopes: List[str]) -> tuple[str, int]:
    mode = (settings.COPILOT_AUTH_MODE or "interactive").lower()

    if mode == "token":
        if not settings.COPILOT_TOKEN:
            raise RuntimeError("COPILOT_AUTH_MODE=token אבל COPILOT_TOKEN ריק")
        # במצב הזה היוזר אחראי ל-refresh; נחזיר tokens תוקף ארוך מלאכותית
        return settings.COPILOT_TOKEN, 3600

    try:
        import msal  # type: ignore[import-not-found]
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("msal לא מותקן. הרץ: pip install msal") from e

    authority = f"https://login.microsoftonline.com/{settings.COPILOT_TENANT_ID}"
    client_id = settings.COPILOT_APP_CLIENT_ID

    if mode == "client_secret":
        if not settings.COPILOT_CLIENT_SECRET:
            raise RuntimeError("COPILOT_AUTH_MODE=client_secret אבל COPILOT_CLIENT_SECRET ריק")
        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=settings.COPILOT_CLIENT_SECRET,
            authority=authority,
        )
        result = app.acquire_token_for_client(scopes=scopes)
    elif mode == "device_code":
        app = msal.PublicClientApplication(client_id=client_id, authority=authority)
        flow = app.initiate_device_flow(scopes=scopes)
        if "user_code" not in flow:
            raise RuntimeError(f"device flow כשל: {flow.get('error_description')}")
        print("\n" + "=" * 60)
        print(flow["message"])
        print("=" * 60 + "\n")
        result = app.acquire_token_by_device_flow(flow)
    else:  # interactive
        app = msal.PublicClientApplication(client_id=client_id, authority=authority)
        # ננסה קודם cache שקט
        accounts = app.get_accounts()
        result = None
        if accounts:
            result = app.acquire_token_silent(scopes, account=accounts[0])
        if not result:
            result = app.acquire_token_interactive(scopes=scopes)

    if "access_token" not in result:
        err = result.get("error_description") or result.get("error") or str(result)
        raise RuntimeError(f"MSAL acquire_token נכשל: {err}")
    return result["access_token"], int(result.get("expires_in", 3600))


def reset_cache() -> None:
    """לבדיקות / לוגאאוט."""
    global _cached_token, _cached_exp
    with _lock:
        _cached_token = None
        _cached_exp = 0.0
