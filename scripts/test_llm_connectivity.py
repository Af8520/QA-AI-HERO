"""דיאגנוסטיקת נגישות ל-LLM (Azure OpenAI / Foundry) — להרצה **ב-VDI**.

למה: כשה-proxy הארגוני (McAfee Web Gateway) חוסם את ה-host של ה-LLM, כל קריאות הקומפיילר נכשלות
וה-UI מציג "compile failed" מטעה. הסקריפט הזה בודק בדיוק מה נגיש ומה לא, בלי להריץ את כל ה-pipeline.

מה הוא עושה, לכל וריאנט-דומיין של אותו רסורס
(<resource>.openai.azure.com / .services.ai.azure.com / .cognitiveservices.azure.com):
  1. DNS — האם ה-host נפתר בכלל (socket.getaddrinfo).
  2. POST אמיתי ל-/openai/v1/chat/completions?api-version=... עם ה-key וה-deployment מ-.env.
  3. מסווג את התוצאה: OK (תשובת-מודל) / auth(401/403) / model(404) / proxy-block (McAfee/HTML) /
     connectivity (timeout/DNS/SSL).

מסקנה: הדומיין שמחזיר OK (או אפילו auth/model — כלומר ה-host נגיש) הוא זה שצריך להיכנס ל-
AZURE_OPENAI_ENDPOINT. אם אף אחד לא נגיש → פנייה ל-IT ל-whitelist.

הרצה:  py scripts/test_llm_connectivity.py
"""

from __future__ import annotations

import json
import socket
import sys
from pathlib import Path
from urllib.parse import urlsplit

# console של Windows legacy (cp1255) לא מקודד emoji/utf-8 → קריסה. כופים UTF-8 עם fallback בטוח.
# (ה-terminal של VS Code כבר UTF-8 → נראה מצוין; console ישן → 'replace' במקום קריסה.)
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ודא שהשורש ב-sys.path כדי לייבא config
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config.settings import settings  # noqa: E402

_DOMAIN_SUFFIXES = ("openai.azure.com", "services.ai.azure.com", "cognitiveservices.azure.com")


def _resource_name(endpoint: str) -> str:
    """מחלץ את שם-הרסורס מה-endpoint: https://<resource>.<suffix>/ → <resource>."""
    host = urlsplit(endpoint).hostname or ""
    return host.split(".")[0] if host else ""


def _candidate_hosts(endpoint: str) -> list:
    res = _resource_name(endpoint)
    if not res:
        return []
    return [f"{res}.{suf}" for suf in _DOMAIN_SUFFIXES]


def _dns(host: str):
    try:
        infos = socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
        ips = sorted({i[4][0] for i in infos})
        return True, ips
    except Exception as e:  # noqa: BLE001
        return False, str(e)


def _looks_like_block_page(text: str) -> bool:
    low = (text or "").lower()
    return any(m in low for m in ("<html", "<!doctype", "mcafee", "web gateway", "not resolvable"))


def _probe_host(host: str, api_version: str, key: str, deployment: str, verify_ssl: bool) -> dict:
    """POST אמיתי ל-route ה-v1 על ה-host. מחזיר dict עם verdict + פרטים."""
    import httpx

    url = f"https://{host}/openai/v1/chat/completions?api-version={api_version}"
    body = {
        "model": deployment,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 1,
    }
    headers = {"api-key": key, "Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    try:
        with httpx.Client(verify=verify_ssl, timeout=20.0) as c:
            r = c.post(url, headers=headers, json=body)
    except Exception as e:  # noqa: BLE001
        return {"verdict": "connectivity", "detail": f"{type(e).__name__}: {str(e)[:200]}"}

    snippet = (r.text or "")[:300]
    if _looks_like_block_page(r.text):
        return {"verdict": "proxy-block", "http": r.status_code,
                "detail": f"דף חסימה של proxy/McAfee (HTML). {snippet[:120]}"}
    if r.status_code in (401, 403):
        return {"verdict": "auth", "http": r.status_code, "detail": "host נגיש; אימות נכשל (בדוק KEY)."}
    if r.status_code == 404:
        return {"verdict": "model", "http": r.status_code,
                "detail": "host נגיש; deployment/route לא נמצא (בדוק DEPLOYMENT/USE_V1/api-version)."}
    if r.status_code < 300:
        return {"verdict": "OK", "http": r.status_code, "detail": "תשובת-מודל תקינה — host+key+deployment עובדים."}
    # 2xx-3xx לא, ולא 401/403/404 — נסה לקרוא error מובנה
    try:
        err = r.json().get("error", {})
        msg = err.get("message") or json.dumps(err)[:200]
    except Exception:  # noqa: BLE001
        msg = snippet
    return {"verdict": "other", "http": r.status_code, "detail": f"HTTP {r.status_code}: {msg[:200]}"}


def main() -> int:
    endpoint = (settings.AZURE_OPENAI_ENDPOINT or "").strip()
    key = settings.AZURE_OPENAI_KEY or ""
    deployment = settings.compiler_deployment
    api_version = settings.AZURE_OPENAI_API_VERSION or "preview"
    verify_ssl = settings.VERIFY_SSL

    print("=" * 70)
    print("LLM connectivity diagnostic — הרץ את זה ב-VDI")
    print("=" * 70)
    print(f"configured endpoint : {endpoint or '(ריק!)'}")
    print(f"deployment          : {deployment}")
    print(f"api_version         : {api_version}")
    print(f"USE_V1              : {getattr(settings, 'AZURE_OPENAI_USE_V1', False)}")
    print(f"VERIFY_SSL          : {verify_ssl}")
    print(f"key                 : {'set (' + str(len(key)) + ' chars)' if key else 'MISSING!'}")
    print()

    if not endpoint:
        print("❌ AZURE_OPENAI_ENDPOINT ריק — אין מה לבדוק. מלא אותו ב-.env.")
        return 2

    hosts = _candidate_hosts(endpoint)
    if not hosts:
        print(f"❌ לא הצלחתי לחלץ שם-רסורס מ-{endpoint}")
        return 2

    configured_host = urlsplit(endpoint).hostname
    reachable = []
    for host in hosts:
        marker = "  ← (זה שב-.env)" if host == configured_host else ""
        print(f"── {host}{marker}")
        ok, dns = _dns(host)
        if ok:
            print(f"   DNS   : ✓ {', '.join(dns)}")
        else:
            print(f"   DNS   : ✗ לא נפתר — {dns}")
        res = _probe_host(host, api_version, key, deployment, verify_ssl)
        v = res["verdict"]
        icon = {"OK": "✅", "auth": "🔑", "model": "🔎", "proxy-block": "⛔", "connectivity": "🚫"}.get(v, "❔")
        http = f" [HTTP {res['http']}]" if "http" in res else ""
        print(f"   PROBE : {icon} {v}{http} — {res['detail']}")
        if v in ("OK", "auth", "model"):   # host נגיש (גם אם key/deployment לא מושלמים)
            reachable.append((host, v))
        print()

    print("=" * 70)
    if any(v == "OK" for _, v in reachable):
        best = next(h for h, v in reachable if v == "OK")
        print(f"✅ המלצה: הגדר  AZURE_OPENAI_ENDPOINT=https://{best}/  ב-.env (host נגיש + תשובת-מודל).")
    elif reachable:
        host, v = reachable[0]
        print(f"⚠ ה-host {host} נגיש אבל יש בעיית {v} (key/deployment). תקן את זה — ה-proxy לא חוסם.")
        print(f"  נסה: AZURE_OPENAI_ENDPOINT=https://{host}/")
    else:
        print("⛔ אף וריאנט-דומיין לא נגיש מה-VDI — זו חסימת proxy. פנה ל-IT ל-whitelist של:")
        for h in hosts:
            print(f"     {h}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
