"""ביצוע request מתוך Postman Collection דרך httpx, עם templating של env vars."""

from __future__ import annotations

import re
import time
from typing import Any, Dict, Optional

import httpx

from config.logging_config import get_logger
from config.settings import settings
from models.postman import PostmanRequest

log = get_logger(__name__)

_VAR_PATTERN = re.compile(r"\{\{\s*([\w.\-]+)\s*\}\}")


def render(template: Optional[str], variables: Dict[str, Any]) -> str:
    if template is None:
        return ""

    def repl(m: re.Match) -> str:
        key = m.group(1)
        val = variables.get(key)
        return "" if val is None else str(val)

    return _VAR_PATTERN.sub(repl, template)


def _merge_vars(env_vars: Dict[str, str], overrides: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    merged.update(env_vars or {})
    merged.update(overrides or {})
    return merged


def _build_headers(req: PostmanRequest, variables: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for h in req.headers:
        if h.disabled:
            continue
        out[render(h.key, variables)] = render(h.value, variables)

    # Auth handling — לא כל הסוגים, רק bearer/basic/apikey נפוצים
    if req.auth and req.auth.type:
        atype = req.auth.type.lower()
        params = {k: render(str(v), variables) for k, v in req.auth.params.items()}
        if atype == "bearer":
            token = params.get("token", "")
            if token:
                out.setdefault("Authorization", f"Bearer {token}")
        elif atype == "basic":
            import base64

            user = params.get("username", "")
            pwd = params.get("password", "")
            creds = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            out.setdefault("Authorization", f"Basic {creds}")
        elif atype == "apikey":
            key = params.get("key") or "X-API-Key"
            value = params.get("value", "")
            location = (params.get("in") or "header").lower()
            if location == "header" and value:
                out.setdefault(key, value)
    return out


def _build_body(req: PostmanRequest, variables: Dict[str, Any]) -> Optional[Any]:
    if not req.body or not req.body.mode:
        return None
    mode = req.body.mode.lower()
    if mode == "raw":
        rendered = render(req.body.raw, variables)
        return rendered if rendered else None
    if mode == "urlencoded":
        return {
            render(p.get("key", ""), variables): render(str(p.get("value", "")), variables)
            for p in (req.body.urlencoded or [])
            if not p.get("disabled")
        }
    if mode == "formdata":
        return {
            render(p.get("key", ""), variables): render(str(p.get("value", "")), variables)
            for p in (req.body.formdata or [])
            if not p.get("disabled") and p.get("type") != "file"
        }
    return None


async def execute_request(
    req: PostmanRequest,
    env_vars: Optional[Dict[str, str]] = None,
    overrides: Optional[Dict[str, Any]] = None,
    timeout_seconds: Optional[int] = None,
    verify_ssl: Optional[bool] = None,
) -> Dict[str, Any]:
    """מבצע HTTP call ומחזיר {status, headers, body, duration_ms, url, method}."""
    variables = _merge_vars(env_vars or {}, overrides or {})
    url = render(req.url_raw, variables)
    method = req.method.upper()
    headers = _build_headers(req, variables)
    body = _build_body(req, variables)

    timeout = timeout_seconds or settings.HTTP_TIMEOUT_SECONDS
    verify = settings.VERIFY_SSL if verify_ssl is None else verify_ssl

    log.info("postman_request_start", method=method, url=url, request_name=req.name)
    started = time.perf_counter()

    async with httpx.AsyncClient(timeout=timeout, verify=verify) as client:
        kwargs: Dict[str, Any] = {"headers": headers}
        ct = (headers.get("Content-Type") or headers.get("content-type") or "").lower()
        if isinstance(body, str):
            if "json" in ct:
                kwargs["content"] = body.encode("utf-8")
            else:
                kwargs["content"] = body
        elif isinstance(body, dict):
            if req.body and req.body.mode == "urlencoded":
                kwargs["data"] = body
            elif req.body and req.body.mode == "formdata":
                kwargs["data"] = body
            else:
                kwargs["json"] = body
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.HTTPError as e:
            duration_ms = int((time.perf_counter() - started) * 1000)
            log.warning("postman_request_failed", url=url, error=str(e))
            return {
                "status": 0,
                "headers": {},
                "body": None,
                "body_text": None,
                "duration_ms": duration_ms,
                "url": url,
                "method": method,
                "error": str(e),
            }

    duration_ms = int((time.perf_counter() - started) * 1000)
    body_text = response.text
    parsed_body: Any
    try:
        parsed_body = response.json()
    except Exception:
        parsed_body = None

    log.info(
        "postman_request_done",
        method=method,
        url=url,
        status=response.status_code,
        duration_ms=duration_ms,
    )
    return {
        "status": response.status_code,
        "headers": dict(response.headers),
        "body": parsed_body,
        "body_text": body_text,
        "duration_ms": duration_ms,
        "url": url,
        "method": method,
    }
