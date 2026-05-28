"""פרסור Postman Collection v2.1 JSON ל-PostmanCollection model."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from models.postman import (
    PostmanAuth,
    PostmanBody,
    PostmanCollection,
    PostmanEnvironment,
    PostmanHeader,
    PostmanRequest,
)


def load_collection_from_file(path: str) -> PostmanCollection:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return load_collection_from_dict(data)


def load_collection_from_dict(data: Dict[str, Any]) -> PostmanCollection:
    info = data.get("info", {}) or {}
    name = info.get("name") or "Unnamed Collection"
    items = data.get("item", []) or []
    requests = list(_walk_items(items))
    return PostmanCollection(name=name, requests=requests, info=info)


def load_environment_from_file(path: str) -> PostmanEnvironment:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return load_environment_from_dict(data)


def load_environment_from_dict(data: Dict[str, Any]) -> PostmanEnvironment:
    name = data.get("name", "default")
    values = {}
    for v in data.get("values", []) or []:
        if v.get("enabled", True) is False:
            continue
        key = v.get("key")
        val = v.get("value", "")
        if key:
            values[str(key)] = "" if val is None else str(val)
    return PostmanEnvironment(name=name, values=values)


def _walk_items(items: List[Dict[str, Any]], path_prefix: str = "") -> List[PostmanRequest]:
    out: List[PostmanRequest] = []
    for item in items:
        # folder
        if "item" in item and isinstance(item["item"], list):
            sub_prefix = (path_prefix + "/" + item.get("name", "")).strip("/")
            out.extend(_walk_items(item["item"], sub_prefix))
            continue
        req = item.get("request")
        if not req:
            continue
        out.append(_parse_request(item, path_prefix))
    return out


def _parse_request(item: Dict[str, Any], path_prefix: str) -> PostmanRequest:
    name_local = item.get("name") or "(unnamed)"
    name = f"{path_prefix}/{name_local}".strip("/") if path_prefix else name_local
    raw_req = item["request"]
    if isinstance(raw_req, str):
        # collection v1 fallback — string URL
        return PostmanRequest(name=name, method="GET", url_raw=raw_req)

    method = (raw_req.get("method") or "GET").upper()
    url_raw = _parse_url(raw_req.get("url"))
    headers = _parse_headers(raw_req.get("header") or [])
    body = _parse_body(raw_req.get("body"))
    auth = _parse_auth(raw_req.get("auth"))
    description = raw_req.get("description") if isinstance(raw_req.get("description"), str) else None
    return PostmanRequest(
        name=name,
        method=method,
        url_raw=url_raw,
        headers=headers,
        body=body,
        auth=auth,
        description=description,
    )


def _parse_url(url: Any) -> str:
    if url is None:
        return ""
    if isinstance(url, str):
        return url
    if isinstance(url, dict):
        if url.get("raw"):
            return url["raw"]
        protocol = url.get("protocol", "https")
        host = url.get("host")
        if isinstance(host, list):
            host = ".".join(host)
        path = url.get("path")
        if isinstance(path, list):
            path = "/".join(path)
        port = url.get("port")
        port_part = f":{port}" if port else ""
        path_part = f"/{path}" if path and not str(path).startswith("/") else (path or "")
        result = f"{protocol}://{host}{port_part}{path_part}"
        # query
        query = url.get("query") or []
        if query:
            qs = "&".join(
                f"{q.get('key')}={q.get('value', '')}"
                for q in query
                if q.get("disabled") is not True and q.get("key")
            )
            if qs:
                result += f"?{qs}"
        return result
    return str(url)


def _parse_headers(raw_headers: List[Dict[str, Any]]) -> List[PostmanHeader]:
    out: List[PostmanHeader] = []
    for h in raw_headers:
        if not isinstance(h, dict):
            continue
        out.append(
            PostmanHeader(
                key=str(h.get("key", "")),
                value=str(h.get("value", "")),
                disabled=bool(h.get("disabled", False)),
            )
        )
    return out


def _parse_body(raw_body: Optional[Dict[str, Any]]) -> Optional[PostmanBody]:
    if not raw_body or not isinstance(raw_body, dict):
        return None
    return PostmanBody(
        mode=raw_body.get("mode"),
        raw=raw_body.get("raw"),
        formdata=raw_body.get("formdata") or [],
        urlencoded=raw_body.get("urlencoded") or [],
        options=raw_body.get("options") or {},
    )


def _parse_auth(raw_auth: Optional[Dict[str, Any]]) -> Optional[PostmanAuth]:
    if not raw_auth or not isinstance(raw_auth, dict):
        return None
    auth_type = raw_auth.get("type")
    params: Dict[str, Any] = {}
    if auth_type and isinstance(raw_auth.get(auth_type), list):
        for p in raw_auth[auth_type]:
            if isinstance(p, dict) and p.get("key"):
                params[p["key"]] = p.get("value", "")
    return PostmanAuth(type=auth_type, params=params)
