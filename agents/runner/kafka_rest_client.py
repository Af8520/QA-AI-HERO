"""KafkaRestClient — produce/consume מול Confluent REST Proxy (port 8082) דרך httpx.

מסלול מועדף ל-.NET runner כשמוגדר KAFKA_REST_PROXY_URL. יתרונות מול הקליינט הנייטיב:
- עובד עם אותו user/password שעובד ב-Postman (ה-proxy מפרסם ב-principal פריבילגי משלו)
- httpx מכבד VERIFY_SSL=false → בעיית corporate SSL inspection נעלמת
- אין תלות ב-librdkafka (greenlet/confluent-kafka שנחסמו ב-JFrog)

Produce: POST /topics/{topic}  (application/vnd.kafka.json.v2+json)
Consume: Confluent REST Proxy v2 consumer API —
    POST /consumers/{group} → subscribe → GET records → DELETE
"""

from __future__ import annotations

import base64
import json
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx

from config.logging_config import get_logger
from config.settings import settings

log = get_logger(__name__)

_PRODUCE_CONTENT_TYPE = "application/vnd.kafka.json.v2+json"
_V2_ACCEPT = "application/vnd.kafka.v2+json"
# ★ consume ב-binary: ה-proxy לא מנסה לפרסר key/value כ-JSON (ה-keys ב-target הם strings
# רגילים כמו 'verifyhub::0::4242' שמפילים את json format). אנחנו מפענחים base64 בעצמנו.
_BINARY_ACCEPT = "application/vnd.kafka.binary.v2+json"

_CANDIDATE_CAP = 50  # כמה רשומות לאסוף ל-logging (לא רק את ההתאמה הראשונה)


class KafkaRestClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        auth: Optional[tuple] = None,
        verify_ssl: Optional[bool] = None,
    ) -> None:
        self.base = (base_url or settings.KAFKA_REST_PROXY_URL or "").rstrip("/")
        self.auth = auth if auth is not None else settings.kafka_rest_auth
        self.verify_ssl = settings.VERIFY_SSL if verify_ssl is None else verify_ssl

    # ============================================================
    # Produce
    # ============================================================

    async def produce(
        self,
        topic: str,
        key: Optional[str],
        value: Any,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """מפרסם רשומה אחת. מחזיר {topic, partition, offset} בהצלחה, או {error} בכשל.
        אותו shape של ה-native delivery_result כדי שה-runner לא יצטרך לדעת מאיזה מסלול הגיע.
        """
        url = f"{self.base}/topics/{topic}"
        record: Dict[str, Any] = {"value": value}
        if key is not None:
            record["key"] = key
        body = {"records": [record]}
        req_headers = {"Content-Type": _PRODUCE_CONTENT_TYPE, "Accept": _V2_ACCEPT}

        try:
            async with httpx.AsyncClient(verify=self.verify_ssl, timeout=30.0) as client:
                r = await client.post(url, headers=req_headers, json=body, auth=self.auth)
                status = r.status_code
                try:
                    data = r.json()
                except Exception:
                    data = {"_raw": r.text}
        except httpx.HTTPError as e:
            return {"error": f"REST proxy transport error: {e}"}

        result = _parse_produce_response(status, data)
        result.setdefault("topic", topic)
        log.info("kafka_rest_produced", topic=topic, status=status,
                 offset=result.get("offset"), error=result.get("error"))
        return result

    # ============================================================
    # Consume — Confluent REST Proxy v2 consumer API
    # ============================================================

    async def consume(
        self,
        topic: str,
        match: Dict[str, Any],
        timeout_seconds: int,
        group: str,
        key_equals: Optional[str] = None,
        key_contains: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """יוצר consumer instance, נרשם ל-topic, ועושה polling עד שמסר תואם מגיע או timeout.

        group — שם ה-consumer group המדויק (כבר resolved ע"י ה-caller; ACL בנוי עליו).

        מחזיר dict עשיר:
        - {"matched": <record|None>, "candidates": [...]} — matched הוא הרשומה התואמת (או None),
          candidates הן כל הרשומות שנראו בחלון (capped) ל-logging.
        - {"fatal_error": "..."} אם 401/403 (auth/ACL) — מפעיל early-stop בפייפליין
        - {"rest_consumer_unavailable": True} אם consumer API כבוי (404/501)
        """
        instance_name = f"qa-{uuid.uuid4().hex[:8]}"
        headers_json = {"Content-Type": _V2_ACCEPT}

        async with httpx.AsyncClient(verify=self.verify_ssl, timeout=30.0) as client:
            # 1. create consumer instance — format=binary (לא json! ה-keys ב-target אינם JSON)
            create_url = f"{self.base}/consumers/{group}"
            create_body = {
                "name": instance_name,
                "format": "binary",
                "auto.offset.reset": "latest",
                "auto.commit.enable": "false",
            }
            try:
                r = await client.post(create_url, headers=headers_json, json=create_body, auth=self.auth)
            except httpx.HTTPError as e:
                return {"fatal_error": f"REST consumer create transport error: {e}"}
            if _consumer_unavailable(r.status_code):
                return {"rest_consumer_unavailable": True, "detail": f"HTTP {r.status_code} on create"}
            if r.status_code >= 400:
                return {"fatal_error": f"HTTP {r.status_code} on consumer create: {r.text[:200]}"}

            # בונים את ה-instance base בעצמנו — לא סומכים על base_uri המוחזר (host פנימי שגוי מאחורי proxy)
            instance_base = f"{self.base}/consumers/{group}/instances/{instance_name}"

            candidates: List[Dict[str, Any]] = []
            try:
                # 2. subscribe
                sub_url = f"{instance_base}/subscription"
                rs = await client.post(sub_url, headers=headers_json, json={"topics": [topic]}, auth=self.auth)
                if rs.status_code >= 400:
                    return {"fatal_error": f"HTTP {rs.status_code} on subscribe: {rs.text[:200]}"}

                # 3. poll loop — אוסף candidates + מחפש התאמה
                records_url = f"{instance_base}/records"
                poll_headers = {"Accept": _BINARY_ACCEPT}
                deadline = time.monotonic() + timeout_seconds
                while time.monotonic() < deadline:
                    try:
                        rr = await client.get(records_url, headers=poll_headers,
                                              params={"timeout": 1000}, auth=self.auth)
                    except httpx.HTTPError as e:
                        log.warning("kafka_rest_poll_transport_error", error=str(e))
                        continue
                    if rr.status_code >= 400:
                        return {"fatal_error": f"HTTP {rr.status_code} on records: {rr.text[:200]}"}
                    try:
                        records = rr.json()
                    except Exception:
                        records = []
                    matched = _scan_records(records, topic, match, candidates,
                                            key_equals=key_equals, key_contains=key_contains)
                    if matched is not None:
                        log.info("kafka_rest_consumed", topic=topic, offset=matched.get("offset"),
                                 candidates_seen=len(candidates))
                        return {"matched": matched, "candidates": candidates}
                # timeout — אין התאמה, אבל מחזירים את ה-candidates ל-logging
                return {"matched": None, "candidates": candidates}
            finally:
                # 4. cleanup — DELETE instance
                try:
                    await client.delete(instance_base, headers={"Content-Type": _V2_ACCEPT}, auth=self.auth)
                except Exception as e:
                    log.warning("kafka_rest_consumer_delete_failed", error=str(e))


# ============================================================
# Pure helpers — testable ללא רשת
# ============================================================

def _parse_produce_response(status: int, data: Any) -> Dict[str, Any]:
    """ממיר תשובת REST produce ל-shape של delivery_result."""
    if status in (401, 403):
        return {"error": f"HTTP {status}: {_short(data)}"}
    if status >= 400:
        return {"error": f"HTTP {status}: {_short(data)}"}
    offsets = (data or {}).get("offsets") if isinstance(data, dict) else None
    if not offsets:
        return {"error": f"no offsets in response: {_short(data)}"}
    first = offsets[0] or {}
    # per-record error (REST proxy מחזיר error_code + error בתוך ה-offset)
    if first.get("error_code") or first.get("error"):
        return {"error": f"error_code={first.get('error_code')} {first.get('error')}"}
    return {"partition": first.get("partition"), "offset": first.get("offset")}


def _b64_to_str(v: Optional[str]) -> Optional[str]:
    """base64 → UTF-8 string. אם לא base64 תקין — מחזיר כפי שהוא."""
    if v is None:
        return None
    try:
        return base64.b64decode(v).decode("utf-8", errors="replace")
    except Exception:
        return v


def _decode_binary_record(rec: Dict[str, Any], topic: str) -> Dict[str, Any]:
    """מפענח רשומת binary מ-REST proxy: value(base64)→dict/str, key(base64)→str."""
    key_str = _b64_to_str(rec.get("key"))
    value_str = _b64_to_str(rec.get("value"))
    value_parsed: Any = value_str
    if isinstance(value_str, str):
        try:
            value_parsed = json.loads(value_str)
        except Exception:
            value_parsed = value_str  # לא JSON — נשאר string
    return {
        "value_parsed": value_parsed,
        "offset": rec.get("offset"),
        "partition": rec.get("partition"),
        "topic": rec.get("topic") or topic,
        "key": key_str,
    }


def _key_matches(key: Optional[str], key_equals: Optional[str], key_contains: Optional[str]) -> bool:
    """True אם ה-key המפוענח עומד ב-matchers (כל אחד שמולא חייב להתקיים)."""
    k = key or ""
    if key_equals is not None and k != key_equals:
        return False
    if key_contains is not None and key_contains not in k:
        return False
    return True


def _scan_records(
    records: Any,
    topic: str,
    match: Dict[str, Any],
    candidates: Optional[List[Dict[str, Any]]] = None,
    key_equals: Optional[str] = None,
    key_contains: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """מפענח רשומות, מוסיף ל-candidates (capped), ומחזיר את הראשונה שתואמת לכל ה-matchers
    (key_equals AND key_contains AND value match) — או None."""
    if candidates is None:
        candidates = []
    if not isinstance(records, list):
        return None
    matched = None
    for rec in records:
        if not isinstance(rec, dict):
            continue
        decoded = _decode_binary_record(rec, topic)
        if len(candidates) < _CANDIDATE_CAP:
            candidates.append(decoded)
        if (matched is None
                and _key_matches(decoded.get("key"), key_equals, key_contains)
                and _record_matches(decoded["value_parsed"], match)):
            matched = decoded
    return matched


_MISSING = object()


def _get_path_raw(d: Any, path: str) -> Any:
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        elif isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError:
                return _MISSING
            if -len(cur) <= idx < len(cur):
                cur = cur[idx]
            else:
                return _MISSING
        else:
            return _MISSING
    return cur


def _get_path(d: Any, path: str) -> Any:
    """dotted path עם list-index + סובלנות logical↔wire (root.X→X, headers.X→header.X)."""
    val = _get_path_raw(d, path)
    if val is _MISSING and path.startswith("root."):
        val = _get_path_raw(d, path[len("root."):])
    if val is _MISSING and path.startswith("headers."):
        val = _get_path_raw(d, "header." + path[len("headers."):])
    return val


def _record_matches(value: Optional[Any], match: Dict[str, Any]) -> bool:
    """True אם value (dict או JSON string) מכיל את כל ה-pairs ב-match.
    מפתח עם נקודה ('header.mac_correlation_id') נחשב dotted path מקונן.
    """
    if not match:
        return True
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return False
    if not isinstance(value, dict):
        return False
    for k, expected in match.items():
        actual = _get_path(value, k) if "." in k else value.get(k, _MISSING)
        if actual is _MISSING or actual != expected:
            return False
    return True


def _consumer_unavailable(status: int) -> bool:
    """404/501 על create = consumer API כבוי ב-deployment."""
    return status in (404, 501)


def _short(data: Any, limit: int = 200) -> str:
    try:
        s = json.dumps(data, ensure_ascii=False) if not isinstance(data, str) else data
    except Exception:
        s = str(data)
    return s[:limit]
