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
_JSON_ACCEPT = "application/vnd.kafka.json.v2+json"


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
        group_prefix: str,
    ) -> Optional[Dict[str, Any]]:
        """יוצר consumer instance, נרשם ל-topic, ועושה polling עד שמסר תואם מגיע או timeout.

        מחזיר:
        - dict {value_parsed, offset, partition, topic, key} אם נמצא מסר תואם
        - None אם timeout (אין מסר תואם)
        - {"fatal_error": "..."} אם 401/403 (auth/ACL) — מפעיל early-stop בפייפליין
        - {"rest_consumer_unavailable": True} אם consumer API כבוי (404/501)
        """
        group = f"{group_prefix}-{uuid.uuid4().hex[:8]}"
        instance_name = f"qa-{uuid.uuid4().hex[:8]}"
        headers_json = {"Content-Type": _V2_ACCEPT}

        async with httpx.AsyncClient(verify=self.verify_ssl, timeout=30.0) as client:
            # 1. create consumer instance
            create_url = f"{self.base}/consumers/{group}"
            create_body = {
                "name": instance_name,
                "format": "json",
                "auto.offset.reset": "latest",
                "auto.commit.enable": "false",
            }
            try:
                r = await client.post(create_url, headers=headers_json, json=create_body, auth=self.auth)
            except httpx.HTTPError as e:
                return {"fatal_error": f"REST consumer create transport error: {e}"}
            unavailable = _consumer_unavailable(r.status_code)
            if unavailable:
                return {"rest_consumer_unavailable": True, "detail": f"HTTP {r.status_code} on create"}
            if r.status_code in (401, 403):
                return {"fatal_error": f"HTTP {r.status_code} on consumer create: {r.text[:200]}"}
            if r.status_code >= 400:
                return {"fatal_error": f"HTTP {r.status_code} on consumer create: {r.text[:200]}"}

            # בונים את ה-instance base בעצמנו — לא סומכים על base_uri המוחזר (host פנימי שגוי מאחורי proxy)
            instance_base = f"{self.base}/consumers/{group}/instances/{instance_name}"

            try:
                # 2. subscribe
                sub_url = f"{instance_base}/subscription"
                rs = await client.post(sub_url, headers=headers_json, json={"topics": [topic]}, auth=self.auth)
                if rs.status_code in (401, 403):
                    return {"fatal_error": f"HTTP {rs.status_code} on subscribe: {rs.text[:200]}"}
                if rs.status_code >= 400:
                    return {"fatal_error": f"HTTP {rs.status_code} on subscribe: {rs.text[:200]}"}

                # 3. poll loop
                records_url = f"{instance_base}/records"
                poll_headers = {"Accept": _JSON_ACCEPT}
                deadline = time.monotonic() + timeout_seconds
                # קריאה ראשונה "מתחממת" — REST proxy לפעמים מחזיר ריק עד שה-assignment מתבצע
                while time.monotonic() < deadline:
                    try:
                        rr = await client.get(records_url, headers=poll_headers,
                                              params={"timeout": 1000}, auth=self.auth)
                    except httpx.HTTPError as e:
                        log.warning("kafka_rest_poll_transport_error", error=str(e))
                        continue
                    if rr.status_code in (401, 403):
                        return {"fatal_error": f"HTTP {rr.status_code} on records: {rr.text[:200]}"}
                    if rr.status_code >= 400:
                        # 404 כאן יכול להיות consumer שפג — נחזיר fatal עם פירוט
                        return {"fatal_error": f"HTTP {rr.status_code} on records: {rr.text[:200]}"}
                    try:
                        records = rr.json()
                    except Exception:
                        records = []
                    observed = _scan_records(records, topic, match)
                    if observed is not None:
                        log.info("kafka_rest_consumed", topic=topic, offset=observed.get("offset"))
                        return observed
                return None  # timeout — אין מסר תואם
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


def _scan_records(records: Any, topic: str, match: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """עובר על רשומות מ-GET /records ומחזיר את הראשונה שתואמת ל-match (או None)."""
    if not isinstance(records, list):
        return None
    for rec in records:
        if not isinstance(rec, dict):
            continue
        value = rec.get("value")
        if _record_matches(value, match):
            return {
                "value_parsed": value,
                "offset": rec.get("offset"),
                "partition": rec.get("partition"),
                "topic": rec.get("topic") or topic,
                "key": rec.get("key"),
            }
    return None


def _record_matches(value: Optional[Any], match: Dict[str, Any]) -> bool:
    """True אם value (dict או JSON string) מכיל את כל ה-key:value-pairs ב-match."""
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
        if k not in value or value[k] != expected:
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
