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

import asyncio
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
# timeout (ms) ל-records-fetch של אימות-קיום partition ב-probe. צריך להיות גדול מספיק
# שה-broker יספיק להחזיר שגיאת UNKNOWN_PARTITION ל-partition לא-קיים, אך קצר (×N partitions).
_PROBE_FETCH_TIMEOUT_MS = 200


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
        on_ready=None,
        skew_ms: int = 0,
    ) -> Optional[Dict[str, Any]]:
        """★ consumer *נפרד לכל partition* (לא consumer יחיד עם multi-partition assign!).

        למה: ה-multi-partition fetch של ה-REST Proxy החזיר בפועל רק partition אחד (verifyhub),
        ופספס את ה-partition שעליו ה-Worker כותב (נצפה: assignment של 6 partitions אבל
        'partitions עם תעבורה: [1]'). consumer single-partition קורא בוודאות את ה-partition שלו,
        וה-verifyhub flood מבודד. כל consumer עושה seek-to-end לפני ה-publish; ה-poll סורק את כולם.

        on_ready — async callable שמורץ **אחרי** seek-to-end של כל ה-consumers (כאן ה-caller
          מפרסם), ומחזיר publish timestamp (ms). מסננים רשומות עם timestamp >= publish_ts - skew_ms.

        מחזיר dict עשיר: {matched, candidates, assign} / {fatal_error} / {rest_consumer_unavailable}.
        """
        headers_json = {"Content-Type": _V2_ACCEPT}
        poll_headers = {"Accept": _BINARY_ACCEPT}
        candidates: List[Dict[str, Any]] = []

        async with httpx.AsyncClient(verify=self.verify_ssl, timeout=30.0) as client:
            # 1. אילו partitions לקרוא (configured / describe / probe)
            part_nums, mode, reason = await self._candidate_partitions(client, topic, headers_json)
            base_assign = {"mode": mode, "n_partitions": 0, "parts": [], "reason": reason}
            if not part_nums:
                return {"fatal_error": f"no candidate partitions ({reason})", "assign": base_assign}

            # 2. consumer ייעודי לכל partition. _open_partition_consumer מאמת קיום דרך
            #    records-fetch ומשמיט partitions פנטום (probe / configured גדול-מדי).
            consumers: List[tuple] = []   # (instance_base, partition)
            unavailable = False
            for p in part_nums:
                ib, status = await self._open_partition_consumer(
                    client, group, topic, p, headers_json, poll_headers)
                if status == "unavailable":
                    unavailable = True
                    break
                if ib is not None:
                    consumers.append((ib, p))

            try:
                if unavailable:
                    return {"rest_consumer_unavailable": True, "detail": "consumer API לא זמין"}
                if not consumers:
                    return {"fatal_error": f"no readable partitions ({reason})", "assign": base_assign}

                assign_info = {"mode": mode, "n_partitions": len(consumers), "reason": reason,
                               "parts": [{"topic": topic, "partition": p} for _, p in consumers]}
                log.info("kafka_rest_per_partition_consumers", topic=topic, mode=mode, n=len(consumers))

                # 3. publish (on_ready) + timestamp filter
                min_timestamp_ms = 0
                if on_ready is not None:
                    pub_ts = await on_ready()
                    if isinstance(pub_ts, int) and pub_ts > 0:
                        min_timestamp_ms = pub_ts - skew_ms

                # 4. ★ קריאה אמינה דרך re-seek לאופסט ספציפי (לא tip-wait).
                # ה-proxy לא דוחף מסר *בודד* ל-partition שקט בהמתנה בסוף (נצפה: live=0 לכולם
                # חוץ מ-p1 הפעיל). אבל קריאה מאופסט-data-קיים *כן* אמינה. לכן: binary-search ל-HW
                # פעם אחת לכל partition, ואז כל סבב re-seek לאופסט + קריאה קדימה — המסר של ה-Worker
                # נקרא ברגע שהוא קיים בלוג. starts[p] מתקדם כדי לא לקרוא ישנים שוב.
                starts: Dict[int, Optional[int]] = {p: None for _, p in consumers}
                live_counts: Dict[int, int] = {p: 0 for _, p in consumers}

                async def _scan_partition(ib, p):
                    if starts[p] is None:
                        # התחל קרוב ל-HW (לא log-start) — מכבד retention ו-out-of-range
                        starts[p] = await self._find_recent_start(
                            client, ib, topic, p, headers_json, poll_headers)
                    try:
                        await client.post(f"{ib}/positions", headers=headers_json,
                                          json={"offsets": [{"topic": topic, "partition": p,
                                                             "offset": starts[p]}]}, auth=self.auth)
                    except httpx.HTTPError:
                        return None
                    for _ in range(8):   # קרא קדימה כמה batches של data קיים
                        try:
                            rr = await client.get(f"{ib}/records", headers=poll_headers,
                                                  params={"timeout": 1000}, auth=self.auth)
                        except httpx.HTTPError:
                            break
                        if rr.status_code >= 400:
                            break
                        try:
                            recs = rr.json()
                        except Exception:
                            recs = []
                        if not isinstance(recs, list) or not recs:
                            break
                        live_counts[p] = live_counts.get(p, 0) + len(recs)
                        maxoff = max((r.get("offset", -1) for r in recs if isinstance(r, dict)), default=-1)
                        if maxoff >= 0:
                            starts[p] = maxoff + 1   # התקדם — לא לקרוא שוב את אותם הישנים
                        m = _scan_records(recs, topic, match, candidates,
                                          key_equals=key_equals, key_contains=key_contains,
                                          min_timestamp_ms=min_timestamp_ms)
                        if m is not None:
                            return m
                    return None

                deadline = time.monotonic() + timeout_seconds
                matched = None
                while time.monotonic() < deadline and matched is None:
                    results = await asyncio.gather(*[_scan_partition(ib, p) for ib, p in consumers])
                    for m in results:
                        if m is not None:
                            matched = m
                            break
                    if matched is None and time.monotonic() < deadline:
                        await asyncio.sleep(6)   # תן ל-Worker זמן לכתוב לפני הסבב הבא

                if matched is not None:
                    log.info("kafka_rest_consumed", topic=topic, offset=matched.get("offset"),
                             partition=matched.get("partition"), candidates_seen=len(candidates))
                    return {"matched": matched, "candidates": candidates,
                            "assign": assign_info, "live_counts": live_counts}
                # ★ לא נמצא match — דיאגנוסטיקה מכריעה
                diag = await self._diagnose_partitions(
                    client, consumers, topic, key_contains, headers_json, poll_headers)
                return {"matched": None, "candidates": candidates, "assign": assign_info,
                        "live_counts": live_counts, "diag": diag}
            finally:
                for ib, _p in consumers:
                    await self._safe_delete(client, ib)

    async def _find_recent_start(self, client, ib: str, topic: str, p: int,
                                 headers_json: Dict[str, str], poll_headers: Dict[str, str],
                                 lookback: int = 3000) -> int:
        """מחזיר אופסט קריאה התחלתי קרוב ל-HW (כדי לא לקרוא את כל ה-retention).

        ★ ה-topic בעל retention → ה-log-start הוא לא 0. seek ל-offset 0 = out-of-range →
        reset ל-tip (ריק). לכן: (1) /positions/beginning → log-start אמיתי; (2) binary-search
        בתוך [log_start, ...] ל-HW (seek לאופסט עם data → records; מעבר ל-HW → ריק).
        מחזיר max(log_start, HW - lookback) — תמיד אופסט תקף (בטווח)."""
        # 1. log-start דרך /positions/beginning (אמין, להבדיל מ-offset 0)
        log_start = 0
        try:
            await client.post(f"{ib}/positions/beginning", headers=headers_json,
                              json={"partitions": [{"topic": topic, "partition": p}]}, auth=self.auth)
            rr = await client.get(f"{ib}/records", headers=poll_headers,
                                  params={"timeout": 1000}, auth=self.auth)
            recs = rr.json() if rr.status_code < 400 else []
            if isinstance(recs, list) and recs:
                log_start = min((r.get("offset", 0) for r in recs if isinstance(r, dict)), default=0)
        except (httpx.HTTPError, Exception):
            pass

        # 2. binary-search ל-HW בתוך [log_start, log_start + רחב] — mid תמיד >= log_start (בטווח)
        lo, hi = log_start, log_start + 20_000_000
        while hi - lo > 2000:
            mid = (lo + hi) // 2
            try:
                await client.post(f"{ib}/positions", headers=headers_json,
                                  json={"offsets": [{"topic": topic, "partition": p, "offset": mid}]},
                                  auth=self.auth)
                rr = await client.get(f"{ib}/records", headers=poll_headers,
                                      params={"timeout": 800}, auth=self.auth)
            except httpx.HTTPError:
                break
            recs = []
            if rr.status_code < 400:
                try:
                    recs = rr.json()
                except Exception:
                    recs = []
            if isinstance(recs, list) and recs:
                lo = mid          # יש data ב-mid → ה-HW גבוה יותר
            else:
                hi = mid          # ריק → mid >= HW
        return max(log_start, lo - lookback)

    async def _diagnose_partitions(self, client, consumers, topic: str, key_contains,
                                   headers_json: Dict[str, str], poll_headers: Dict[str, str]):
        """post-failure בלבד: לכל partition עושה seek-to-BEGINNING וקורא batch — מכריע אם
        בכלל אפשר לקרוא ממנו (status/count) ומה ה-mac_sys_name שם. אם partition כלשהו מחזיר
        count=0/שגיאה בעוד אחר מחזיר → בעיית fetch/leader צד-שרת ל-partition הזה."""
        diag: Dict[int, Dict[str, Any]] = {}
        for ib, p in consumers:
            info: Dict[str, Any] = {"status": None, "count": 0, "sys": {}, "has_key": False}
            try:
                await client.post(f"{ib}/positions/beginning", headers=headers_json,
                                  json={"partitions": [{"topic": topic, "partition": p}]}, auth=self.auth)
                rr = await client.get(f"{ib}/records", headers=poll_headers,
                                      params={"timeout": 1500}, auth=self.auth)
                info["status"] = rr.status_code
                if rr.status_code < 400:
                    try:
                        recs = rr.json()
                    except Exception:
                        recs = []
                    if isinstance(recs, list):
                        info["count"] = len(recs)
                        for rec in recs:
                            if not isinstance(rec, dict):
                                continue
                            dec = _decode_binary_record(rec, topic)
                            vp = dec.get("value_parsed")
                            sn = "?"
                            if isinstance(vp, dict):
                                hdr = vp.get("header") or vp.get("headers") or {}
                                sn = (hdr.get("mac_sys_name") if isinstance(hdr, dict) else None) \
                                    or vp.get("mac_sys_name") or "?"
                            info["sys"][sn] = info["sys"].get(sn, 0) + 1
                            if key_contains and key_contains in (dec.get("key") or ""):
                                info["has_key"] = True
            except httpx.HTTPError as e:
                info["status"] = f"err:{e}"
            diag[p] = info
        return diag

    # ============================================================
    # Partition discovery — manual assign של *כל* ה-partitions, ללא Describe ACL
    # ============================================================

    async def _candidate_partitions(self, client, topic: str, headers_json: Dict[str, str]):
        """מחזיר (partition_numbers, mode, reason) — קבוצת מועמדים לפני אימות קיום.

        סדר עדיפויות:
          1. KAFKA_TARGET_PARTITIONS (override ידוע) → 0..N-1.
          2. Describe (GET /topics) → רשימה אמיתית (עובד רק עם ACL — במכבי חסום).
          3. Probe (ברירת מחדל) → 0..PROBE_MAX-1; records-fetch per-partition יסנן לא-קיימים.
        """
        cfg = settings.KAFKA_TARGET_PARTITIONS
        if cfg and int(cfg) > 0:
            n = int(cfg)
            return list(range(n)), "configured", f"KAFKA_TARGET_PARTITIONS={n}"

        describe_reason = "describe skipped"
        try:
            tmeta = await client.get(f"{self.base}/topics/{topic}", headers=headers_json, auth=self.auth)
            if tmeta.status_code < 400:
                nums = [p.get("partition") for p in (tmeta.json() or {}).get("partitions") or []]
                nums = sorted({p for p in nums if p is not None})
                if nums:
                    return nums, "describe", "GET /topics ok"
                describe_reason = "GET /topics החזיר 0 partitions"
            else:
                describe_reason = f"GET /topics HTTP {tmeta.status_code} (אין Describe ACL?)"
        except httpx.HTTPError as e:
            describe_reason = f"GET /topics transport error: {e}"

        mx = max(1, int(settings.KAFKA_PARTITION_PROBE_MAX or 16))
        return list(range(mx)), "probe", f"{describe_reason}; probing 0..{mx - 1}"

    async def _open_partition_consumer(self, client, group: str, topic: str, p: int,
                                       headers_json: Dict[str, str], poll_headers: Dict[str, str]):
        """יוצר consumer ייעודי ל-partition *יחיד*, מקצה ומאמת קיום דרך records-fetch.
        מחזיר (instance_base, status):
          - (instance_base, None) → consumer מוכן לקריאה.
          - (None, 'unavailable')  → ה-consumer API כבוי (404/501 על create).
          - (None, None)           → partition לא-קיים (broker UNKNOWN_PARTITION) או כשל; נמחק."""
        instance_name = f"qa-{uuid.uuid4().hex[:8]}"
        create_body = {"name": instance_name, "format": "binary",
                       "auto.offset.reset": "latest", "auto.commit.enable": "false"}
        try:
            r = await client.post(f"{self.base}/consumers/{group}", headers=headers_json,
                                  json=create_body, auth=self.auth)
        except httpx.HTTPError:
            return None, None
        if _consumer_unavailable(r.status_code):
            return None, "unavailable"
        if r.status_code >= 400:
            return None, None

        instance_base = f"{self.base}/consumers/{group}/instances/{instance_name}"
        parts = [{"topic": topic, "partition": p}]
        try:
            asg = await client.post(f"{instance_base}/assignments",
                                    headers=headers_json, json={"partitions": parts}, auth=self.auth)
            if asg.status_code >= 400:
                await self._safe_delete(client, instance_base)
                return None, None
            # seek-to-end ואז records-fetch: partition פנטום → ה-broker מחזיר UNKNOWN_PARTITION (4xx/5xx).
            # seek-to-end לבדו משקר (200 גם לפנטום), לכן ה-fetch הוא אות הקיום האמיתי.
            await client.post(f"{instance_base}/positions/end",
                              headers=headers_json, json={"partitions": parts}, auth=self.auth)
            rr = await client.get(f"{instance_base}/records", headers=poll_headers,
                                  params={"timeout": _PROBE_FETCH_TIMEOUT_MS}, auth=self.auth)
            if rr.status_code >= 400:
                await self._safe_delete(client, instance_base)
                return None, None
        except httpx.HTTPError:
            await self._safe_delete(client, instance_base)
            return None, None
        return instance_base, None

    async def _safe_delete(self, client, instance_base: str) -> None:
        """מוחק consumer instance, בולע שגיאות (cleanup best-effort)."""
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
        "timestamp": rec.get("timestamp"),  # ms epoch — ל-temporal correlation
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
    min_timestamp_ms: int = 0,
) -> Optional[Dict[str, Any]]:
    """מפענח רשומות, מוסיף ל-candidates (capped), ומחזיר את הראשונה שתואמת לכל ה-matchers:
    timestamp >= min_timestamp_ms AND key_equals AND key_contains AND value match — או None.
    מסר עם timestamp ישן (מ-TC קודם) ייאסף כ-candidate אך *לא* ייחשב match (מסומן too_old)."""
    if candidates is None:
        candidates = []
    if not isinstance(records, list):
        return None
    matched = None
    for rec in records:
        if not isinstance(rec, dict):
            continue
        decoded = _decode_binary_record(rec, topic)
        ts = decoded.get("timestamp")
        too_old = bool(min_timestamp_ms) and isinstance(ts, int) and ts < min_timestamp_ms
        if too_old:
            decoded = {**decoded, "too_old": True}
        if len(candidates) < _CANDIDATE_CAP:
            candidates.append(decoded)
        if (matched is None
                and not too_old
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
