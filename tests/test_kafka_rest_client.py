"""טסטים ל-KafkaRestClient — פרסור תשובות + matching (ללא רשת)."""

from __future__ import annotations

import pytest

import base64
import json

from agents.runner.kafka_rest_client import (
    KafkaRestClient,
    _consumer_unavailable,
    _decode_binary_record,
    _parse_produce_response,
    _record_matches,
    _scan_records,
    _unique_present,
)
from agents.runner.dotnet_runner import _extract_sys_name, _normalize_topic, _tc_key
from config.settings import settings as _settings


def _b64(s):
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


# ============================================================
# _parse_produce_response
# ============================================================

def test_produce_success():
    body = {"offsets": [{"partition": 3, "offset": 42}]}
    out = _parse_produce_response(200, body)
    assert out == {"partition": 3, "offset": 42}


def test_produce_per_record_error():
    body = {"offsets": [{"partition": None, "offset": None,
                         "error_code": 40301, "error": "Not authorized"}]}
    out = _parse_produce_response(200, body)
    assert "error" in out
    assert "40301" in out["error"]
    assert "Not authorized" in out["error"]


def test_produce_http_403():
    out = _parse_produce_response(403, {"message": "forbidden"})
    assert "error" in out
    assert "HTTP 403" in out["error"]


def test_produce_no_offsets():
    out = _parse_produce_response(200, {"weird": "shape"})
    assert "error" in out
    assert "no offsets" in out["error"]


# ============================================================
# _record_matches / _scan_records
# ============================================================

def test_record_matches_dict():
    assert _record_matches({"a": 1, "b": 2}, {"a": 1})
    assert not _record_matches({"a": 1}, {"a": 2})
    assert not _record_matches(None, {"a": 1})
    assert _record_matches({"a": 1}, {})  # empty match always true


def test_record_matches_json_string():
    # REST proxy לפעמים מחזיר value כ-string
    assert _record_matches('{"status": "ok"}', {"status": "ok"})
    assert not _record_matches("not json", {"status": "ok"})


def test_record_matches_precise_correlation():
    """★ correlation מדויק: member_id + action + entity_type — דוחה מסר Worker של member אחר.
    זה התיקון ל-key_contains הרופף ('55' תפס '243551785')."""
    ours = {"entity_type": "child_development", "action": "create",
            "_data": {"parameters": [{"member_id": "555", "gender": "זכר"}]}}
    other = {"entity_type": "child_development", "action": "create",
             "_data": {"parameters": [{"member_id": "233848555", "gender": "M"}]}}
    m = {"entity_type": "child_development", "_data.parameters.0.member_id": "555", "root.action": "create"}
    assert _record_matches(ours, m)
    assert not _record_matches(other, m)        # 233848555 מכיל 555 אבל member_id מדויק ≠
    # action שגוי → לא תואם (create כשרצינו delete)
    assert not _record_matches(ours, {"_data.parameters.0.member_id": "555", "root.action": "delete"})


def test_record_matches_autolist_and_type_tolerant():
    """match סלחני: list ללא index (parameters.member_id) + השוואת str (555 == '555')."""
    val = {"_data": {"parameters": [{"member_id": "555"}]}}
    assert _record_matches(val, {"_data.parameters.member_id": "555"})   # בלי index → auto [0]
    assert _record_matches(val, {"_data.parameters.0.member_id": 555})   # int vs str → str()-tolerant


def test_record_matches_numeric_tolerant_leading_zeros():
    """★ ה-Worker מסיר אפסים מובילים מ-member_id (ת.ז) → '000123456' במקור = '123456' ביעד.
    הקורלציה int-tolerant תופסת בכל מקרה (גם אם ה-Worker מסיר וגם אם לא)."""
    m = {"entity_type": "child_development", "_data.parameters.0.member_id": "123456"}
    stripped = {"entity_type": "child_development", "_data": {"parameters": [{"member_id": "123456"}]}}
    unstripped = {"entity_type": "child_development", "_data": {"parameters": [{"member_id": "000123456"}]}}
    assert _record_matches(stripped, m)
    assert _record_matches(unstripped, m)    # 000123456 == 123456 (int-tolerant)
    # מספר אחר עדיין נדחה
    assert not _record_matches({"_data": {"parameters": [{"member_id": "999"}]}},
                               {"_data.parameters.0.member_id": "123456"})


def test_record_matches_leaf_fallback_fhir():
    """★ format-agnostic: כשהנתיב המדויק לא קיים (FHIR ללא _data.parameters) — fallback ל-leaf
    בכל מקום ב-tree. additive: רץ רק כשהנתיב המדויק מחזיר _MISSING.
    בטוח מ-false-match כי key_contains=uid כבר מצמצם ל-candidate בודד לפני בדיקת ה-value."""
    fhir = {"resourceType": "Bundle",
            "entry": [{"resource": {"resourceType": "DiagnosticReport",
                                    "examination_type_code": "1"}}]}
    # נתיב MACKAF קשיח לא קיים ב-FHIR, אבל ה-leaf 'examination_type_code' קיים עמוק ב-tree
    assert _record_matches(fhir, {"_data.parameters.0.examination_type_code": "1"})
    # ערך שגוי עדיין נדחה
    assert not _record_matches(fhir, {"_data.parameters.0.examination_type_code": "9"})
    # leaf שלא קיים בכלל → _MISSING → False
    assert not _record_matches(fhir, {"_data.parameters.0.nonexistent": "1"})


def test_record_matches_exact_path_wins_over_leaf():
    """★ ה-leaf-fallback הוא additive — נתיב מדויק שקיים גובר (לא מדלגים אליו). מסר Worker
    של member אחר עדיין נדחה (regression ל-precise_correlation)."""
    other = {"_data": {"parameters": [{"member_id": "233848555"}]}}
    # הנתיב קיים (233848555) → אין fallback; ערך ≠ 555 → לא תואם
    assert not _record_matches(other, {"_data.parameters.0.member_id": "555"})


def test_unique_present_key_or_body():
    """★ value_contains: ה-uid נתפס אם הוא ב-KEY *או* בגוף ה-value (format-agnostic)."""
    # uid בגוף (FHIR: ה-KEY הוא scc_message_id, ה-uid ב-_data.member_id)
    in_body = {"key": "SCC-TST.128128403.0004549368.HISTO.final.0",
               "value_parsed": {"_data": {"member_id": "999735863"}}}
    assert _unique_present(in_body, "999735863")
    # uid ב-KEY (MACKAF)
    in_key = {"key": "child_development::999735863::4242", "value_parsed": {"x": 1}}
    assert _unique_present(in_key, "999735863")
    # uid לא קיים בשום מקום
    assert not _unique_present(in_body, "111111111")
    # None → אין דרישה → True
    assert _unique_present(in_body, None)


def test_scan_records_value_contains_picks_our_message():
    """★ הבאג מהריצה: KEY משותף בין כל המסרים (scc_message_id), כך ש-key לא מבדיל. value_contains
    תופס את המסר *שלנו* לפי ה-uid בגוף, ולא מסר זר עם אותו entity_type+action."""
    same_key = "SCC-TST.128128403.0004549368.HISTO.final.0"
    foreign = {"value": {"entity_type": "test_lab_result_approval", "action": "create",
                         "_data": {"member_id": "111", "examination_type_code": 3}},
               "offset": 16641, "partition": 0, "topic": "t", "key": same_key}
    ours = {"value": {"entity_type": "test_lab_result_approval", "action": "create",
                      "_data": {"member_id": "999735863", "examination_type_code": 1}},
            "offset": 16686, "partition": 0, "topic": "t", "key": same_key}
    match = {"entity_type": "test_lab_result_approval", "root.action": "create"}
    # בלי value_contains → תופס את הראשון (הזר) — בדיוק הבאג
    assert _scan_records([foreign, ours], "t", match)["offset"] == 16641
    # עם value_contains=uid → מדלג על הזר ותופס את שלנו
    got = _scan_records([foreign, ours], "t", match, value_contains="999735863")
    assert got is not None and got["offset"] == 16686


def test_scan_records_finds_match():
    records = [
        {"value": {"id": 1}, "offset": 10, "partition": 0, "topic": "t"},
        {"value": {"id": 2, "status": "enriched"}, "offset": 11, "partition": 0, "topic": "t"},
    ]
    out = _scan_records(records, "t", {"status": "enriched"})
    assert out is not None
    assert out["offset"] == 11
    assert out["value_parsed"]["id"] == 2


def test_scan_records_no_match_returns_none():
    records = [{"value": {"id": 1}, "offset": 10, "topic": "t"}]
    assert _scan_records(records, "t", {"id": 999}) is None


def test_scan_records_empty():
    assert _scan_records([], "t", {"x": 1}) is None
    assert _scan_records("not a list", "t", {}) is None


# ============================================================
# _consumer_unavailable
# ============================================================

def test_consumer_unavailable():
    assert _consumer_unavailable(404) is True
    assert _consumer_unavailable(501) is True
    assert _consumer_unavailable(200) is False
    assert _consumer_unavailable(403) is False


# ============================================================
# _tc_key (key default for publish)
# ============================================================

def test_tc_key_extracts_tc_number():
    assert _tc_key("TC-01: הקמת אורח") == "TC-01"
    assert _tc_key("TC03 – סינון") == "TC03"
    assert _tc_key("tc_5 something") == "tc-5"


def test_tc_key_fallback():
    assert _tc_key("") == "unknown"
    out = _tc_key("random title with spaces")
    assert " " not in out
    assert len(out) <= 32


# ============================================================
# _normalize_topic — Kafka topics are case-sensitive; org uses lowercase
# ============================================================

def test_normalize_topic_lowercases():
    assert _normalize_topic("Clicks-referral-streaming") == "clicks-referral-streaming"
    assert _normalize_topic("Patient_parameters-raw") == "patient_parameters-raw"
    assert _normalize_topic("already-lower") == "already-lower"


def test_normalize_topic_strips():
    assert _normalize_topic("  Topic-X  ") == "topic-x"
    assert _normalize_topic("") == ""
    assert _normalize_topic(None) == ""


# ============================================================
# Binary decode — the 408 fix: keys like 'verifyhub::0::4242' aren't JSON
# ============================================================

def test_decode_binary_record_json_value():
    rec = {
        "key": _b64("verifyhub::0::4242"),
        "value": _b64('{"header":{"mac_sys_name":"encryption_child_development_worker"}}'),
        "offset": 617009, "partition": 1, "topic": "patient_parameters-raw",
    }
    out = _decode_binary_record(rec, "patient_parameters-raw")
    assert out["key"] == "verifyhub::0::4242"          # non-JSON key decoded fine
    assert out["value_parsed"]["header"]["mac_sys_name"] == "encryption_child_development_worker"
    assert out["offset"] == 617009


def test_decode_binary_record_non_json_value():
    rec = {"key": _b64("k"), "value": _b64("not json at all"), "offset": 1}
    out = _decode_binary_record(rec, "t")
    assert out["value_parsed"] == "not json at all"     # stays a string, no crash


# ============================================================
# Candidate collection — scan fills candidates even past the match
# ============================================================

def test_scan_collects_all_candidates():
    records = [
        {"key": _b64("verifyhub::0::1"), "value": _b64('{"id":1}'), "offset": 10},
        {"key": _b64("qa_ai_hero_TC-01"), "value": _b64('{"id":2,"status":"enriched"}'), "offset": 11},
        {"key": _b64("verifyhub::0::3"), "value": _b64('{"id":3}'), "offset": 12},
    ]
    candidates = []
    matched = _scan_records(records, "t", {"status": "enriched"}, candidates)
    assert matched is not None and matched["offset"] == 11
    assert len(candidates) == 3   # all three collected for logging, not just the match


# ============================================================
# Key matching — the correlation handle (child_development::<member_id>::<code>)
# ============================================================

_TARGET_RECORDS = [
    {"key": _b64("verifyhub::0::27394311"), "value": _b64('{"id":1}'), "offset": 1},
    {"key": _b64("child_development::038374476::0"), "value": _b64('{"action":"create"}'), "offset": 2},
    {"key": _b64("user_login_status::0001938670"), "value": _b64('{"id":3}'), "offset": 3},
]


def test_scan_key_contains_picks_our_message():
    candidates = []
    matched = _scan_records(_TARGET_RECORDS, "t", {}, candidates, key_contains="038374476")
    assert matched is not None and matched["offset"] == 2   # not the verifyhub one
    assert len(candidates) == 3


def test_scan_key_equals_exact():
    matched = _scan_records(_TARGET_RECORDS, "t", {}, [], key_equals="child_development::038374476::0")
    assert matched is not None and matched["offset"] == 2
    assert _scan_records(_TARGET_RECORDS, "t", {}, [], key_equals="child_development::999::0") is None


def test_scan_key_and_value_both_required():
    # key matches but value field doesn't → no match
    matched = _scan_records(_TARGET_RECORDS, "t", {"action": "delete"}, [], key_contains="038374476")
    assert matched is None
    # both satisfied
    matched = _scan_records(_TARGET_RECORDS, "t", {"action": "create"}, [], key_contains="038374476")
    assert matched is not None and matched["offset"] == 2


def test_scan_no_key_match_returns_none():
    assert _scan_records(_TARGET_RECORDS, "t", {}, [], key_contains="nonexistent") is None


# ============================================================
# Timestamp filter — don't accept an old message from a previous TC
# ============================================================

def test_scan_rejects_old_message_by_timestamp():
    recs = [
        {"key": _b64("child_development::0::555"), "value": _b64('{"action":"create"}'),
         "offset": 1, "timestamp": 1000},   # ישן (לפני ה-publish)
        {"key": _b64("child_development::0::555"), "value": _b64('{"action":"create"}'),
         "offset": 2, "timestamp": 5000},   # חדש (אחרי ה-publish)
    ]
    cands = []
    # publish ב-ts=3000 → רק offset 2 (ts=5000) תקף
    matched = _scan_records(recs, "t", {}, cands, key_contains="555", min_timestamp_ms=3000)
    assert matched is not None and matched["offset"] == 2
    # שניהם נאספים כ-candidates, הישן מסומן too_old
    assert len(cands) == 2
    assert cands[0].get("too_old") is True
    assert "too_old" not in cands[1]


def test_scan_old_only_returns_none():
    recs = [{"key": _b64("child_development::0::555"), "value": _b64('{}'),
             "offset": 1, "timestamp": 1000}]
    cands = []
    # כל המסרים ישנים מ-publish (ts=3000) → אין match (זה תרחיש שלילי תקין)
    assert _scan_records(recs, "t", {}, cands, key_contains="555", min_timestamp_ms=3000) is None
    assert cands[0].get("too_old") is True


def test_decode_includes_timestamp():
    rec = {"key": _b64("k"), "value": _b64("{}"), "offset": 1, "timestamp": 1718000000000}
    out = _decode_binary_record(rec, "t")
    assert out["timestamp"] == 1718000000000


# ============================================================
# Dotted-path matching — match on nested header.mac_correlation_id
# ============================================================

def test_record_matches_dotted_path():
    value = {"header": {"mac_correlation_id": "abc-123"}, "root": {"id": 5}}
    assert _record_matches(value, {"header.mac_correlation_id": "abc-123"})
    assert not _record_matches(value, {"header.mac_correlation_id": "other"})
    assert not _record_matches(value, {"header.missing_field": "x"})
    # flat keys still work alongside dotted
    assert _record_matches(value, {"header.mac_correlation_id": "abc-123"})


# ============================================================
# _extract_sys_name — surfaces which worker wrote a candidate
# ============================================================

def test_extract_sys_name():
    assert _extract_sys_name({"header": {"mac_sys_name": "encryption_child_development_worker"}}) \
        == "encryption_child_development_worker"
    assert _extract_sys_name({"headers": {"mac_sys_name": "verifyhub"}}) == "verifyhub"
    assert _extract_sys_name({"mac_sys_name": "top-level"}) == "top-level"
    assert _extract_sys_name({"no": "sysname"}) == "?"
    assert _extract_sys_name("not a dict") == "?"


# ============================================================
# Partition discovery — manual assign של *כל* ה-partitions ללא Describe ACL.
# זה הליבה של תיקון "no matching message": ה-group משותף עם ה-Worker, subscribe
# נתן כיסוי חלקי ופספס את ה-partition של המסר. ה-probe מגלה את כל ה-partitions.
# ============================================================

class _FakeResp:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self):
        return self._payload


class _FakeProxyClient:
    """httpx-like fake שמחקה את ה-REST Proxy האמיתי:
      - GET /topics/{t}      → describe (status/partitions ניתנים לשליטה)
      - POST /assignments    → תמיד 200; זוכר אילו partitions הוקצו
      - POST /positions/end  → תמיד 200 (★ ה-proxy האמיתי *משקר* — מצליח גם ל-partition פנטום)
      - GET /records         → 404 אם partition מוקצה לא קיים (broker UNKNOWN_PARTITION),
                               אחרת 200 [] — זה אות הקיום האמיתי שעליו ה-probe מסתמך."""

    def __init__(self, existing, describe_status=403, describe_partitions=None, create_status=200):
        self.existing = set(existing)
        self.describe_status = describe_status
        self.describe_partitions = describe_partitions
        self.create_status = create_status
        self.calls = []
        self._assigned = []

    async def get(self, url, **kw):
        self.calls.append(("GET", url))
        if "/topics/" in url and "/instances/" not in url:
            if self.describe_status < 400:
                parts = [{"partition": p} for p in (self.describe_partitions or [])]
                return _FakeResp(200, {"partitions": parts})
            return _FakeResp(self.describe_status, {}, "forbidden")
        if url.endswith("/records"):
            if any(p not in self.existing for p in self._assigned):
                return _FakeResp(404, {}, "unknown partition")
            return _FakeResp(200, [])
        return _FakeResp(200, {})

    async def post(self, url, json=None, **kw):
        self.calls.append(("POST", url, json))
        if "/consumers/" in url and "/instances/" not in url:   # create consumer
            return _FakeResp(self.create_status, {"instance_id": "i", "base_uri": url + "/instances/i"})
        if url.endswith("/assignments"):
            self._assigned = [p["partition"] for p in (json or {}).get("partitions", [])]
            return _FakeResp(200, {})
        if url.endswith("/positions/end"):
            return _FakeResp(200, {})   # ★ משקר כמו ה-proxy האמיתי — תמיד 200
        return _FakeResp(200, {})

    async def delete(self, url, **kw):
        self.calls.append(("DELETE", url))
        return _FakeResp(200, {})


_HJ = {"Content-Type": "x"}
_PH = {"Accept": "binary"}   # poll headers ל-records-fetch


def _rest_client():
    return KafkaRestClient(base_url="http://proxy", auth=("u", "p"), verify_ssl=False)


@pytest.mark.asyncio
async def test_candidate_partitions_configured_override(monkeypatch):
    monkeypatch.setattr(_settings, "KAFKA_TARGET_PARTITIONS", 5)
    fake = _FakeProxyClient(existing={0, 1, 2, 3, 4})
    nums, mode, _reason = await _rest_client()._candidate_partitions(fake, "t", _HJ)
    assert mode == "configured"
    assert nums == [0, 1, 2, 3, 4]
    assert not any(c[0] == "GET" for c in fake.calls)   # override → לא קוראים ל-GET /topics


@pytest.mark.asyncio
async def test_candidate_partitions_describe_ok(monkeypatch):
    monkeypatch.setattr(_settings, "KAFKA_TARGET_PARTITIONS", None)
    fake = _FakeProxyClient(existing={0, 1, 2}, describe_status=200, describe_partitions=[2, 0, 1])
    nums, mode, _reason = await _rest_client()._candidate_partitions(fake, "t", _HJ)
    assert mode == "describe"
    assert nums == [0, 1, 2]   # ממוין


@pytest.mark.asyncio
async def test_candidate_partitions_falls_back_to_probe(monkeypatch):
    monkeypatch.setattr(_settings, "KAFKA_TARGET_PARTITIONS", None)
    monkeypatch.setattr(_settings, "KAFKA_PARTITION_PROBE_MAX", 16)
    fake = _FakeProxyClient(existing={0, 1, 2, 3, 4, 5}, describe_status=403)
    nums, mode, reason = await _rest_client()._candidate_partitions(fake, "t", _HJ)
    assert mode == "probe"
    assert nums == list(range(16))
    assert "403" in reason


@pytest.mark.asyncio
async def test_open_partition_consumer_valid():
    """partition קיים → consumer מוכן (instance_base מוחזר, status None)."""
    fake = _FakeProxyClient(existing={0, 1, 2, 3, 4, 5})
    ib, status = await _rest_client()._open_partition_consumer(fake, "g", "t", 5, _HJ, _PH)
    assert status is None
    assert ib is not None and "/instances/" in ib


@pytest.mark.asyncio
async def test_open_partition_consumer_phantom_dropped():
    """★ partition פנטום (9, לא קיים) → records-fetch מחזיר 404 → consumer נמחק ומושמט.
    זה מה שמונע זיהום ה-fetch ב-partitions לא-קיימים."""
    fake = _FakeProxyClient(existing={0, 1, 2, 3, 4, 5})
    ib, status = await _rest_client()._open_partition_consumer(fake, "g", "t", 9, _HJ, _PH)
    assert ib is None and status is None
    assert any(c[0] == "DELETE" for c in fake.calls)   # נוקה


@pytest.mark.asyncio
async def test_open_partition_consumer_unavailable():
    """create מחזיר 404/501 → ה-consumer API כבוי → status 'unavailable'."""
    fake = _FakeProxyClient(existing={0, 1}, create_status=501)
    ib, status = await _rest_client()._open_partition_consumer(fake, "g", "t", 0, _HJ, _PH)
    assert ib is None and status == "unavailable"


@pytest.mark.asyncio
async def test_consume_reads_per_partition_finds_worker_message(monkeypatch):
    """★ הרגרסיה המרכזית: 6 partitions (configured), ה-Worker כתב *רק* ל-partition 5.
    consumer-per-partition קורא את p5 ומוצא את המסר (ה-multi-partition fetch של ה-proxy
    החזיר רק p1 ופספס אותו). רק ה-consumer של p5 מחזיר את מסר ה-Worker."""
    monkeypatch.setattr(_settings, "KAFKA_TARGET_PARTITIONS", 6)

    worker_rec = {"key": _b64("child_development::0::555"),
                  "value": _b64('{"header":{"mac_sys_name":"encryption_child_development_worker"},'
                                 '"entity_type":"child_development"}'),
                  "offset": 395507, "partition": 5, "timestamp": 5000}

    def _inst(url):
        return url.split("/instances/")[1].split("/")[0] if "/instances/" in url else None

    _HW5 = 395515   # offset של מסר ה-Worker ב-partition 5

    class _PerPartFake(_FakeProxyClient):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            self._by_instance = {}   # instance_name → assigned partitions
            self._pos = {}           # instance_name → current seek offset

        async def post(self, url, json=None, **kw):
            inst = _inst(url)
            if url.endswith("/assignments"):
                self._by_instance[inst] = [p["partition"] for p in (json or {}).get("partitions", [])]
            elif url.endswith("/positions/beginning"):
                self._pos[inst] = 0
            elif url.endswith("/positions/end"):
                self._pos[inst] = 10_000_000        # tip
            elif url.endswith("/positions"):
                offs = (json or {}).get("offsets", [])
                if offs:
                    self._pos[inst] = offs[0].get("offset", 0)
            return await super().post(url, json=json, **kw)

        async def get(self, url, **kw):
            self.calls.append(("GET", url))
            if url.endswith("/records"):
                inst = _inst(url)
                # רק p5 מכיל את מסר ה-Worker, וקריא רק כשממוקמים *בתוך* ה-data (offset <= HW)
                if self._by_instance.get(inst) == [5] and self._pos.get(inst, 10_000_000) <= _HW5:
                    return _FakeResp(200, [worker_rec])
                return _FakeResp(200, [])
            return _FakeResp(200, {})

    fake = _PerPartFake(existing={0, 1, 2, 3, 4, 5})

    class _CM:   # מחליף את httpx.AsyncClient(...) ב-consume
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return fake

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr("agents.runner.kafka_rest_client.httpx.AsyncClient", _CM)

    rich = await _rest_client().consume(
        "t", {"entity_type": "child_development"}, timeout_seconds=2, group="g",
        key_contains="555", on_ready=None, skew_ms=0)
    assert rich.get("matched") is not None
    assert rich["matched"]["partition"] == 5
    assert rich["assign"]["n_partitions"] == 6
