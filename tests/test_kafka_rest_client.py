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
    """httpx-like fake ל-REST Proxy:
      - GET /topics/{t}        → describe (status/partitions ניתנים לשליטה)
      - POST /assignments      → תמיד 200 (ה-proxy מקבל assignment ללא ולידציה)
      - POST /positions/end    → 200 רק אם *כל* ה-partitions בבקשה קיימים, אחרת 404
    כך partition לא-קיים "נכשל" ב-seek ומסונן — בדיוק כמו ה-proxy האמיתי."""

    def __init__(self, existing, describe_status=403, describe_partitions=None):
        self.existing = set(existing)
        self.describe_status = describe_status
        self.describe_partitions = describe_partitions
        self.calls = []

    async def get(self, url, **kw):
        self.calls.append(("GET", url))
        if "/topics/" in url and "/instances/" not in url:
            if self.describe_status < 400:
                parts = [{"partition": p} for p in (self.describe_partitions or [])]
                return _FakeResp(200, {"partitions": parts})
            return _FakeResp(self.describe_status, {}, "forbidden")
        return _FakeResp(200, {})

    async def post(self, url, json=None, **kw):
        self.calls.append(("POST", url, json))
        if url.endswith("/positions/end"):
            reqs = [p["partition"] for p in (json or {}).get("partitions", [])]
            ok = bool(reqs) and all(p in self.existing for p in reqs)
            return _FakeResp(200 if ok else 404, {}, "" if ok else "unknown partition")
        if url.endswith("/assignments"):
            return _FakeResp(200, {})
        return _FakeResp(200, {})


_INST = "http://proxy/consumers/g/instances/i"
_HJ = {"Content-Type": "x"}


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
async def test_discover_probe_finds_real_count_without_describe(monkeypatch):
    """★ התרחיש האמיתי במכבי: אין Describe ACL, ה-topic בעל 6 partitions (0..5).
    ה-probe מגלה אותם לבד למרות PROBE_MAX=16 → כיסוי מלא (כולל partition 5)."""
    monkeypatch.setattr(_settings, "KAFKA_TARGET_PARTITIONS", None)
    monkeypatch.setattr(_settings, "KAFKA_PARTITION_PROBE_MAX", 16)
    fake = _FakeProxyClient(existing={0, 1, 2, 3, 4, 5}, describe_status=403)
    info = await _rest_client()._discover_partitions(fake, _INST, "t", _HJ)
    assert info["mode"] == "probe"
    assert info["n_partitions"] == 6
    assert [p["partition"] for p in info["parts"]] == [0, 1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_discover_configured_too_large_self_corrects(monkeypatch):
    """המשתמש אמר 8 אבל קיימים רק 0..4 → batch seek נכשל → per-partition מסנן ל-5.
    מבטיח שספירה ידנית שגויה לא שוברת כיסוי."""
    monkeypatch.setattr(_settings, "KAFKA_TARGET_PARTITIONS", 8)
    fake = _FakeProxyClient(existing={0, 1, 2, 3, 4})
    info = await _rest_client()._discover_partitions(fake, _INST, "t", _HJ)
    assert info["mode"] == "configured"
    assert info["n_partitions"] == 5
    assert [p["partition"] for p in info["parts"]] == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_discover_no_partitions_fails_cleanly(monkeypatch):
    """אף partition לא קיים → n_partitions=0 (ה-caller יחזיר fatal_error, *לא* subscribe חלקי)."""
    monkeypatch.setattr(_settings, "KAFKA_TARGET_PARTITIONS", None)
    monkeypatch.setattr(_settings, "KAFKA_PARTITION_PROBE_MAX", 4)
    fake = _FakeProxyClient(existing=set(), describe_status=403)
    info = await _rest_client()._discover_partitions(fake, _INST, "t", _HJ)
    assert info["n_partitions"] == 0
    assert info["parts"] == []
