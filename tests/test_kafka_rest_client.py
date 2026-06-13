"""טסטים ל-KafkaRestClient — פרסור תשובות + matching (ללא רשת)."""

from __future__ import annotations

import pytest

import base64
import json

from agents.runner.kafka_rest_client import (
    _consumer_unavailable,
    _decode_binary_record,
    _parse_produce_response,
    _record_matches,
    _scan_records,
)
from agents.runner.dotnet_runner import _extract_sys_name, _normalize_topic, _tc_key


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
