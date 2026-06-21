"""טסטים לפיצ'ר מסרי-דוגמה מקור: פירוס קלט + חילוץ key_built_from."""

from __future__ import annotations

import os

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "")

from server.routes import _parse_messages_json  # noqa: E402
from agents.compiler.dotnet_compiler import _extract_kbf  # noqa: E402
from pipeline.dotnet_pipeline import _extract_key_built_from  # noqa: E402
from agents.runner.dotnet_runner import _primary_id_field, _primary_id_path  # noqa: E402


# ============================================================
# _parse_messages_json — array / object / JSONL / fenced
# ============================================================

def test_parse_json_array():
    out = _parse_messages_json('[{"a":1},{"b":2}]')
    assert out == [{"a": 1}, {"b": 2}]


def test_parse_single_object():
    out = _parse_messages_json('{"resourceType":"Bundle","entry":[]}')
    assert out == [{"resourceType": "Bundle", "entry": []}]


def test_parse_jsonl():
    out = _parse_messages_json('{"a":1}\n{"b":2}\n# comment\n')
    assert out == [{"a": 1}, {"b": 2}]


def test_parse_fenced_json():
    out = _parse_messages_json('בלה בלה\n```json\n[{"x":9}]\n```\nעוד')
    assert out == [{"x": 9}]


def test_parse_empty_or_invalid():
    assert _parse_messages_json("") is None
    assert _parse_messages_json("not json at all") is None
    assert _parse_messages_json("[]") is None   # array ריק → None


# ============================================================
# _extract_kbf / _extract_key_built_from — מ-Payload Builder
# ============================================================

def test_extract_kbf_from_target_templates():
    pt = {"target_templates": {"create": {"key_built_from": ["root.entity_id", "root.entity_code"]}}}
    assert _extract_kbf(pt) == ["root.entity_id", "root.entity_code"]
    assert _extract_key_built_from(pt) == ["root.entity_id", "root.entity_code"]


def test_extract_kbf_top_level():
    pt = {"key_built_from": ["_data.member_details.member_id"]}
    assert _extract_kbf(pt) == ["_data.member_details.member_id"]


def test_extract_kbf_none():
    assert _extract_kbf({}) is None
    assert _extract_kbf({"target_templates": {"create": {}}}) is None
    assert _extract_key_built_from(None) is None


# ============================================================
# _primary_id_field — בוחר את השדה הראשי (לא code)
# ============================================================

def test_primary_id_field_skips_code():
    assert _primary_id_field(["_data.member_details.member_id",
                              "_data.member_details.member_id_code"]) == "member_id"
    assert _primary_id_field(["root.entity_id", "root.entity_code"]) == "entity_id"


def test_primary_id_field_fallback():
    assert _primary_id_field(None) is None
    assert _primary_id_field([]) is None
    # אם הכל code — מחזיר את הראשון
    assert _primary_id_field(["a.member_id_code"]) == "member_id_code"


# ============================================================
# _primary_id_path — נתיב מלא (Phase 3, מונע leaf גנרי)
# ============================================================

def test_primary_id_path_returns_full_path():
    """★ FHIR: מחזיר נתיב מלא (ServiceRequest.identifier.value) ולא leaf גנרי 'value'."""
    kbf = ["ServiceRequest.identifier.value", "DiagnosticReport.status"]
    assert _primary_id_path(kbf) == "ServiceRequest.identifier.value"
    assert _primary_id_field(kbf) == "value"   # ה-leaf הגנרי — בדיוק הסיכון שה-path מתקן


def test_primary_id_path_skips_code_and_fallback():
    assert _primary_id_path(["_data.x.member_id_code", "_data.x.member_id"]) == "_data.x.member_id"
    assert _primary_id_path(None) is None
    assert _primary_id_path(["a.code"]) == "a.code"   # הכל code → הראשון
