"""טסטים לפיצ'ר מסרי-דוגמה מקור: פירוס קלט + חילוץ key_built_from."""

from __future__ import annotations

import os

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "")

from server.routes import _parse_messages_json  # noqa: E402
from agents.compiler.dotnet_compiler import _extract_kbf  # noqa: E402
from pipeline.dotnet_pipeline import (  # noqa: E402
    _build_transform_index,
    _extract_key_built_from,
    _extract_key_source_path,
)
from agents.runner.dotnet_runner import _parse_transform_rule  # noqa: E402
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


# ============================================================
# _extract_key_source_path — שדה-המקור שהופך ל-KEY (מ-transformations)
# ============================================================

def test_extract_key_source_path_from_transformations():
    """★ ה-KEY ביעד = scc_message_id המגיע מ-MessageHeader.id (verbatim). מחלצים אותו מה-
    transformations כדי להזריק שם uid ולקבל KEY ייחודי."""
    pt = {"transformations": {
        "MessageHeader.id": {"target_field_path": "_data.scc_message_id", "rule": "..."},
        "Patient.identifier.value": {"target_field_path": "_data.member_id", "rule": "strip first"},
    }}
    assert _extract_key_source_path(pt) == "MessageHeader.id"


def test_extract_key_source_path_entity_id_target():
    pt = {"transformations": {"SomeRes.uid": {"target_field_path": "entity_id"}}}
    assert _extract_key_source_path(pt) == "SomeRes.uid"


def test_extract_key_source_path_explicit_field_wins():
    """★ שדה מפורש key_source_field מה-PB גובר על ההיוריסטיקה (הכי אמין, דינמי לכל אפיון)."""
    pt = {"key_source_field": "MessageHeader.id",
          "transformations": {"Other.x": {"target_field_path": "scc_message_id"}}}
    assert _extract_key_source_path(pt) == "MessageHeader.id"
    # גם בתוך target_templates
    pt2 = {"target_templates": {"create": {"key_source_field": "Bundle.id"}}}
    assert _extract_key_source_path(pt2) == "Bundle.id"


def test_extract_key_source_path_none():
    assert _extract_key_source_path({}) is None
    assert _extract_key_source_path({"transformations": {"x": {"target_field_path": "_data.member_name"}}}) is None


# ============================================================
# _parse_transform_rule — מיפויי-קוד / verbatim / derived
# ============================================================

def test_parse_transform_rule_code_map():
    r = _parse_transform_rule("M_PAT_HPV/Z_PAT_HPV=1, M_PAT_NGC=2, M_PAT_HIST=3, M_CYT=7, EXT_LAB=5")
    assert r["kind"] == "code_map"
    assert r["map"]["M_PAT_HPV"] == "1" and r["map"]["Z_PAT_HPV"] == "1"   # פיצול LHS על '/'
    assert r["map"]["M_CYT"] == "7" and r["map"]["EXT_LAB"] == "5"


def test_parse_transform_rule_arrow_and_verbatim():
    assert _parse_transform_rule("Abnormal→1, else 0")["kind"] == "code_map"
    assert _parse_transform_rule("verbatim")["kind"] == "verbatim"
    assert _parse_transform_rule("copy as-is")["kind"] == "verbatim"


def test_parse_transform_rule_derived():
    # ביטויים מורכבים → derived (לא code_map שקרי)
    assert _parse_transform_rule("family + ' ' + given[0]")["kind"] == "derived"
    assert _parse_transform_rule("")["kind"] == "derived"
    assert _parse_transform_rule(None)["kind"] == "derived"
    # ★ מנוע מלא: "first char/split" מזוהה כעת כ-positional (ת"ז → member_id_code) — מדויק יותר מ-derived,
    #   והתנהגות forward/verify זהה (presence); ה-setup הוא set_first_char.
    assert _parse_transform_rule("strip first char (which becomes member_id_code)")["kind"] == "positional"


# ============================================================
# _build_transform_index — forward / reverse / leaf / collision / rules
# ============================================================

def test_build_transform_index_basic():
    pt = {"transformations": {
        "DiagnosticReport.category[0].coding[0].code": {"target_field_path": "_data.examination_type_code",
                                                        "rule": "M_PAT_HPV=1, M_CYT=7"},
        "Patient.name": {"target_field_path": "_data.member_name", "rule": "family + given[0]"},
        "MessageHeader.id": {"target_field_path": "_data.scc_message_id", "rule": "verbatim"},
    }}
    idx = _build_transform_index(pt)
    assert idx["by_target_path"]["_data.examination_type_code"] == "DiagnosticReport.category[0].coding[0].code"
    assert idx["by_target_leaf"]["examination_type_code"] == "DiagnosticReport.category[0].coding[0].code"
    assert idx["rules"]["_data.examination_type_code"]["kind"] == "code_map"
    assert idx["rules"]["_data.member_name"]["kind"] == "derived"
    assert idx["rules"]["_data.scc_message_id"]["kind"] == "verbatim"
    assert set(idx["target_paths"]) == {"_data.examination_type_code", "_data.member_name", "_data.scc_message_id"}


def test_build_transform_index_leaf_collision_is_none():
    """★ שני source שונים לאותו leaf → by_target_leaf[leaf]=None (לא לנחש)."""
    pt = {"transformations": {
        "A.x": {"target_field_path": "ref.practitioner_id", "rule": "v"},
        "B.y": {"target_field_path": "act.practitioner_id", "rule": "v"},
    }}
    idx = _build_transform_index(pt)
    assert idx["by_target_leaf"]["practitioner_id"] is None       # collision
    assert idx["by_target_path"]["ref.practitioner_id"] == "A.x"  # exact עדיין עובד


def test_build_transform_index_none_without_transformations():
    assert _build_transform_index({}) is None
    assert _build_transform_index({"transformations": {}}) is None
    assert _build_transform_index(None) is None
