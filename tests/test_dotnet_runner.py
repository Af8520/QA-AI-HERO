"""טסטים ל-DotNetRunner — בעיקר ל-pure helpers ולמצב BLOCKED.

קריאות אמיתיות ל-Kafka/Couchbase לא נבדקות כי אין broker זמין ב-CI.
מבוטל אוטומטית כש-KAFKA_BOOTSTRAP_SERVERS לא מוגדר.
"""

from __future__ import annotations

import copy
import os

import pytest

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = ""
os.environ["COUCHBASE_CONNECTION_STRING"] = ""

from agents.runner.dotnet_runner import (  # noqa: E402
    DotNetRunner,
    _check_expected_fields,
    _compute_expected,
    _strip_synthetic_suffix,
    _make_key_unique,
    _matches,
    _parse_transform_rule,
    _resolve_source_path,
    _sanitize_expected_fields,
    _override_by_path,
    _override_field_smart,
    _resolve_logical_holder,
    _substitute_token,
    _to_wire_message,
)
from models.dotnet_test_case import (  # noqa: E402
    CouchbaseWaitAction,
    DotNetExecutableTestCase,
    KafkaPublishAction,
    KafkaWaitAction,
)
from models.test_case import TestStatus  # noqa: E402


def test_semicolon_codemap_and_synthetic_suffix_compute():
    """★★★ שני באגים בחוזה ה-PB: (1) code_map עם מפריד ';' לא נפענח (→ presence-pass שקרי);
    (2) 'code__name' synthetic path → examination_type_name לא חושב. שניהם מתוקנים: override על הקוד
    האמיתי מחשב **גם** את הקוד (1) **וגם** את השם (PAP/HPV), מאותו ערך-מקור."""
    assert _strip_synthetic_suffix("DiagnosticReport.category[0].coding[0].code__name") == \
        "DiagnosticReport.category[0].coding[0].code"
    assert _strip_synthetic_suffix("MessageHeader.id__transaction") == "MessageHeader.id"
    assert _strip_synthetic_suffix("Patient.identifier.value") == "Patient.identifier.value"  # רגיל ללא שינוי

    # code_map עם ';' (כמו בחוזה האמיתי) נפענח; RHS רב-מילים ('מעבדות חוץ') מותר
    r = _parse_transform_rule("M_PAT_HPV/Z_PAT_HPV=PAP/HPV; M_CYT=ציטוגנטיקה; EXT_LAB=מעבדות חוץ")
    assert r["kind"] == "code_map"
    assert r["map"]["M_PAT_HPV"] == "PAP/HPV" and r["map"]["EXT_LAB"] == "מעבדות חוץ"

    code_path = "DiagnosticReport.category[0].coding[0].code"
    idx = {
        "by_target_path": {"_data.examination_type_code": code_path,
                           "_data.examination_type_name": code_path + "__name"},
        "by_target_leaf": {"examination_type_code": code_path, "examination_type_name": code_path + "__name"},
        "rules": {"_data.examination_type_code": {"kind": "code_map", "map": {"M_PAT_HPV": "1"}},
                  "_data.examination_type_name": {"kind": "code_map", "map": {"M_PAT_HPV": "PAP/HPV"}}},
        "target_paths": ["_data.examination_type_code", "_data.examination_type_name"],
    }
    applied = {code_path: "M_PAT_HPV"}                    # override על הקוד האמיתי בלבד
    assert _compute_expected(idx, "examination_type_code", applied) == "1"
    assert _compute_expected(idx, "examination_type_name", applied) == "PAP/HPV"  # מחושב מה-base


def test_substitute_token():
    obj = {"a": "__UNIQUE_ID__", "b": {"c": ["x", "__UNIQUE_ID__"]}, "n": 5}
    out = _substitute_token(obj, "__UNIQUE_ID__", "123456789")
    assert out["a"] == "123456789"
    assert out["b"]["c"] == ["x", "123456789"]
    assert out["n"] == 5   # non-strings untouched


def test_apply_unique_id_replaces_consistently():
    """★ אותו member_id ייחודי מוזרק ל-publish + correlation + expected_fields."""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-u",
        actions=[
            KafkaPublishAction(topic="src",
                               value={"_data": {"member_details": {"member_id": "__UNIQUE_ID__"}}}),
            KafkaWaitAction(topic="tgt", key_contains="__UNIQUE_ID__",
                            match={"entity_type": "child_development",
                                   "_data.parameters.0.member_id": "__UNIQUE_ID__"},
                            expected_fields={"_data.parameters.0.member_id": "__UNIQUE_ID__"}),
        ],
    )
    uid = DotNetRunner()._apply_unique_id(ex)   # מחזיר את ה-form הנקי (יעד)
    assert uid and uid.isdigit() and uid != "__UNIQUE_ID__"
    pub, wait = ex.actions
    # ה-member_id המקורי (token) בלי אפסים מובילים → מקור=נקי (לא מוסיפים אפסים שלא נדרשו)
    assert pub.value["_data"]["member_details"]["member_id"] == uid
    assert wait.key_contains == uid
    assert wait.match["_data.parameters.0.member_id"] == uid          # correlation (form נקי)
    assert wait.expected_fields["_data.parameters.0.member_id"] == uid
    assert wait.match["entity_type"] == "child_development"           # ללא token — נשאר


def test_apply_unique_id_overrides_concrete_member_id():
    """★ הבאג מ-TC02: ה-LLM שם member_id קונקרטי (555) במקור (לא token) → ה-runner דורס
    דטרמיניסטית בכל מקום (מקור מקונן + correlation + expected_fields), בלי תלות ב-token."""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-c",
        actions=[
            KafkaPublishAction(topic="src",
                               value={"_data": {"member_details": {"member_id": "555", "member_id_code": "0"}}}),
            KafkaWaitAction(topic="tgt", key_contains="555",
                            match={"entity_type": "child_development",
                                   "_data.parameters.0.member_id": "555", "root.action": "create"},
                            expected_fields={"_data.parameters.0.member_id": "555"}),
        ],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    assert uid and uid != "555"
    pub, wait = ex.actions
    # member_id מקורי "555" בלי אפסים → מקור=נקי (תלוי-בקשה, לא ממציאים בדיקת אפסים)
    assert pub.value["_data"]["member_details"]["member_id"] == uid
    assert pub.value["_data"]["member_details"]["member_id_code"] == "0"     # code לא נגע
    assert wait.value_contains == uid                                        # ★ קורלציה על ה-uid (key/גוף)
    assert wait.match["_data.parameters.0.member_id"] == uid
    assert wait.expected_fields["_data.parameters.0.member_id"] == uid
    assert wait.match["entity_type"] == "child_development"


def test_apply_unique_id_leading_zeros_conditional():
    """★ תלוי-בקשה: רק כשה-member_id *בתסריט* מתחיל באפסים (000555) — המקור נשלח עם אפסים
    מובילים (9 ספרות) והיעד נקי, לבדיקת הסרת אפסים. (אם רגיל — ראה הטסט הקודם: בלי אפסים.)"""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-z",
        actions=[
            KafkaPublishAction(topic="src", value={"_data": {"member_details": {"member_id": "000555"}}}),
            KafkaWaitAction(topic="tgt",
                            match={"entity_type": "child_development", "_data.parameters.0.member_id": "000555"},
                            expected_fields={"_data.parameters.0.member_id": "000555"}),
        ],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    pub, wait = ex.actions
    src = pub.value["_data"]["member_details"]["member_id"]
    assert len(src) == 9 and src[0] == "0"          # מקור: 9 ספרות עם אפס מוביל
    assert int(src) == int(uid)                     # אותו מספר; ה-form הנקי = uid
    assert wait.expected_fields["_data.parameters.0.member_id"] == uid   # יעד ללא אפסים → בודק הסרה


def test_apply_unique_id_negative_correlates_via_value_contains():
    """★ תרחיש שלילי: ה-wait (expect_no_message) בלי member_id ב-match → הקורלציה היא
    value_contains=uid (ה-uid מופיע ב-target ב-key או בגוף). השלילי מחפש את ה-id שלנו (שלא הופק)
    → timeout → PASS נכון. (לא מזריקים נתיב MACKAF קשיח — format-agnostic.)"""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-neg",
        actions=[
            KafkaPublishAction(topic="src", value={"_data": {"member_details": {"member_id": "555"}}}),
            KafkaWaitAction(topic="tgt", match={"entity_type": "child_development"}, expect_no_message=True),
        ],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    assert uid and uid != "555"
    pub, wait = ex.actions
    assert pub.value["_data"]["member_details"]["member_id"] == uid   # "555" בלי אפסים → מקור נקי
    assert wait.value_contains == uid                           # ★ קורלציה על ה-uid (key או גוף)
    assert "_data.parameters.0.member_id" not in wait.match      # לא מזריקים נתיב קשיח
    assert wait.match["entity_type"] == "child_development"      # הקיים נשמר


def test_apply_unique_id_format_agnostic_via_key_built_from():
    """★ format-agnostic: key_built_from מצביע על entity_id (לא member_id, פורמט FHIR ללא
    header/_data) → ה-runner דורס entity_id ושומר על מבנה הדוגמה."""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-fhir",
        key_built_from=["root.entity_id", "root.entity_id_code"],
        actions=[
            KafkaPublishAction(topic="src",
                               value={"resourceType": "Bundle", "entity_id": "777", "entity_id_code": "0"}),
            KafkaWaitAction(topic="tgt", match={"entity_type": "lab", "_data.0.entity_id": "777"},
                            expected_fields={"_data.0.entity_id": "777"}),
        ],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    assert uid and uid != "777"
    pub, wait = ex.actions
    assert pub.value["entity_id"] == uid             # ★ entity_id נדרס (לא member_id)
    assert pub.value["entity_id_code"] == "0"        # ה-code לא נגע
    assert pub.value["resourceType"] == "Bundle"     # מבנה FHIR נשמר (בלי header/_data)
    assert wait.match["_data.0.entity_id"] == uid    # יעד: entity_id נדרס
    assert wait.value_contains == uid


def test_apply_unique_id_noop_without_member_id():
    """אין שדה member_id ואין publish → no-op (degradation graceful)."""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-n",
        actions=[KafkaWaitAction(topic="t", key_contains="abc", match={"entity_type": "x"})],
    )
    assert DotNetRunner()._apply_unique_id(ex) is None
    assert ex.actions[0].key_contains == "abc"


def test_matches_helper():
    assert _matches({"a": 1, "b": 2}, {"a": 1})
    assert not _matches({"a": 1}, {"a": 2})
    assert not _matches(None, {"a": 1})
    assert _matches({"a": 1}, {})  # empty match → תמיד תואם


def test_check_expected_fields():
    assert _check_expected_fields({"a": 1, "b": 2}, {"a": "1"}) == []
    assert "c (missing)" in _check_expected_fields({"a": 1}, {"c": "x"})
    assert any("≠" in x for x in _check_expected_fields({"a": "x"}, {"a": "y"}))


def test_to_wire_message_flattens_root_and_renames_header():
    logical = {
        "headers": {"mac_sys_name": "CLICKS"},
        "root": {"message_id": "x", "action": "create", "entity_type": "referral"},
        "_data": {"member_details": {"member_id": "555"}},
    }
    wire = _to_wire_message(logical)
    assert "headers" not in wire and "root" not in wire
    assert wire["header"]["mac_sys_name"] == "CLICKS"     # header (singular)
    assert wire["action"] == "create"                     # root fields flattened to top level
    assert wire["entity_type"] == "referral"
    assert wire["message_id"] == "x"
    assert wire["_data"]["member_details"]["member_id"] == "555"


def test_to_wire_message_idempotent_on_wire_input():
    wire_in = {"header": {"a": 1}, "action": "create", "_data": {}}
    out = _to_wire_message(wire_in)
    assert out is wire_in   # no 'root'/'headers' → returns the same object (no-op)


def test_check_expected_fields_tolerates_logical_vs_wire_paths():
    # message is in WIRE format (action top-level, header singular)
    wire = {"header": {"mac_sys_name": "worker"}, "action": "create", "_data": {}}
    # brain emitted LOGICAL paths (root.action, headers.mac_sys_name) → must still resolve
    assert _check_expected_fields(wire, {"root.action": "create"}) == []
    assert _check_expected_fields(wire, {"headers.mac_sys_name": "worker"}) == []
    # plain wire paths also work
    assert _check_expected_fields(wire, {"action": "create", "header.mac_sys_name": "worker"}) == []


def test_check_expected_fields_dotted_and_list():
    """אסרשנים על שדות target מקוננים/מומרים — header.x, _data.parameters.0.gender."""
    value = {
        "header": {"mac_sys_name": "worker"},
        "root": {"action": "create", "entity_type": "child_development"},
        "_data": {"parameters": [{"gender": "זכר", "member_id": "038374476"}]},
    }
    # הכל תואם → אין issues
    assert _check_expected_fields(value, {
        "header.mac_sys_name": "worker",
        "root.action": "create",
        "_data.parameters.0.gender": "זכר",
    }) == []
    # ערך מומר שגוי → issue
    issues = _check_expected_fields(value, {"_data.parameters.0.gender": "M"})
    assert any("gender" in i for i in issues)
    # path חסר (לא-metadata) → missing
    assert any("missing" in i for i in _check_expected_fields(value, {"root.no_such_field": "x"}))


def test_check_expected_fields_parent_leaf_fallback():
    """★ fallback סלחני-אך-בטוח: סיומת parent.leaf בעומק שונה נפתרת (סלחנות לטעות-נתיב), אבל
    **בלי חציית-אחים** — נתיב תחת parent שלא קיים לא נפתר לאח עם אותו leaf (מונע false-pass)."""
    # depth tolerance: ה-LLM כתב _data.member_details.0.gender אבל זה תחת _data.x — parent.leaf מוצא
    val = {"_data": {"x": {"member_details": [{"gender": "זכר"}]}}}
    assert _check_expected_fields(val, {"_data.member_details.0.gender": "זכר"}) == []
    # שם-שדה שלא קיים בשום מקום → missing
    assert any("missing" in i for i in _check_expected_fields(val, {"_data.x.no_field": "y"}))


def test_check_expected_fields_no_cross_sibling_resolution():
    """★★★ הבאג מ-TC20: referral_practitioner.practitioner_id לא יפתר ל-act_practitioner.practitioner_id
    (false-pass). אם ה-parent (referral_practitioner) חסר → השדה חסר, גם אם ה-leaf קיים אצל אח."""
    val = {"_data": {"act_practitioner": {"practitioner_id": "051756047"}}}
    # __PRESENT__ על referral שאינו קיים → נכשל (לא חוצה ל-act)
    assert any("referral_practitioner" in i for i in
               _check_expected_fields(val, {"_data.referral_practitioner.practitioner_id": "__PRESENT__"}))
    # אבל act_practitioner.practitioner_id (קיים) → עובר
    assert _check_expected_fields(val, {"_data.act_practitioner.practitioner_id": "051756047"}) == []


def test_check_expected_fields_absent_marker():
    """★ __ABSENT__: תרחיש 'האובייקט לא אמור להופיע ביעד'. עובר אם חסר/ריק/null, נכשל אם קיים עם ערך."""
    # referral_practitioner חסר → __ABSENT__ עובר
    assert _check_expected_fields({"_data": {"act_practitioner": {"id": "1"}}},
                                  {"_data.referral_practitioner": "__ABSENT__"}) == []
    # קיים null/ריק → עובר
    assert _check_expected_fields({"_data": {"referral_practitioner": None}},
                                  {"_data.referral_practitioner": "__ABSENT__"}) == []
    # קיים עם ערך → נכשל
    issues = _check_expected_fields({"_data": {"referral_practitioner": {"id": "x"}}},
                                    {"_data.referral_practitioner": "__ABSENT__"})
    assert any("referral_practitioner" in i for i in issues)


def test_check_expected_fields_present_marker():
    """★ ערך דינמי/מוצפן → __PRESENT__ בודק נוכחות (קיים ולא-ריק), לא שוויון."""
    value = {"_data": {"parameters": [{"pdf_link": "FVbtGX...encrypted...", "member_id": "555"}]}}
    assert _check_expected_fields(value, {"_data.parameters.0.pdf_link": "__PRESENT__"}) == []
    # ריק → missing/empty
    assert any("missing/empty" in i for i in
               _check_expected_fields({"_data": {"parameters": [{"pdf_link": ""}]}},
                                      {"_data.parameters.0.pdf_link": "__PRESENT__"}))
    # חסר לגמרי → missing/empty
    assert any("missing/empty" in i for i in
               _check_expected_fields({"_data": {"parameters": [{}]}},
                                      {"_data.parameters.0.pdf_link": "__PRESENT__"}))


def test_check_expected_fields_auto_list_index():
    """★ ה-LLM נוטה להשמיט index ל-list: '_data.parameters.member_id' צריך לפתור ל-[0]."""
    value = {"_data": {"parameters": [{"member_id": "038374476", "gender": "זכר"}]}}
    # בלי index — אמור לפתור אוטומטית ל-parameters[0]
    assert _check_expected_fields(value, {"_data.parameters.member_id": "038374476"}) == []
    assert _check_expected_fields(value, {"_data.parameters.gender": "זכר"}) == []
    # ערך שגוי עדיין נתפס
    assert any("≠" in i for i in _check_expected_fields(value, {"_data.parameters.member_id": "999"}))
    # list ריק → missing (לא קורס)
    assert any("missing" in i for i in
               _check_expected_fields({"_data": {"parameters": []}}, {"_data.parameters.member_id": "1"}))


def test_check_expected_fields_skips_producer_metadata():
    """★ header.mac_* = metadata של ה-producer (לא טרנספורמציה) → מדולג, גם אם הערך 'שגוי'.
    ה-LLM לא יודע את ערך ה-mac_sys_name האמיתי (encryption_child_development_worker)."""
    value = {"header": {"mac_sys_name": "encryption_child_development_worker"},
             "action": "create",
             "_data": {"parameters": [{"member_id": "555"}]}}
    # mac_sys_name='Worker' שגוי — אבל מדולג (metadata) → אין כשל
    assert _check_expected_fields(value, {"header.mac_sys_name": "Worker"}) == []
    assert _check_expected_fields(value, {"headers.mac_producer_name": "Worker"}) == []
    # שדה דאטה אמיתי עדיין נאכף לצד metadata מדולג
    issues = _check_expected_fields(value, {
        "header.mac_sys_name": "Worker",          # מדולג
        "_data.parameters.0.member_id": "999",    # שגוי → נתפס
    })
    assert any("member_id" in i for i in issues)
    assert not any("mac_sys_name" in i for i in issues)


@pytest.mark.asyncio
async def test_runner_blocked_when_no_actions():
    runner = DotNetRunner()
    ex = DotNetExecutableTestCase(
        test_case_id="TC-empty",
        actions=[],
        compiler_notes="no actions",
    )
    r = await runner.execute(ex)
    assert r.status == TestStatus.BLOCKED


@pytest.mark.asyncio
async def test_runner_blocked_when_kafka_not_configured():
    """ללא KAFKA_BOOTSTRAP_SERVERS → publish/wait מסומנים BLOCKED."""
    runner = DotNetRunner()
    ex = DotNetExecutableTestCase(
        test_case_id="TC-no-kafka",
        actions=[KafkaPublishAction(topic="t", value={"a": 1})],
    )
    r = await runner.execute(ex)
    assert r.status == TestStatus.BLOCKED
    assert "Kafka not configured" in (r.step_results[0].error_message or "")


@pytest.mark.asyncio
async def test_runner_blocked_when_couchbase_not_configured():
    runner = DotNetRunner()
    ex = DotNetExecutableTestCase(
        test_case_id="TC-no-cb",
        actions=[CouchbaseWaitAction(bucket="b", key="k")],
    )
    r = await runner.execute(ex)
    assert r.status == TestStatus.BLOCKED
    assert "Couchbase not configured" in (r.step_results[0].error_message or "")


# ============================================================
# Phase 2 — _override_by_path + _apply_source_sample (FHIR sample base)
# ============================================================

def test_override_by_path_bracket_index_notation():
    """★ נתיב בסגנון JSONPath עם ברקטים [0] (כפי שה-LLM/PB מחזירים) — נפתר נכון."""
    obj = {"category": [{"coding": [{"code": "OLD"}]}]}
    assert _override_by_path(obj, "category[0].coding[0].code", "M_PAT_HPV")
    assert obj["category"][0]["coding"][0]["code"] == "M_PAT_HPV"


def test_override_by_path_jsonpath_filter():
    """★ פילטר JSONPath [?(@.system=='PID')] — בוחר את האלמנט הנכון ב-list, לא [0]."""
    obj = {"identifier": [{"system": "X", "value": "a"}, {"system": "PID", "value": "050"}]}
    assert _override_by_path(obj, "identifier[?(@.system=='PID')].value", "49069711")
    assert obj["identifier"][1]["value"] == "49069711"     # PID נדרס
    assert obj["identifier"][0]["value"] == "a"            # X לא נגע
    # פילטר שלא מתאים → False
    assert not _override_by_path(obj, "identifier[?(@.system=='NOPE')].value", "x")


def test_override_field_smart_resourcetype_prefixed_bracket_path():
    """★ הבאג מהריצה: 'DiagnosticReport.category[0].coding[0].code' (ResourceType + ברקטים) —
    הדריסה (M_PAT_HPV) מוחלת על ה-resource הנכון ב-Bundle."""
    bundle = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "DiagnosticReport",
                      "category": [{"coding": [{"code": "M_PAT_HIST"}]}]}}]}
    assert _override_field_smart(bundle, "DiagnosticReport.category[0].coding[0].code", "M_PAT_HPV")
    assert bundle["entry"][0]["resource"]["category"][0]["coding"][0]["code"] == "M_PAT_HPV"


def test_override_by_path_filter_targets_right_identifier():
    """★ הבאג מ-TC09: ת"ז צה"ל צריכה ללכת ל-identifier כאשר system=PID, לא ל-MRN. הפילטר בוחר נכון."""
    p = {"identifier": [{"system": "MRN", "value": "0004549368"},
                        {"system": "PID", "value": "050526227"}]}
    assert _override_by_path(p, "identifier[?(@.system=='PID')].value", "5999735863")
    assert p["identifier"][0]["value"] == "0004549368"   # MRN לא נגע
    assert p["identifier"][1]["value"] == "5999735863"   # PID נדרס


def test_override_by_path_nested_filter_key_auto_index():
    """★ פילטר עם מפתח מקונן (type.coding.code) — נפתר עם auto-index (בלי סוגריים פנימיים)."""
    pr = {"identifier": [{"type": {"coding": [{"code": "NID"}]}, "value": "1"},
                         {"type": {"coding": [{"code": "LN"}]}, "value": "2"}]}
    assert _override_by_path(pr, "identifier[?(@.type.coding.code=='LN')].value", "X")
    assert pr["identifier"][1]["value"] == "X"           # LN נדרס
    assert pr["identifier"][0]["value"] == "1"           # NID לא נגע


def test_override_field_smart_resourcetype_aware_picks_right_resource():
    """★★★ הבאג מהריצה: Observation עם category מופיע *לפני* DiagnosticReport. ה-ResourceType-aware
    מוודא שהדריסה פוגעת ב-DiagnosticReport.category (מה שה-Worker קורא), לא ב-category הראשון האקראי."""
    bundle = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "Observation",                      # decoy: category ראשון
                      "category": [{"coding": [{"code": "OBS"}]}]}},
        {"resource": {"resourceType": "DiagnosticReport",
                      "category": [{"coding": [{"system": "ICD", "code": "M_PAT_HIST"}]}],
                      "code": {"coding": [{"code": "other"}]}}},
    ]}
    assert _override_field_smart(bundle, "DiagnosticReport.category[0].coding[0].code", "M_PAT_HPV")
    assert bundle["entry"][1]["resource"]["category"][0]["coding"][0]["code"] == "M_PAT_HPV"  # ה-DiagnosticReport
    assert bundle["entry"][0]["resource"]["category"][0]["coding"][0]["code"] == "OBS"        # ה-decoy לא נגע
    assert bundle["entry"][1]["resource"]["code"]["coding"][0]["code"] == "other"             # שדה אחר לא נגע


def test_override_field_smart_finds_resource_in_wrapped_bundle():
    """★★★ הבאג האמיתי מהריצה (M_PAT_HIST נשאר): מסר Kafka/REST-Proxy עוטף את ה-Bundle
    ({'value': <bundle>}). חיפוש resource רק ברמה העליונה החמיץ את ה-DiagnosticReport → ה-suffix-fallback
    כתב ל-Observation.category (decoy) וה-DiagnosticReport נשאר M_PAT_HIST → ה-Worker קרא 3 במקום הקוד החדש.
    הפתרון: _fhir_resources_of_type רקורסיבי → מוצא את ה-resource גם כשעטוף ומחיל שם."""
    inner = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "Observation", "category": [{"coding": [{"code": "OBSCAT"}]}]}},
        {"resource": {"resourceType": "DiagnosticReport",
                      "category": [{"coding": [{"system": "ICD", "code": "M_PAT_HIST"}]}]}},
        {"resource": {"resourceType": "Patient", "identifier": [{"system": "PID", "value": "0999735863"}]}},
    ]}
    for wrap, get in [({"value": copy.deepcopy(inner)}, lambda m: m["value"]),
                      ({"records": [{"value": copy.deepcopy(inner)}]}, lambda m: m["records"][0]["value"])]:
        assert _override_field_smart(wrap, "DiagnosticReport.category[0].coding[0].code", "M_PAT_NGC")
        assert _override_field_smart(wrap, "Patient.identifier.value[system=PID]", "299999999")
        b = get(wrap)
        res = {e["resource"]["resourceType"]: e["resource"] for e in b["entry"]}
        assert res["DiagnosticReport"]["category"][0]["coding"][0]["code"] == "M_PAT_NGC"  # ה-resource הנכון
        assert res["Observation"]["category"][0]["coding"][0]["code"] == "OBSCAT"          # decoy לא נגע
        assert res["Patient"]["identifier"][0]["value"] == "299999999"                     # PID נדרס (צה"ל)


def test_override_by_path_index_tolerates_object_vs_array():
    """★ הבאג מהריצה: ה-PB מייצר 'category[0].coding[0].code' (מניח מערך), אבל ב-sample האמיתי
    category/coding הם אובייקט בודד. אינדקס על אובייקט → מתייחס לאובייקט כאלמנט (לא נכשל)."""
    # category=object, coding=object
    o = {"category": {"coding": {"code": "OLD"}}}
    assert _override_by_path(o, "category[0].coding[0].code", "M_PAT_HPV")
    assert o["category"]["coding"]["code"] == "M_PAT_HPV"
    # mixed: array + object
    m = {"category": [{"coding": {"code": "OLD"}}]}
    assert _override_by_path(m, "category[0].coding[0].code", "NEW")
    assert m["category"][0]["coding"]["code"] == "NEW"


def test_override_by_path_nested_and_list_autoindex():
    """דריסה לפי נתיב מלא: dict מקונן + auto-index [0] לרשימה כשהסגמנט אינו מספר."""
    obj = {"category": {"coding": {"code": "OLD"}},
           "entry": [{"resource": {"id": "a"}}]}
    assert _override_by_path(obj, "category.coding.code", "M_PAT_HPV")
    assert obj["category"]["coding"]["code"] == "M_PAT_HPV"
    # list ללא index → auto [0]
    assert _override_by_path(obj, "entry.resource.id", "b")
    assert obj["entry"][0]["resource"]["id"] == "b"
    # נתיב לא קיים → False (ה-caller יפול ל-leaf)
    assert not _override_by_path(obj, "category.coding.nope", "x")


def test_override_by_path_does_not_touch_sibling_value_leaves():
    """★ בטיחות FHIR: דריסה לפי נתיב מדויק לא נוגעת בשדות `value` אחים (הסיכון של leaf גנרי)."""
    fhir = {"identifier": {"value": "ID-TARGET"},
            "entry": [{"resource": {"valueQuantity": {"value": "99.9"}}},
                      {"resource": {"code": {"value": "KEEP"}}}]}
    assert _override_by_path(fhir, "identifier.value", "33245649")
    assert fhir["identifier"]["value"] == "33245649"
    # שאר ה-`value` לא נגעו
    assert fhir["entry"][0]["resource"]["valueQuantity"]["value"] == "99.9"
    assert fhir["entry"][1]["resource"]["code"]["value"] == "KEEP"


def test_apply_source_sample_builds_publish_from_sample_plus_overrides():
    """★ הרנר בונה את ה-publish מהדוגמה האמיתית + דריסות התסריט (לא מ-value של ה-LLM)."""
    sample = {"resourceType": "Bundle",
              "entry": [{"resource": {"resourceType": "DiagnosticReport",
                                      "category": {"coding": {"code": "OLD"}}}}],
              "identifier": {"value": "999"}}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-fhir-sample",
        source_sample=sample,
        source_overrides={"entry.resource.category.coding.code": "M_PAT_HPV"},
        actions=[KafkaPublishAction(topic="src", value={})],   # ה-LLM החזיר {} בכוונה
    )
    assert DotNetRunner()._apply_source_sample(ex) is True
    pub = ex.actions[0]
    assert pub.value["resourceType"] == "Bundle"               # בסיס מהדוגמה
    assert pub.value["entry"][0]["resource"]["category"]["coding"]["code"] == "M_PAT_HPV"  # דריסה הוחלה
    # deepcopy — המקור לא מושפע
    assert sample["entry"][0]["resource"]["category"]["coding"]["code"] == "OLD"


def test_apply_source_sample_leaf_fallback_when_path_missing():
    """דריסה לפי שם-שדה (leaf) ספציפי כשהנתיב המדויק לא נמצא — תאימות עם נתיב חלקי של ה-LLM."""
    sample = {"resourceType": "Bundle", "entry": [{"resource": {"examination_status": "OLD"}}]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-leaf",
        source_sample=sample,
        source_overrides={"examination_status": "final"},      # leaf ספציפי (לא גנרי)
        actions=[KafkaPublishAction(topic="src", value={})],
    )
    assert DotNetRunner()._apply_source_sample(ex) is True
    assert ex.actions[0].value["entry"][0]["resource"]["examination_status"] == "final"


def test_apply_source_sample_suffix_match_not_generic_leaf():
    """★ בטיחות FHIR: override 'category.coding.code' דורס רק את ה-code תחת category.coding,
    לא code גנרי אחר ב-Bundle (suffix-match ולא leaf 'code' בודד)."""
    sample = {"resourceType": "Bundle",
              "entry": [{"resource": {"category": {"coding": {"code": "OLD"}}}},
                        {"resource": {"type": {"coding": {"code": "KEEP"}}}}]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-suffix",
        source_sample=sample,
        source_overrides={"DiagnosticReport.category.coding.code": "M_PAT_HPV"},
        actions=[KafkaPublishAction(topic="src", value={})],
    )
    assert DotNetRunner()._apply_source_sample(ex) is True
    val = ex.actions[0].value
    assert val["entry"][0]["resource"]["category"]["coding"]["code"] == "M_PAT_HPV"
    assert val["entry"][1]["resource"]["type"]["coding"]["code"] == "KEEP"   # code אחר לא נגע


def test_apply_source_sample_remove_marker():
    """★ TC22: __REMOVE__ מסיר שדה ספציפי (ת"ז של רופא) — לא מרוקן את כל המערך. לתרחיש 'לא לבנות אובייקט'."""
    sample = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "Practitioner",
                      "identifier": [{"type": {"coding": [{"code": "NID"}]}, "value": "013"},
                                     {"type": {"coding": [{"code": "LN"}]}, "value": "030"}]}}]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-remove",
        source_sample=sample,
        source_overrides={"Practitioner.identifier[?(@.type.coding.code=='NID')]": "__REMOVE__"},
        actions=[KafkaPublishAction(topic="src", value={})],
    )
    assert DotNetRunner()._apply_source_sample(ex) is True
    ids = ex.actions[0].value["entry"][0]["resource"]["identifier"]
    assert [i.get("value") for i in ids] == ["030"]   # NID הוסר, LN נשאר (לא רוקן הכל)


def test_apply_source_sample_noop_without_sample():
    """★ אין source_sample → no-op (False) → המסלול הישן (value מה-LLM) ללא שינוי. תאימות MACKAF."""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-mackaf",
        actions=[KafkaPublishAction(topic="src", value={"_data": {"member_id": "555"}})],
    )
    assert DotNetRunner()._apply_source_sample(ex) is False
    assert ex.actions[0].value == {"_data": {"member_id": "555"}}   # ללא שינוי


# ============================================================
# Phase 3 — path-based unique-id injection (FHIR generic-leaf safety)
# ============================================================

def test_apply_unique_id_path_based_fhir_no_sibling_clobber():
    """★ key_built_from=identifier.value (leaf גנרי 'value') → ההזרקה לפי נתיב מדויק דורסת *רק*
    את identifier.value, ולא שדות `value` אחים ב-Bundle (הסיכון של §4)."""
    fhir = {"resourceType": "Bundle",
            "identifier": {"value": "999"},
            "entry": [{"resource": {"valueQuantity": {"value": "12.5"}}},
                      {"resource": {"code": {"value": "PAP"}}}]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-fhir-path",
        key_built_from=["ServiceRequest.identifier.value", "DiagnosticReport.status"],
        actions=[
            KafkaPublishAction(topic="src", value=fhir),
            KafkaWaitAction(topic="tgt", match={"entity_type": "lab"}),
        ],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    assert uid and uid != "999"
    pub, wait = ex.actions
    assert pub.value["identifier"]["value"] == uid                       # ★ נדרס לפי נתיב
    assert pub.value["entry"][0]["resource"]["valueQuantity"]["value"] == "12.5"  # אח לא נגע
    assert pub.value["entry"][1]["resource"]["code"]["value"] == "PAP"   # אח לא נגע
    assert wait.value_contains == uid                                    # קורלציה על ה-uid (key/גוף)


def test_sanitize_expected_fields_strips_identity_and_metadata():
    """★ מסיר KEY/זהות/metadata מ-expected_fields (מונע כשל-שווא על ערך ישן/ייחודי). שדות אמיתיים נשארים."""
    ef = {"_data.examination_type_code": "1", "_data.examination_type_name": "PAP/HPV",
          "_data.scc_message_id": "SCC-X", "_data.member_id": "999735863",
          "_data.member_id_code": "0", "header.mac_transaction_id": "G", "entity_id": "SCC-X"}
    removed = _sanitize_expected_fields(ef, strip_member=True)
    assert set(ef.keys()) == {"_data.examination_type_code", "_data.examination_type_name"}
    assert "_data.member_id" in removed and "entity_id" in removed and "header.mac_transaction_id" in removed


def test_sanitize_expected_fields_keeps_member_id_for_mackaf():
    """★ מסלול MACKAF (strip_member=False): member_id נשמר (הוא ה-uid המאומת), אבל scc/metadata עדיין מוסר."""
    ef = {"_data.parameters.0.member_id": "123", "_data.parameters.0.gender": "זכר",
          "scc_message_id": "X"}
    _sanitize_expected_fields(ef, strip_member=False)
    assert "_data.parameters.0.member_id" in ef and "_data.parameters.0.gender" in ef
    assert "scc_message_id" not in ef


def test_resolve_logical_holder_fhir_messageheader():
    """★ נתיב לוגי 'MessageHeader.id' → מאתר את ה-resource הנכון ב-Bundle.entry."""
    bundle = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "MessageHeader", "id": "SCC-TST.128.0004.260.HISTO.final.0"}},
        {"resource": {"resourceType": "Patient", "id": "X"}},
    ]}
    holder, field = _resolve_logical_holder(bundle, "MessageHeader.id")
    assert holder is bundle["entry"][0]["resource"] and field == "id"


def test_make_key_unique_replaces_first_digit_run_preserving_format():
    """★ ה-KEY נעשה ייחודי: רצף הספרות הראשון מוחלף ב-uid, הסיומת (HISTO.final.0) נשמרת."""
    bundle = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "MessageHeader",
                      "id": "SCC-TST.128128403.0004549368.260000548.HISTO.final.0"}}]}
    new = _make_key_unique(bundle, "MessageHeader.id", "49069711")
    assert new == "SCC-TST.49069711.0004549368.260000548.HISTO.final.0"   # רק הסגמנט הראשון
    assert bundle["entry"][0]["resource"]["id"] == new


def test_apply_unique_id_injects_into_key_source_path():
    """★★★ התיקון המרכזי: ה-uid מוזרק לשדה ה-KEY (MessageHeader.id→scc_message_id verbatim) → ה-KEY
    ביעד ייחודי. member_id (שעובר טרנספורמציה) לא נדרס. value_contains=uid → קורלציה לפי ה-KEY."""
    bundle = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "MessageHeader",
                      "id": "SCC-TST.128128403.0004549368.260000548.HISTO.final.0"}},
        {"resource": {"resourceType": "Patient", "identifier": [{"system": "PID", "value": "050526227"}]}},
    ]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-key",
        key_source_path="MessageHeader.id",
        source_sample=bundle,                       # מסלול מסר-דוגמה (FHIR) — מפעיל את הזרקת ה-KEY
        actions=[KafkaPublishAction(topic="src", value=bundle),
                 KafkaWaitAction(topic="tgt", match={"entity_type": "test_lab_result_approval"})],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    val = ex.actions[0].value
    mh_id = val["entry"][0]["resource"]["id"]
    assert uid in mh_id                                              # ה-uid בתוך ה-KEY field
    assert mh_id.endswith(".HISTO.final.0")                         # פורמט נשמר
    assert val["entry"][1]["resource"]["identifier"][0]["value"] == "050526227"  # member לא נגע
    assert ex.actions[1].value_contains == uid                      # קורלציה לפי ה-uid (KEY)


def test_apply_unique_id_token_in_source_sets_value_contains():
    """★ FHIR: ה-LLM שם __UNIQUE_ID__ בשדה ה-member id (כ-source_override). ה-runner מחליף אותו
    ב-uid (path-free, אמין) ומגדיר value_contains=uid → קורלציה לפי ה-uid בכל מקום ב-target."""
    fhir = {"resourceType": "Bundle",
            "entry": [{"resource": {"resourceType": "Patient",
                                    "identifier": [{"system": "x"}, {"value": "__UNIQUE_ID__"}]}}]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-token",
        key_built_from=["ServiceRequest.identifier.value"],   # leaf גנרי — הזרקת path עלולה להחמיץ
        actions=[
            KafkaPublishAction(topic="src", value=fhir),
            KafkaWaitAction(topic="tgt", match={"entity_type": "test_lab_result_approval"}),
        ],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    assert uid and uid != "__UNIQUE_ID__"
    pub, wait = ex.actions
    assert pub.value["entry"][0]["resource"]["identifier"][1]["value"] == uid   # token הוחלף
    assert wait.value_contains == uid                                           # ★ קורלציה לפי ה-uid
    # שדה ה-system לא נגע (לא דרסנו leaf גנרי בכל מקום)
    assert pub.value["entry"][0]["resource"]["identifier"][0]["system"] == "x"


def test_apply_unique_id_token_does_not_overinject_other_identifiers():
    """★ regression: ה-Bundle מלא ב-identifier.value (request/institute/practitioner). כשה-LLM
    שם __UNIQUE_ID__ בשדה אחד — ה-runner מזריק רק שם, ולא דורס את כל ה-identifier.value (29 הבאג)."""
    fhir = {"resourceType": "Bundle",
            "identifier": {"value": "260000548"},                      # message id — לא לגעת
            "entry": [
                {"resource": {"resourceType": "Patient",
                              "identifier": [{"value": "__UNIQUE_ID__"}]}},   # ה-member id (token)
                {"resource": {"resourceType": "ServiceRequest",
                              "identifier": [{"value": "REQ-12345"}]}},       # request — לא לגעת
                {"resource": {"resourceType": "Practitioner",
                              "identifier": [{"value": "LIC-999"}]}},          # practitioner — לא לגעת
            ]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-no-overinject",
        key_built_from=["ServiceRequest.identifier.value"],
        actions=[KafkaPublishAction(topic="src", value=fhir),
                 KafkaWaitAction(topic="tgt", match={"entity_type": "lab"})],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    val = ex.actions[0].value
    assert val["entry"][0]["resource"]["identifier"][0]["value"] == uid        # רק ה-token הוזרק
    assert val["identifier"]["value"] == "260000548"                           # message id נשמר
    assert val["entry"][1]["resource"]["identifier"][0]["value"] == "REQ-12345"  # request נשמר
    assert val["entry"][2]["resource"]["identifier"][0]["value"] == "LIC-999"   # practitioner נשמר


def test_apply_unique_id_no_token_single_match_only():
    """★ ללא token (fallback path-based) — מזריקים מופע **אחד** של identifier.value, לא את כולם."""
    fhir = {"resourceType": "Bundle",
            "entry": [{"resource": {"identifier": [{"value": "AAA"}]}},
                      {"resource": {"identifier": [{"value": "BBB"}]}}]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-single",
        key_built_from=["ServiceRequest.identifier.value"],
        actions=[KafkaPublishAction(topic="src", value=fhir),
                 KafkaWaitAction(topic="tgt", match={"entity_type": "lab"})],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    val = ex.actions[0].value
    vals = [val["entry"][0]["resource"]["identifier"][0]["value"],
            val["entry"][1]["resource"]["identifier"][0]["value"]]
    assert vals.count(uid) == 1                      # בדיוק אחד נדרס (לא שניהם)


def test_apply_unique_id_path_fallback_to_leaf_when_path_missing():
    """key_built_from עם נתיב שלא קיים במבנה בפועל → fallback ל-leaf-override (תאימות עם הטסט
    הקיים test_apply_unique_id_format_agnostic_via_key_built_from)."""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-fallback",
        key_built_from=["root.entity_id"],                              # אין 'root' wrapper במבנה
        actions=[
            KafkaPublishAction(topic="src", value={"entity_id": "777", "x": 1}),
            KafkaWaitAction(topic="tgt", match={"entity_type": "lab"}),
        ],
    )
    uid = DotNetRunner()._apply_unique_id(ex)
    assert uid and uid != "777"
    assert ex.actions[0].value["entity_id"] == uid                      # נדרס דרך leaf-fallback


# ============================================================
# Phase 2 — deterministic resolve + expected + verify_spec
# ============================================================

_IDX = {
    "by_target_path": {"_data.examination_type_code": "DiagnosticReport.category[0].coding[0].code",
                       "_data.member_name": "Patient.name",
                       "_data.scc_message_id": "MessageHeader.id"},
    "by_target_leaf": {"examination_type_code": "DiagnosticReport.category[0].coding[0].code",
                       "member_name": "Patient.name", "scc_message_id": "MessageHeader.id"},
    "rules": {"_data.examination_type_code": {"kind": "code_map", "map": {"M_PAT_HPV": "1", "M_CYT": "7"}},
              "_data.member_name": {"kind": "derived", "map": None},
              "_data.scc_message_id": {"kind": "verbatim", "map": None}},
    "target_paths": ["_data.examination_type_code", "_data.member_name", "_data.scc_message_id"],
}


def test_resolve_source_path_exact_and_leaf():
    assert _resolve_source_path(_IDX, "_data.examination_type_code") == "DiagnosticReport.category[0].coding[0].code"
    assert _resolve_source_path(_IDX, "examination_type_code") == "DiagnosticReport.category[0].coding[0].code"
    assert _resolve_source_path(_IDX, "nope") is None
    assert _resolve_source_path(None, "x") is None


def test_compute_expected_code_map_and_present():
    src = "DiagnosticReport.category[0].coding[0].code"
    # code_map + override M_PAT_HPV → "1"
    assert _compute_expected(_IDX, "_data.examination_type_code", {src: "M_PAT_HPV"}) == "1"
    assert _compute_expected(_IDX, "examination_type_code", {src: "M_CYT"}) == "7"
    # override value not in map → __PRESENT__ (לא literal שגוי)
    assert _compute_expected(_IDX, "_data.examination_type_code", {src: "WEIRD"}) == "__PRESENT__"
    # derived (member_name) → __PRESENT__ (לא ערך template)
    assert _compute_expected(_IDX, "_data.member_name", {}) == "__PRESENT__"


def test_parse_transform_rule_recognizes_all_kinds():
    """★ מנוע-חוקים עשיר: code_map/verbatim/concatenate/concat_multi/strip/positional/fixed; לא-ידוע→derived."""
    assert _parse_transform_rule("A=1, B=2")["kind"] == "code_map"
    assert _parse_transform_rule("verbatim")["kind"] == "verbatim"
    # concatenate (מקור-יחיד) — מזהה מפריד
    c = _parse_transform_rule("concatenate values with ;", "DiagnosticReport.organ")
    assert c["kind"] == "concatenate" and c["sep"] == ";"
    assert _parse_transform_rule("join by |", "X.list")["sep"] == "|"
    # concat_multi — ה-source הוא ביטוי '+' (כמה נתיבים)
    assert _parse_transform_rule("family + given", "Patient.name.family + name.given[0]")["kind"] == "concat_multi"
    # strip / positional / fixed
    assert _parse_transform_rule("strip leading zeros")["kind"] == "strip"
    assert _parse_transform_rule("first digit is type code")["kind"] == "positional"
    f = _parse_transform_rule("FIXED 510")
    assert f["kind"] == "fixed" and f["value"] == "510"
    # לא-ידוע → derived (ללא רגרסיה)
    assert _parse_transform_rule("resolve reference and encrypt")["kind"] == "derived"
    assert _parse_transform_rule("")["kind"] == "derived"


_IDX_RICH = {
    "by_target_path": {"_data.organ": "DiagnosticReport.organ", "_data.tz": "Patient.identifier.value"},
    "by_target_leaf": {"organ": "DiagnosticReport.organ", "tz": "Patient.identifier.value"},
    "rules": {"_data.organ": {"kind": "concatenate", "sep": ";", "map": None},
              "_data.tz": {"kind": "strip", "what": "leading_zeros", "map": None}},
    "target_paths": ["_data.organ", "_data.tz"],
}


def test_compute_expected_forward_concatenate_strip_from_sample():
    """★ forward מדויק: concatenate משרשר את רשימת-המקור מהדוגמה; strip מסיר אפסים מובילים."""
    sample = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "DiagnosticReport", "organ": ["LUNG", "LIVER"]}},
        {"resource": {"resourceType": "Patient", "identifier": {"value": "0004549368"}}},
    ]}
    # concatenate מהדוגמה (בלי override) → "LUNG;LIVER"
    assert _compute_expected(_IDX_RICH, "organ", {}, sample) == "LUNG;LIVER"
    # strip leading zeros → "4549368"
    assert _compute_expected(_IDX_RICH, "tz", {}, sample) == "4549368"
    # בלי sample → presence (לא ניתן לחשב)
    assert _compute_expected(_IDX_RICH, "organ", {}) == "__PRESENT__"


def test_apply_source_sample_ensure_multi_op():
    """★ __ENSURE_MULTI__: רשימת-מקור עם ערך-יחיד → מתווסף ערך שני (כדי שהשרשור יפיק מפריד). ≥2 → no-op."""
    idx = {"by_target_path": {"_data.organ": "DiagnosticReport.organ"},
           "by_target_leaf": {"organ": "DiagnosticReport.organ"},
           "rules": {"_data.organ": {"kind": "concatenate", "sep": ";", "map": None}},
           "target_paths": ["_data.organ"]}
    sample = {"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "DiagnosticReport", "organ": ["LUNG"]}}]}
    ex = DotNetExecutableTestCase(
        test_case_id="organ", transform_index=idx, source_sample=sample,
        source_overrides={"DiagnosticReport.organ": "__ENSURE_MULTI__"},
        actions=[KafkaPublishAction(topic="s", value={}), KafkaWaitAction(topic="t", match={})],
    )
    DotNetRunner()._apply_source_sample(ex)
    pub = next(a for a in ex.actions if isinstance(a, KafkaPublishAction))
    dr = [e["resource"] for e in pub.value["entry"] if e["resource"]["resourceType"] == "DiagnosticReport"][0]
    assert dr["organ"] == ["LUNG", "LUNG"]                       # נוסף ערך שני


def test_apply_verify_spec_builds_expected_fields():
    ex = DotNetExecutableTestCase(
        test_case_id="TC-vs",
        transform_index=_IDX,
        source_overrides={"DiagnosticReport.category[0].coding[0].code": "M_PAT_HPV"},
        verify_spec={"verify": [{"target_field": "examination_type_code"},
                                {"target_field": "_data.referral_practitioner", "expect": "absent"}]},
        actions=[KafkaPublishAction(topic="src", value={"x": 1}),
                 KafkaWaitAction(topic="tgt", match={"entity_type": "lab"})],
    )
    DotNetRunner()._apply_verify_spec(ex)
    ef = ex.actions[1].expected_fields
    assert ef["_data.examination_type_code"] == "1"          # code_map computed (leaf→canonical path)
    assert ef["_data.referral_practitioner"] == "__ABSENT__"  # absent marker


def test_apply_source_sample_set_first_char_preserves_rest():
    """★ ת"ז צה"ל: op set_first_char מחליף **רק** את התו הראשון של הערך המקורי מהדוגמה (0→2), ושומר את
    שאר 9 הספרות ואת האורך (10) — לא מפברק ערך. דינמי לכל שדה/אורך."""
    sample = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "Patient", "identifier": [{"system": "PID", "value": "0999735863"}]}}]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-tzahal",
        source_sample=sample,
        source_overrides={"Patient.identifier.value[system=PID]": "__SET_FIRST_CHAR__:2"},
        actions=[KafkaPublishAction(topic="src", value={}), KafkaWaitAction(topic="tgt", match={})],
    )
    DotNetRunner()._apply_source_sample(ex)
    pub = next(a for a in ex.actions if isinstance(a, KafkaPublishAction))
    pat = [e["resource"] for e in pub.value["entry"] if e["resource"]["resourceType"] == "Patient"][0]
    assert pat["identifier"][0]["value"] == "2999735863"     # תו ראשון 0→2, השאר נשמר


def test_apply_source_sample_converts_resource_for_absent_filter():
    """★ או/או: התסריט מאמת referral_practitioner (מקור PractitionerRole[code=R]) אך הדוגמה מכילה רק
    code=N (act). **ממירים** את הקיים N→R (לא מוסיפים) — באפיון זה שולחים מבצע *או* מפנה, לא שניהם.
    דינמי לכל סוג/פילטר."""
    idx = {
        "by_target_path": {"_data.referral_practitioner": "PractitionerRole[code=R].practitioner.reference"},
        "by_target_leaf": {"referral_practitioner": "PractitionerRole[code=R].practitioner.reference"},
        "rules": {"_data.referral_practitioner": {"kind": "verbatim", "map": None}},
        "target_paths": ["_data.referral_practitioner"],
    }
    sample = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "PractitionerRole", "code": "N",
                      "practitioner": {"reference": "Practitioner/1"}}},
        {"resource": {"resourceType": "Practitioner", "id": "1", "name": "Dr Cohen"}},
    ]}
    ex = DotNetExecutableTestCase(
        test_case_id="referral", transform_index=idx, source_sample=sample,
        verify_spec={"verify": [{"target_field": "referral_practitioner"}]},
        actions=[KafkaPublishAction(topic="s", value={}), KafkaWaitAction(topic="t", match={})],
    )
    DotNetRunner()._apply_source_sample(ex)
    pub = next(a for a in ex.actions if isinstance(a, KafkaPublishAction))
    roles = [e["resource"] for e in pub.value["entry"] if e["resource"]["resourceType"] == "PractitionerRole"]
    codes = sorted(r.get("code") for r in roles)
    assert codes == ["R"]                                    # הומר N→R במקום (לא נוסף) — או/או

    # ביקורת: תרחיש "השמט code=R" (remove) **לא** ממיר — אסור להמיר את מה שמבקשים להסיר
    ex2 = DotNetExecutableTestCase(
        test_case_id="omit", transform_index=idx,
        source_sample=copy.deepcopy(sample),
        source_overrides={"PractitionerRole[code=R].practitioner.reference": "__REMOVE__"},
        verify_spec={"verify": [{"target_field": "referral_practitioner", "expect": "absent"}]},
        actions=[KafkaPublishAction(topic="s", value={}), KafkaWaitAction(topic="t", match={})],
    )
    DotNetRunner()._apply_source_sample(ex2)
    pub2 = next(a for a in ex2.actions if isinstance(a, KafkaPublishAction))
    roles2 = [e["resource"] for e in pub2.value["entry"] if e["resource"]["resourceType"] == "PractitionerRole"]
    assert sorted(r.get("code") for r in roles2) == ["N"]    # לא הומר (התסריט מבקש היעדר code=R)


def test_apply_verify_spec_all_populated_and_sanitize():
    # מסר-דוגמה שמכיל בפועל את המקורות (DiagnosticReport.category, Patient.name) — אחרת verify_all מדלג עליהם
    sample = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "DiagnosticReport",
                      "category": [{"coding": [{"code": "M_PAT_HPV"}]}]}},
        {"resource": {"resourceType": "Patient", "name": [{"family": "כהן", "given": ["יוסי"]}]}},
        {"resource": {"resourceType": "MessageHeader", "id": "128"}},
    ]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-all",
        transform_index=_IDX,
        source_sample=sample,                               # → strip_member + מקורות קיימים
        verify_spec={"verify_all_populated": True},
        actions=[KafkaPublishAction(topic="src", value={}),
                 KafkaWaitAction(topic="tgt", match={})],
    )
    DotNetRunner()._apply_verify_spec(ex)
    ef = ex.actions[1].expected_fields
    assert ef["_data.examination_type_code"] == "__PRESENT__"
    assert ef["_data.member_name"] == "__PRESENT__"
    assert "_data.scc_message_id" not in ef                  # זהות → סונן ע"י _sanitize


def test_apply_verify_spec_all_populated_skips_absent_source():
    """★ verify_all מדלג על שדה שמקורו אינו בדוגמה (referral_practitioner כשאין PractitionerRole code=R) —
    כך 'ודא הכל מאוכלס' לא נכשל על שדה שה-Worker כלל לא יפיק. שדה שמקורו קיים (examination_type_code) נשמר."""
    idx = {
        "by_target_path": {"_data.examination_type_code": "DiagnosticReport.category[0].coding[0].code",
                           "_data.referral_practitioner": "PractitionerRole[code=R].practitioner.reference"},
        "by_target_leaf": {"examination_type_code": "DiagnosticReport.category[0].coding[0].code",
                           "referral_practitioner": "PractitionerRole[code=R].practitioner.reference"},
        "rules": {"_data.examination_type_code": {"kind": "code_map", "map": {"M_PAT_HPV": "1"}},
                  "_data.referral_practitioner": {"kind": "verbatim", "map": None}},
        "target_paths": ["_data.examination_type_code", "_data.referral_practitioner"],
    }
    sample = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "DiagnosticReport", "category": [{"coding": [{"code": "M_PAT_HPV"}]}]}},
        {"resource": {"resourceType": "PractitionerRole", "code": "N",         # רק מבצע (N), אין מפנה (R)
                      "practitioner": {"reference": "Practitioner/1"}}},
    ]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-absent", transform_index=idx, source_sample=sample,
        verify_spec={"verify_all_populated": True},
        actions=[KafkaPublishAction(topic="src", value={}), KafkaWaitAction(topic="tgt", match={})],
    )
    DotNetRunner()._apply_verify_spec(ex)
    ef = ex.actions[1].expected_fields
    assert ef.get("_data.examination_type_code") == "__PRESENT__"   # מקור קיים → נשמר
    assert "_data.referral_practitioner" not in ef                  # מקור (code=R) חסר → דולג


def test_apply_verify_spec_derived_literal_downgraded_to_present():
    """★ member_name (rule=derived, שרשור) — ה-LLM נתן literal 'כהן יוסי' שאינו תואם את שם-הדוגמה
    ('טסט טסט חדש'). שדה נגזר אינו בר-דריסה והערך תלוי-דוגמה → מאמתים נוכחות בלבד, לא את ה-literal."""
    ex = DotNetExecutableTestCase(
        test_case_id="TC-name",
        transform_index=_IDX,
        verify_spec={"verify": [{"target_field": "member_name", "expect": "כהן יוסי"}]},
        actions=[KafkaPublishAction(topic="src", value={}),
                 KafkaWaitAction(topic="tgt", match={})],
    )
    DotNetRunner()._apply_verify_spec(ex)
    assert ex.actions[1].expected_fields["_data.member_name"] == "__PRESENT__"   # לא 'כהן יוסי'

    # code_map עם literal — נשאר literal (לא נגזר)
    ex2 = DotNetExecutableTestCase(
        test_case_id="TC-code",
        transform_index=_IDX,
        verify_spec={"verify": [{"target_field": "examination_type_code", "expect": "7"}]},
        actions=[KafkaPublishAction(topic="src", value={}),
                 KafkaWaitAction(topic="tgt", match={})],
    )
    DotNetRunner()._apply_verify_spec(ex2)
    assert ex2.actions[1].expected_fields["_data.examination_type_code"] == "7"  # literal נשמר


def test_apply_verify_spec_noop_without_index():
    ex = DotNetExecutableTestCase(
        test_case_id="TC-noop",
        verify_spec={"verify_all_populated": True},          # אבל אין transform_index
        actions=[KafkaWaitAction(topic="tgt", match={}, expected_fields={"a": "1"})],
    )
    DotNetRunner()._apply_verify_spec(ex)
    assert ex.actions[0].expected_fields == {"a": "1"}       # ללא שינוי (מסלול ישן)


def test_anchored_end_to_end_source_and_expected():
    """★★★ אינטגרציה פאזה 1-3: override ממופה ל-source_path מדויק (DiagnosticReport, לא Observation),
    ו-expected מחושב דטרמיניסטית (M_PAT_HPV→1). מדמה את פלט ה-compiler המעוגן דרך ה-runner."""
    idx = {
        "by_target_path": {"_data.examination_type_code": "DiagnosticReport.category[0].coding[0].code"},
        "by_target_leaf": {"examination_type_code": "DiagnosticReport.category[0].coding[0].code"},
        "rules": {"_data.examination_type_code": {"kind": "code_map", "map": {"M_PAT_HPV": "1", "M_CYT": "7"}}},
        "target_paths": ["_data.examination_type_code"],
    }
    sample = {"resourceType": "Bundle", "entry": [
        {"resource": {"resourceType": "Observation", "category": [{"coding": [{"code": "OBS"}]}]}},   # decoy
        {"resource": {"resourceType": "DiagnosticReport", "category": [{"coding": [{"code": "M_PAT_HIST"}]}]}},
    ]}
    ex = DotNetExecutableTestCase(
        test_case_id="TC-e2e",
        transform_index=idx,
        source_sample=sample,
        source_overrides={"DiagnosticReport.category[0].coding[0].code": "M_PAT_HPV"},
        verify_spec={"verify": [{"target_field": "examination_type_code"}]},
        actions=[KafkaPublishAction(topic="src", value={}),
                 KafkaWaitAction(topic="tgt", match={})],
    )
    r = DotNetRunner()
    r._apply_source_sample(ex)
    r._apply_verify_spec(ex)
    val = ex.actions[0].value
    # ה-code נחת ב-DiagnosticReport הנכון, ה-Observation (decoy) לא נגע
    assert val["entry"][1]["resource"]["category"][0]["coding"][0]["code"] == "M_PAT_HPV"
    assert val["entry"][0]["resource"]["category"][0]["coding"][0]["code"] == "OBS"
    # expected מחושב: M_PAT_HPV → 1
    assert ex.actions[1].expected_fields["_data.examination_type_code"] == "1"


def test_override_by_path_pb_filter_syntax():
    """★ תחביר הפילטר של ה-Payload Builder ([system=PID], multi-key [system=ICD,version=2]) —
    נפתר כמו JSONPath, לא נחשב למפתח literal (תיקון לכשל ה-צה"ל/organ)."""
    # single-key [system=PID]
    p = {"identifier": [{"system": "MRN", "value": "a"}, {"system": "PID", "value": "b"}]}
    assert _override_by_path(p, "identifier[system=PID].value", "X")
    assert p["identifier"][1]["value"] == "X" and p["identifier"][0]["value"] == "a"
    # multi-key [system=ICD,version=2]
    c = {"coding": [{"system": "ICD", "version": "1", "display": "x"},
                    {"system": "ICD", "version": "2", "display": "y"}]}
    assert _override_by_path(c, "coding[system=ICD,version=2].display", "Z")
    assert c["coding"][1]["display"] == "Z" and c["coding"][0]["display"] == "x"


def test_normalize_filter_position():
    """★ ה-PB שם פילטר על leaf סקלרי (value[system=PID]) — מעבירים ל-list שמכיל (identifier)."""
    from agents.runner.dotnet_runner import _normalize_filter_position as nf
    assert nf("Patient.identifier.value[system=PID]") == "Patient.identifier[system=PID].value"
    # פילטר על ה-list עצמו → לא נוגעים
    assert nf("PractitionerRole[code=R].practitioner.reference") == "PractitionerRole[code=R].practitioner.reference"
    # אינדקס [0] → לא מזיזים
    assert nf("category[0].coding[0].code") == "category[0].coding[0].code"
