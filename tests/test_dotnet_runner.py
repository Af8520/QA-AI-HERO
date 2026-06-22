"""טסטים ל-DotNetRunner — בעיקר ל-pure helpers ולמצב BLOCKED.

קריאות אמיתיות ל-Kafka/Couchbase לא נבדקות כי אין broker זמין ב-CI.
מבוטל אוטומטית כש-KAFKA_BOOTSTRAP_SERVERS לא מוגדר.
"""

from __future__ import annotations

import os

import pytest

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = ""
os.environ["COUCHBASE_CONNECTION_STRING"] = ""

from agents.runner.dotnet_runner import (  # noqa: E402
    DotNetRunner,
    _check_expected_fields,
    _make_key_unique,
    _matches,
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


def test_check_expected_fields_leaf_name_fallback():
    """★ נתיב שגוי של ה-LLM (resource_type תחת _data אבל נכתב parameters.0.resource_type) —
    fallback גורף לפי שם-השדה האחרון מוצא אותו בכל מקום ב-tree."""
    value = {"_data": {"resource_type": "parameters", "parameters": [{"member_id": "555"}]}}
    assert _check_expected_fields(value, {"_data.parameters.0.resource_type": "parameters"}) == []
    # שם-שדה שלא קיים בשום מקום → missing
    assert any("missing" in i for i in _check_expected_fields(value, {"_data.x.no_field": "y"}))


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
