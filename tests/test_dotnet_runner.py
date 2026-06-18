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
    _matches,
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
    uid = DotNetRunner()._apply_unique_id(ex)
    assert uid and uid.isdigit() and uid != "__UNIQUE_ID__"
    pub, wait = ex.actions
    assert pub.value["_data"]["member_details"]["member_id"] == uid   # מקור
    assert wait.key_contains == uid
    assert wait.match["_data.parameters.0.member_id"] == uid          # correlation
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
    assert pub.value["_data"]["member_details"]["member_id"] == uid          # ★ המקור נדרס
    assert pub.value["_data"]["member_details"]["member_id_code"] == "0"     # code לא נגע
    assert wait.key_contains == uid
    assert wait.match["_data.parameters.0.member_id"] == uid
    assert wait.expected_fields["_data.parameters.0.member_id"] == uid
    assert wait.match["entity_type"] == "child_development"


def test_apply_unique_id_injects_correlation_into_negative_wait():
    """★ הבאג בתרחיש שלילי: ה-wait (expect_no_message) בלי member_id ב-match → היה תופס מסר
    child_development אקראי ונכשל. ה-runner מזריק את ה-id הייחודי לקורלציה → השלילי מחפש את
    ה-id שלנו (שלא הופק) → timeout → PASS נכון."""
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
    assert pub.value["_data"]["member_details"]["member_id"] == uid
    assert wait.match["_data.parameters.0.member_id"] == uid     # ★ הוזרק לקורלציה
    assert wait.match["entity_type"] == "child_development"      # הקיים נשמר
    assert wait.key_contains == uid


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
    # list index מחוץ לטווח → missing
    assert any("missing" in i for i in _check_expected_fields(value, {"_data.parameters.5.gender": "x"}))


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
