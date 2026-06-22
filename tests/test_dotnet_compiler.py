"""טסטים ל-DotNetCompiler — regex extraction של 3 ה-actions."""

from __future__ import annotations

import os

import pytest

# בלי Azure OpenAI — הטסטים בודקים רק את ה-regex fast path
os.environ["AZURE_OPENAI_KEY"] = ""

from agents.compiler.dotnet_compiler import DotNetCompiler  # noqa: E402
from models.dotnet_test_case import (  # noqa: E402
    CouchbaseWaitAction,
    KafkaPublishAction,
    KafkaWaitAction,
)


@pytest.mark.asyncio
async def test_regex_extracts_publish_and_wait():
    """Publish + Wait kafka → 2 actions."""
    compiler = DotNetCompiler(spec_md=None)
    raw = {
        "id": 1,
        "title": "TC-01 flow תקין",
        "text": (
            "פרסם ל-topic patient.admission.input את {\"patient_id\": \"123\"}\n"
            "ודא שמסר הגיע ל-topic patient.admission.enriched תוך 30 שניות"
        ),
    }
    ex = await compiler.compile(raw)
    assert ex.test_case_id == "TC-01 flow תקין"
    assert len(ex.actions) == 2
    assert isinstance(ex.actions[0], KafkaPublishAction)
    assert ex.actions[0].topic == "patient.admission.input"
    assert ex.actions[0].value == {"patient_id": "123"}
    assert isinstance(ex.actions[1], KafkaWaitAction)
    assert ex.actions[1].topic == "patient.admission.enriched"
    assert ex.actions[1].timeout_seconds == 30


@pytest.mark.asyncio
async def test_regex_extracts_couchbase_wait():
    """publish ל-Kafka + couchbase_wait."""
    compiler = DotNetCompiler(spec_md=None)
    raw = {
        "id": 2,
        "title": "TC-02 כתיבה ל-CB",
        "text": (
            "פרסם ל-topic guest.creation.input את {\"id\": \"X\"}\n"
            "ודא שמסמך נכתב ל-Couchbase bucket guests key=X"
        ),
    }
    ex = await compiler.compile(raw)
    assert len(ex.actions) == 2
    assert isinstance(ex.actions[1], CouchbaseWaitAction)
    assert ex.actions[1].bucket == "guests"
    assert ex.actions[1].key == "X"


@pytest.mark.asyncio
async def test_compile_blocks_when_no_actions_detected():
    """טקסט בלי publish/wait → blocked (actions=[])."""
    compiler = DotNetCompiler(spec_md=None)
    raw = {"id": 3, "title": "TC-3 garbage", "text": "do something vague"}
    ex = await compiler.compile(raw)
    assert ex.actions == []
    assert "לא ניתן לחלץ" in (ex.compiler_notes or "")


@pytest.mark.asyncio
async def test_expected_fields_captured_after_wait():
    """ודא שמסר הגיע + with field X=Y → expected_fields של ה-wait."""
    compiler = DotNetCompiler(spec_md=None)
    raw = {
        "id": 4,
        "title": "TC-04 אסרשנים",
        "text": (
            "פרסם ל-topic in את {\"a\": 1}\n"
            "ודא שמסר הגיע ל-topic out with status=enriched with priority=high"
        ),
    }
    ex = await compiler.compile(raw)
    wait = next(a for a in ex.actions if a.kind == "kafka_wait")
    assert wait.expected_fields.get("status") == "enriched"
    assert wait.expected_fields.get("priority") == "high"


def test_parse_llm_response_stamps_sample_and_overrides():
    """★ Phase 2: כשיש sample_messages → ה-executable מקבל source_sample + source_overrides
    מתשובת ה-LLM (הרנר יבנה מהם את ה-publish דטרמיניסטית)."""
    sample = {"resourceType": "Bundle", "identifier": {"value": "999"}}
    compiler = DotNetCompiler(spec_md=None, sample_messages=[sample])
    data = {
        "source_overrides": {"category.coding.code": "M_PAT_HPV"},
        "actions": [
            {"kind": "kafka_publish", "topic": "src", "value": {}},
            {"kind": "kafka_wait", "topic": "tgt", "match": {"entity_type": "lab"}},
        ],
    }
    ex = compiler._parse_llm_response("TC-fhir", None, "text", data, source_label="templates")
    assert ex is not None
    assert ex.source_sample == sample
    assert ex.source_overrides == {"category.coding.code": "M_PAT_HPV"}


def test_parse_llm_response_no_sample_leaves_fields_empty():
    """★ תאימות לאחור: אין sample_messages → source_sample=None, source_overrides={} (מסלול MACKAF)."""
    compiler = DotNetCompiler(spec_md=None)
    data = {"actions": [{"kind": "kafka_publish", "topic": "src", "value": {"a": 1}}]}
    ex = compiler._parse_llm_response("TC-mackaf", None, "text", data, source_label="templates")
    assert ex is not None
    assert ex.source_sample is None
    assert ex.source_overrides == {}


# ============================================================
# Phase 3 — anchored contract: _parse_anchored_response
# ============================================================

_ANCHORED_IDX = {
    "by_target_path": {"_data.examination_type_code": "DiagnosticReport.category[0].coding[0].code"},
    "by_target_leaf": {"examination_type_code": "DiagnosticReport.category[0].coding[0].code"},
    "rules": {"_data.examination_type_code": {"kind": "code_map", "map": {"M_PAT_HPV": "1"}}},
    "target_paths": ["_data.examination_type_code"],
}


def test_parse_anchored_response_maps_overrides_to_source_paths():
    """★ פאזה 3: ה-LLM נותן שדה לוגי (examination_type_code) + ערך; המערכת ממפה ל-source_path מדויק."""
    sample = {"resourceType": "Bundle"}
    c = DotNetCompiler(payload_templates={"source_topic": "src", "target_topic": "tgt",
                                          "templates": {"create": {}}},
                       sample_messages=[sample], transform_index=_ANCHORED_IDX)
    data = {
        "action_type": "create",
        "overrides": [{"target_field": "examination_type_code", "value": "M_PAT_HPV"}],
        "verify": [{"target_field": "examination_type_code"}],
        "expect_no_message": False, "timeout_seconds": 150,
    }
    ex = c._parse_anchored_response("TC-anchored", None, "text", data)
    assert ex is not None
    # ה-override מופה ל-source_path המדויק (DiagnosticReport.category...), לא נשאר שם לוגי
    assert ex.source_overrides == {"DiagnosticReport.category[0].coding[0].code": "M_PAT_HPV"}
    assert ex.verify_spec["verify"] == [{"target_field": "examination_type_code"}]
    assert ex.source_sample == sample
    kinds = [a.kind for a in ex.actions]
    assert "kafka_publish" in kinds and "kafka_wait" in kinds


def test_parse_anchored_response_remove_and_negative():
    c = DotNetCompiler(payload_templates={"source_topic": "s", "target_topic": "t", "templates": {"create": {}}},
                       sample_messages=[{"resourceType": "Bundle"}], transform_index=_ANCHORED_IDX)
    data = {"overrides": [{"target_field": "examination_type_code", "op": "remove"}],
            "expect_no_message": True}
    ex = c._parse_anchored_response("TC-neg", None, "t", data)
    assert ex.source_overrides == {"DiagnosticReport.category[0].coding[0].code": "__REMOVE__"}
    wait = next(a for a in ex.actions if a.kind == "kafka_wait")
    assert wait.expect_no_message is True


def test_parse_anchored_response_unresolved_field_skipped():
    """★ שדה שאינו ב-transformations → מדולג (לא ניחוש), נרשם ב-compiler_notes."""
    c = DotNetCompiler(payload_templates={"source_topic": "s", "target_topic": "t", "templates": {"create": {}}},
                       sample_messages=[{"resourceType": "Bundle"}], transform_index=_ANCHORED_IDX)
    data = {"overrides": [{"target_field": "nonexistent_field", "value": "x"}]}
    ex = c._parse_anchored_response("TC-u", None, "t", data)
    assert ex.source_overrides == {}                     # לא הוזרק כלום
    assert "nonexistent_field" in (ex.compiler_notes or "")
