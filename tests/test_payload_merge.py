"""טסטים לפיצ'ר Payload Builder:
- _try_extract_json_object של ה-bridge (פרסור תשובת DirectLine)
- compile() של DotNetCompiler במצב templates-mode (LLM mocked)
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# בלי Azure OpenAI במצב default
os.environ.setdefault("AZURE_OPENAI_KEY", "")

from agents.compiler.dotnet_compiler import DotNetCompiler  # noqa: E402
from agents.payload_builder.payload_builder_bridge import _try_extract_json_object  # noqa: E402
from models.dotnet_test_case import KafkaPublishAction, KafkaWaitAction  # noqa: E402


# ============================================================
# _try_extract_json_object — בלי שיחת רשת
# ============================================================

def test_extract_json_object_fenced():
    text = 'בלה בלה\n```json\n{"source_topic": "t", "templates": {"create": {}}}\n```\nסיום'
    obj = _try_extract_json_object(text, required_keys=("templates", "source_topic"))
    assert obj is not None
    assert obj["source_topic"] == "t"


def test_extract_json_object_raw():
    text = 'הנה הטמפלייטס: {"source_topic":"t","templates":{}} עוד טקסט'
    obj = _try_extract_json_object(text, required_keys=("source_topic",))
    assert obj is not None
    assert obj["source_topic"] == "t"


def test_extract_json_object_missing_required_returns_none():
    text = '{"foo": 1}'
    obj = _try_extract_json_object(text, required_keys=("templates",))
    assert obj is None


def test_extract_json_object_invalid_json():
    text = '{invalid: json}'
    obj = _try_extract_json_object(text, required_keys=())
    assert obj is None


# ============================================================
# DotNetCompiler עם templates — LLM mocked
# ============================================================

PAYLOAD_TEMPLATES = {
    "source_topic": "patient.input",
    "target_topic": "patient.enriched",
    "templates": {
        "create": {
            "headers": {"event_type": "create"},
            "root": {"id": "{{id}}", "type_code": "00000"},
            "_data": {"first_name": "", "last_name": ""},
        }
    },
    "field_catalog": {
        "type_code": {"type": "string", "required": True},
        "first_name": {"type": "string", "required": True},
    },
}


@pytest.mark.asyncio
async def test_compile_uses_llm_when_templates_present():
    """אם יש templates ו-Azure OpenAI מופעל — Compiler קורא ל-LLM עם prompt חדש."""
    # מוקאפ של LLM שמחזיר תשובת actions תקנית
    mock_resp = MagicMock()
    mock_resp.choices = [MagicMock(message=MagicMock(content=(
        '{"test_case_id": "TC-1", "actions": ['
        '{"kind": "kafka_publish", "topic": "patient.input", '
        '"value": {"headers": {"event_type": "create"}, "root": {"type_code": "99918"}, "_data": {}}},'
        '{"kind": "kafka_wait", "topic": "patient.enriched", "expect_no_message": false, "timeout_seconds": 30}'
        '], "expected_status": 200, "compiler_notes": "type_code override"}'
    )))]

    mock_client = MagicMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_resp)

    # נדלק AZURE_OPENAI_KEY כדי ש-azure_openai_enabled יחזיר True
    with patch("agents.compiler.dotnet_compiler.settings") as mock_settings, \
         patch("agents.compiler.dotnet_compiler._make_openai_client", return_value=mock_client):
        mock_settings.azure_openai_enabled = True
        mock_settings.AZURE_OPENAI_DEPLOYMENT = "gpt-x"
        mock_settings.KAFKA_DEFAULT_TIMEOUT_SECONDS = 30
        mock_settings.COUCHBASE_DEFAULT_TIMEOUT_SECONDS = 30

        compiler = DotNetCompiler(payload_templates=PAYLOAD_TEMPLATES)
        ex = await compiler.compile({
            "id": 1,
            "title": "TC-1 type_code override",
            "text": "פתח אורח עם type_code=99918",
        })

    assert len(ex.actions) == 2
    assert isinstance(ex.actions[0], KafkaPublishAction)
    assert ex.actions[0].topic == "patient.input"
    # ה-value מכיל את ה-override
    assert ex.actions[0].value["root"]["type_code"] == "99918"
    assert isinstance(ex.actions[1], KafkaWaitAction)
    assert ex.actions[1].expect_no_message is False
    assert "templates" in (ex.compiler_notes or "") or "override" in (ex.compiler_notes or "")


@pytest.mark.asyncio
async def test_compile_negative_test_marks_expect_no_message():
    """LLM אומר expect_no_message=true → KafkaWaitAction מסומן ככזה."""
    mock_resp = MagicMock()
    mock_resp.choices = [MagicMock(message=MagicMock(content=(
        '{"test_case_id": "TC-neg", "actions": ['
        '{"kind": "kafka_publish", "topic": "patient.input", "value": {"root": {"type_code": "INVALID"}}},'
        '{"kind": "kafka_wait", "topic": "patient.enriched", "expect_no_message": true, "timeout_seconds": 10}'
        '], "expected_status": 200, "compiler_notes": "negative"}'
    )))]

    mock_client = MagicMock()
    mock_client.chat.completions.create = AsyncMock(return_value=mock_resp)

    with patch("agents.compiler.dotnet_compiler.settings") as mock_settings, \
         patch("agents.compiler.dotnet_compiler._make_openai_client", return_value=mock_client):
        mock_settings.azure_openai_enabled = True
        mock_settings.AZURE_OPENAI_DEPLOYMENT = "gpt-x"
        mock_settings.KAFKA_DEFAULT_TIMEOUT_SECONDS = 30
        mock_settings.COUCHBASE_DEFAULT_TIMEOUT_SECONDS = 30

        compiler = DotNetCompiler(payload_templates=PAYLOAD_TEMPLATES)
        ex = await compiler.compile({
            "id": 99,
            "title": "TC-99 negative",
            "text": "פתח עם type_code שגוי. ודא שלא נשלח ל-target.",
        })

    wait = next(a for a in ex.actions if a.kind == "kafka_wait")
    assert wait.expect_no_message is True


@pytest.mark.asyncio
async def test_compile_falls_back_to_regex_when_templates_mode_fails():
    """אם ה-LLM נכשל בכל זאת — נופלים ל-regex-only mode."""
    mock_client = MagicMock()
    mock_client.chat.completions.create = AsyncMock(side_effect=Exception("LLM down"))

    with patch("agents.compiler.dotnet_compiler.settings") as mock_settings, \
         patch("agents.compiler.dotnet_compiler._make_openai_client", return_value=mock_client):
        mock_settings.azure_openai_enabled = True
        mock_settings.AZURE_OPENAI_DEPLOYMENT = "gpt-x"
        mock_settings.KAFKA_DEFAULT_TIMEOUT_SECONDS = 30
        mock_settings.COUCHBASE_DEFAULT_TIMEOUT_SECONDS = 30

        compiler = DotNetCompiler(payload_templates=PAYLOAD_TEMPLATES)
        ex = await compiler.compile({
            "id": 2,
            "title": "TC-2 regex fallback",
            "text": "פרסם ל-topic patient.input את {\"id\": 5} ודא שמסר הגיע ל-topic patient.enriched",
        })

    # regex תפס משהו
    assert len(ex.actions) == 2
    assert any(a.kind == "kafka_publish" for a in ex.actions)
