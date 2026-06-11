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
