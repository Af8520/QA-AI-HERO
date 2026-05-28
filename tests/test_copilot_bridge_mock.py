"""Unit tests ל-CopilotBridgeMock + completion detection."""

from __future__ import annotations

import pytest

from agents.copilot_bridge import CopilotBridgeMock


@pytest.mark.asyncio
async def test_full_flow_mock():
    bridge = CopilotBridgeMock()
    sid = "s1"

    greeting = await bridge.start_session(sid)
    assert "שלום" in greeting

    # Document upload -> table preview
    table_msg = await bridge.send_document(sid, "API להוספת מטופל", filename="spec.txt")
    assert "תרחיש" in table_msg or "טבלת" in table_msg

    # Approval -> ask for US
    us_prompt = await bridge.send(sid, "תקין")
    assert "US" in us_prompt or "ספרות" in us_prompt

    # Provide US -> success message
    final = await bridge.send(sid, "123456")
    completion = bridge.is_completion_message(final)
    assert completion is not None
    assert completion.suite_id == 999


@pytest.mark.asyncio
async def test_invalid_us_rejected():
    bridge = CopilotBridgeMock()
    sid = "s2"
    await bridge.start_session(sid)
    await bridge.send_document(sid, "spec", filename="x")
    await bridge.send(sid, "תקין")
    msg = await bridge.send(sid, "abc")
    assert bridge.is_completion_message(msg) is None


def test_completion_detection_various_formats():
    bridge = CopilotBridgeMock()
    samples_positive = [
        "התסריטים הועלו בהצלחה ל-ADO suite 12345",
        "uploaded successfully to ADO suite 7777",
        "התיקייה 555 נוצרה והכל הועלה בהצלחה",
    ]
    for s in samples_positive:
        c = bridge.is_completion_message(s)
        assert c is not None, f"failed: {s}"
        assert c.suite_id > 0

    samples_negative = [
        "אנא תן לי מספר US",
        "הטבלה החדשה",
        "",
    ]
    for s in samples_negative:
        assert bridge.is_completion_message(s) is None, f"false positive: {s}"
