"""E2E test ב-mock mode: מ-CopilotBridgeMock ועד reporter."""

from __future__ import annotations

import os

import pytest

# נכפה mock mode לפני יבוא modules התלויים ב-settings
os.environ["RUNNER_MODE"] = "mock"
os.environ["COPILOT_CONNECTION_STRING"] = ""
os.environ["AZURE_OPENAI_KEY"] = ""

from agents.copilot_bridge import CopilotBridgeMock  # noqa: E402
from pipeline.esb_pipeline import run_esb_pipeline  # noqa: E402
from server.chat_session import ChatSession  # noqa: E402


@pytest.mark.asyncio
async def test_pipeline_runs_in_mock_mode_without_postman():
    bridge = CopilotBridgeMock()
    sid = "e2e-1"
    await bridge.start_session(sid)
    await bridge.send_document(sid, "spec דמה", filename="spec.txt")
    await bridge.send(sid, "תקין")
    final = await bridge.send(sid, "123456")
    completion = bridge.is_completion_message(final)
    assert completion is not None

    session = ChatSession(session_id=sid)
    session.suite_id = completion.suite_id
    session.phase = "B_pipeline"

    # auto-approve bugs ברקע
    import asyncio

    async def auto_approve():
        for _ in range(50):
            await asyncio.sleep(0.05)
            if session.bugs_decision and not session.bugs_decision.done():
                session.bugs_decision.set_result(True)
                return
    asyncio.create_task(auto_approve())

    result = await run_esb_pipeline(session)
    assert result.suite_id == 999
    assert result.total > 0
    # mock runner מציג ~30% failures
    assert result.passed + result.failed + result.blocked == result.total
