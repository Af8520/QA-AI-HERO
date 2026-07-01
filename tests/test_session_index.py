"""טסטים לניהול-סשנים + הרצה-חוזרת: אינדקס-הדיסק ו-reload מהקאש (בלי Copilot)."""

from __future__ import annotations

import json

import pytest

from server.chat_session import ChatSession, store
from server.routes import (
    _reload_session_from_disk,
    _read_sessions_index,
    _upsert_session_index,
)


def _mk(sid, **kw):
    s = ChatSession(session_id=sid)
    for k, v in kw.items():
        setattr(s, k, v)
    return s


def test_index_upsert_and_merge(monkeypatch, tmp_path):
    """append-only JSONL; הקריאה מאחדת לפי session_id (האחרון מנצח) — בלי כפילויות."""
    monkeypatch.chdir(tmp_path)
    _upsert_session_index(_mk("s1", department="dotnet",
                              phase_a_json_file="a.json", direct_test_cases=[{"x": 1}]))
    _upsert_session_index(_mk("s2", department="esb"))
    _upsert_session_index(_mk("s1", department="dotnet", phase="done", run_id="RUN9",
                              direct_test_cases=[{"x": 1}, {"y": 2}]))
    items = _read_sessions_index()
    assert {r["session_id"] for r in items} == {"s1", "s2"}          # מאוחד, לא כפול
    s1 = next(r for r in items if r["session_id"] == "s1")
    assert s1["run_id"] == "RUN9" and s1["phase"] == "done"          # הרשומה האחרונה מנצחת
    assert s1["n_test_cases"] == 2


@pytest.mark.asyncio
async def test_reload_session_from_disk(monkeypatch, tmp_path):
    """reload טוען test_cases + payload_templates + sample_messages + spec מהדיסק ל-session חדש."""
    monkeypatch.chdir(tmp_path)
    paj = tmp_path / "phase_a.json"
    paj.write_text(json.dumps([{"test_case_id": "TC1",
                                "steps": [{"step": "פרסם ל-topic X את {}", "expected_result": "ok"}]}]),
                   encoding="utf-8")
    ptf = tmp_path / "pt.json"
    ptf.write_text(json.dumps({"source_topic": "s", "target_topic": "t", "templates": {"create": {}}}),
                   encoding="utf-8")
    smf = tmp_path / "sm.json"
    smf.write_text(json.dumps([{"resourceType": "Bundle"}]), encoding="utf-8")
    spf = tmp_path / "spec.txt"
    spf.write_text("SPEC CONTENT", encoding="utf-8")
    _upsert_session_index(_mk("orig", department="dotnet", phase_a_json_file=str(paj),
                              payload_templates_file=str(ptf), sample_messages_file=str(smf),
                              spec_file=str(spf), direct_test_cases=[{"x": 1}]))

    s = await _reload_session_from_disk("orig")
    try:
        assert s.session_id != "orig"                               # session_id חדש
        assert s.department == "dotnet"
        assert s.payload_templates and s.payload_templates["source_topic"] == "s"
        assert s.sample_source_messages == [{"resourceType": "Bundle"}]
        assert s.spec_text == "SPEC CONTENT"
        assert s.direct_test_cases                                  # foundry_to_raw_cases רץ
    finally:
        await store.delete(s.session_id)


@pytest.mark.asyncio
async def test_reload_missing_session_raises(monkeypatch, tmp_path):
    from fastapi import HTTPException
    monkeypatch.chdir(tmp_path)
    with pytest.raises(HTTPException):
        await _reload_session_from_disk("does-not-exist")
