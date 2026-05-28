"""טסטים ל-SmartCompiler.

מטעם שאין Azure OpenAI ב-CI, הטסטים בעיקר על:
- Fallback path (אין LLM) → רנדור template עם env vars
- Fallback path (אין collection match) → BLOCKED placeholder
- בנייה של ExecutableTestCase מנכון מ-LLM response (mock)
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

# ביטול Azure OpenAI כדי שה-Compiler יפול ל-fallback באופן צפוי
os.environ["AZURE_OPENAI_KEY"] = ""

from agents.compiler.smart_compiler import SmartCompiler  # noqa: E402
from agents.postman.postman_loader import load_collection_from_file  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def collection():
    return load_collection_from_file(str(FIXTURES / "sample_collection.json"))


@pytest.fixture
def env_vars():
    return {"baseUrl": "https://api-test.local/esb", "patientId": "123456", "token": "test-token"}


@pytest.mark.asyncio
async def test_compile_falls_back_to_template_without_llm(collection, env_vars):
    """ללא Azure OpenAI: ה-Compiler חייב להחזיר ExecutableTestCase עם ה-template rendered."""
    compiler = SmartCompiler(spec_md=None, collection=collection, env_vars=env_vars)
    raw_tc = {
        "id": 100,
        "title": "POST /patients/admit",  # תואם לשם ה-request
        "text": "שלח admission תקין",
    }
    ex = await compiler.compile(raw_tc)
    assert ex.test_case_id == "POST /patients/admit"
    assert ex.ado_test_case_id == 100
    assert ex.request.method == "POST"
    assert "api-test.local" in ex.request.url
    assert "patients/admit" in ex.request.url
    # body צריך להיות rendered
    assert ex.request.body is not None
    body_str = json.dumps(ex.request.body) if isinstance(ex.request.body, dict) else ex.request.body
    assert "123456" in str(body_str)
    assert "fallback" in (ex.compiler_notes or "").lower()


@pytest.mark.asyncio
async def test_compile_returns_blocked_when_no_collection():
    """ללא collection — ExecutableTestCase מסומן BLOCKED עם about:blank."""
    compiler = SmartCompiler(spec_md=None, collection=None, env_vars={})
    ex = await compiler.compile({"id": 1, "title": "TC1", "text": "do something"})
    assert ex.request.url == "about:blank"
    assert ex.expected_response.status == 0


@pytest.mark.asyncio
async def test_compile_returns_blocked_when_no_match(collection):
    """אם השם של ה-tc לא תואם לשום request — Compiler מחזיר את הראשון בקולקציה (per llm_request_matcher fallback)."""
    compiler = SmartCompiler(spec_md=None, collection=collection, env_vars={})
    ex = await compiler.compile({"id": 1, "title": "תרחיש שלא קיים בכלל", "text": "irrelevant"})
    # ה-matcher מחזיר את הראשון אם אין התאמה — לא about:blank
    assert ex.request.url != "about:blank"
    # אבל ה-method יהיה תקני
    assert ex.request.method in {"GET", "POST", "PUT", "DELETE"}


@pytest.mark.asyncio
async def test_build_executable_from_llm_response(collection, env_vars):
    """ולידציה ל-_build_executable: בהינתן data מ-LLM, נבנה ExecutableTestCase תקני."""
    compiler = SmartCompiler(spec_md="# spec", collection=collection, env_vars=env_vars)
    template = collection.find_by_name("POST /patients/admit")
    rendered = compiler._render_template(template)

    llm_data = {
        "test_case_id": "TC-INVALID-MEMBER",
        "request": {
            "method": "POST",
            "url": "https://api-test.local/esb/patients/admit",
            "headers": {"Content-Type": "application/json"},
            "body": {"member_id": "abc-not-int", "patient_name": "Cohen"},
        },
        "expected_response": {"status": 400, "schema_assertions": {}},
        "kafka_assertion": None,
        "elastic_assertion": {"index": "esb-logs-*", "query": "patientId:123", "must_not_contain_level": "ERROR"},
        "compiler_notes": "Replaced member_id with invalid string",
    }

    ex = compiler._build_executable(
        test_case_id="TC-INVALID-MEMBER",
        ado_id=999,
        text="member_id לא תקין",
        data=llm_data,
        fallback_request=rendered,
    )
    assert ex.test_case_id == "TC-INVALID-MEMBER"
    assert ex.ado_test_case_id == 999
    assert ex.request.body["member_id"] == "abc-not-int"
    assert ex.expected_response.status == 400
    assert ex.kafka_assertion is None
    assert ex.elastic_assertion is not None
    assert ex.elastic_assertion.index == "esb-logs-*"
    assert "Replaced" in (ex.compiler_notes or "")


@pytest.mark.asyncio
async def test_build_executable_with_partial_llm_data(collection, env_vars):
    """אם LLM החזיר חלקי — fallback ל-template values."""
    compiler = SmartCompiler(spec_md=None, collection=collection, env_vars=env_vars)
    template = collection.find_by_name("POST /patients/admit")
    rendered = compiler._render_template(template)

    llm_data = {
        # רק expected_response, חסר request → ייפול ל-template
        "expected_response": {"status": 200},
    }
    ex = compiler._build_executable(
        test_case_id="TC1",
        ado_id=1,
        text="happy path",
        data=llm_data,
        fallback_request=rendered,
    )
    assert ex.request.method == "POST"
    assert "patients/admit" in ex.request.url
    assert ex.expected_response.status == 200
