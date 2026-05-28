"""Unit tests ל-Postman loader/executor."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agents.postman.postman_executor import render
from agents.postman.postman_loader import (
    load_collection_from_file,
    load_environment_from_file,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_load_collection_basic():
    coll = load_collection_from_file(str(FIXTURES / "sample_collection.json"))
    assert coll.name == "ESB Sample Collection"
    assert len(coll.requests) == 2
    names = coll.request_names()
    assert "POST /patients/admit" in names
    assert "GET /patients/{id}" in names


def test_load_environment():
    env = load_environment_from_file(str(FIXTURES / "sample_environment.json"))
    assert env.values["baseUrl"] == "https://api-test.local/esb"
    assert env.values["patientId"] == "123456"


def test_find_by_name_case_insensitive():
    coll = load_collection_from_file(str(FIXTURES / "sample_collection.json"))
    req = coll.find_by_name("post /patients/admit")
    assert req is not None
    assert req.method == "POST"


def test_render_substitutes_variables():
    template = "{{baseUrl}}/patients/{{patientId}}"
    rendered = render(template, {"baseUrl": "https://x", "patientId": "999"})
    assert rendered == "https://x/patients/999"


def test_render_handles_missing_var():
    rendered = render("{{baseUrl}}/missing/{{nope}}", {"baseUrl": "https://x"})
    assert rendered == "https://x/missing/"


def test_request_url_parsed_from_object():
    coll = load_collection_from_file(str(FIXTURES / "sample_collection.json"))
    req = coll.find_by_name("POST /patients/admit")
    assert req.url_raw == "{{baseUrl}}/patients/admit"


def test_body_raw_template():
    coll = load_collection_from_file(str(FIXTURES / "sample_collection.json"))
    req = coll.find_by_name("POST /patients/admit")
    assert req.body and req.body.mode == "raw"
    payload = json.loads(render(req.body.raw, {"patientId": "999"}))
    assert payload["patientId"] == "999"
    assert payload["event_type"] == "ADMISSION"
