"""Unit tests ל-ValidatorAgent."""

from __future__ import annotations

import pytest

from agents.validator.validator_agent import ValidatorAgent
from models.test_case import (
    ElasticAssertion,
    KafkaAssertion,
    ResponseAssertion,
    TestCase,
    TestCaseResult,
    TestStatus,
)


def _result(status_code=200, body=None, kafka=None, elastic=None) -> TestCaseResult:
    return TestCaseResult(
        test_case_id="TC1",
        status=TestStatus.PASSED if status_code < 400 else TestStatus.FAILED,
        api_response={"status": status_code, "body": body or {"ok": True}, "headers": {}, "duration_ms": 10, "url": "http://x", "method": "GET"},
        kafka_result=kafka,
        elastic_result=elastic,
    )


@pytest.mark.asyncio
async def test_response_status_match():
    v = ValidatorAgent()
    tc = TestCase(test_case_id="TC1", expected_response=ResponseAssertion(status=200))
    res = _result(status_code=200)
    val = v.validate_one(tc, res)
    assert val.overall_status == TestStatus.PASSED


@pytest.mark.asyncio
async def test_response_status_mismatch():
    v = ValidatorAgent()
    tc = TestCase(test_case_id="TC1", expected_response=ResponseAssertion(status=200))
    res = _result(status_code=500)
    val = v.validate_one(tc, res)
    assert val.overall_status == TestStatus.FAILED
    assert any("Status code" in r for r in val.failure_reasons)


def test_jsonpath_value_match():
    v = ValidatorAgent()
    tc = TestCase(
        test_case_id="TC1",
        expected_response=ResponseAssertion(
            status=200,
            schema_assertions={"$.patientId": "999"},
        ),
    )
    res = _result(status_code=200, body={"patientId": "999"})
    val = v.validate_one(tc, res)
    assert val.overall_status == TestStatus.PASSED


def test_jsonpath_value_mismatch():
    v = ValidatorAgent()
    tc = TestCase(
        test_case_id="TC1",
        expected_response=ResponseAssertion(
            status=200,
            schema_assertions={"$.patientId": "999"},
        ),
    )
    res = _result(status_code=200, body={"patientId": "111"})
    val = v.validate_one(tc, res)
    assert val.overall_status == TestStatus.FAILED


def test_jsonpath_type_match():
    v = ValidatorAgent()
    tc = TestCase(
        test_case_id="TC1",
        expected_response=ResponseAssertion(
            status=200,
            schema_assertions={"$.count": "number"},
        ),
    )
    res = _result(status_code=200, body={"count": 42})
    val = v.validate_one(tc, res)
    assert val.overall_status == TestStatus.PASSED


def test_kafka_not_found():
    v = ValidatorAgent()
    tc = TestCase(
        test_case_id="TC1",
        expected_response=ResponseAssertion(status=200),
        kafka_assertion=KafkaAssertion(topic="patient-events", search_term="999"),
    )
    res = _result(status_code=200, kafka={"found": False})
    val = v.validate_one(tc, res)
    assert val.overall_status == TestStatus.FAILED
    assert any("Kafka" in r or "מסר" in r for r in val.failure_reasons)


def test_elastic_with_errors():
    v = ValidatorAgent()
    tc = TestCase(
        test_case_id="TC1",
        expected_response=ResponseAssertion(status=200),
        elastic_assertion=ElasticAssertion(index="esb-logs-*", query="x"),
    )
    res = _result(status_code=200, elastic={"hits": 5, "errors": 1})
    val = v.validate_one(tc, res)
    assert val.overall_status == TestStatus.FAILED
    assert any("ERROR" in r for r in val.failure_reasons)
