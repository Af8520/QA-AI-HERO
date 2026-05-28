from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TestStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    BLOCKED = "blocked"
    NOT_RUN = "not_run"


class TestStep(BaseModel):
    step: str
    expected_result: str


class KafkaAssertion(BaseModel):
    topic: str
    search_term: str
    expected_value: Optional[str] = None


class ElasticAssertion(BaseModel):
    index: str
    query: str
    must_not_contain_level: Optional[str] = "ERROR"


class ResponseAssertion(BaseModel):
    status: int = 200
    schema_assertions: Dict[str, Any] = Field(default_factory=dict)


class TestCase(BaseModel):
    test_case_id: str
    ado_test_case_id: Optional[int] = None
    postman_request_name: Optional[str] = None
    input_overrides: Dict[str, Any] = Field(default_factory=dict)
    expected_response: Optional[ResponseAssertion] = None
    kafka_assertion: Optional[KafkaAssertion] = None
    elastic_assertion: Optional[ElasticAssertion] = None
    steps: List[TestStep] = Field(default_factory=list)


class StepResult(BaseModel):
    step: str
    expected_result: str
    actual_result: str
    status: TestStatus
    error_message: Optional[str] = None
    response_dump: Optional[Dict[str, Any]] = None


class TestCaseResult(BaseModel):
    test_case_id: str
    ado_test_case_id: Optional[int] = None
    status: TestStatus
    step_results: List[StepResult] = Field(default_factory=list)
    duration_seconds: float = 0.0
    api_response: Optional[Dict[str, Any]] = None
    kafka_result: Optional[Dict[str, Any]] = None
    elastic_result: Optional[Dict[str, Any]] = None
