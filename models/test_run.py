from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from models.test_case import TestCaseResult, TestStatus


class ValidationResult(BaseModel):
    test_case_id: str
    ado_test_case_id: Optional[int] = None
    overall_status: TestStatus
    response_check: Optional[Dict[str, Any]] = None
    kafka_check: Optional[Dict[str, Any]] = None
    elastic_check: Optional[Dict[str, Any]] = None
    failure_reasons: List[str] = Field(default_factory=list)


class TestRun(BaseModel):
    suite_id: int
    results: List[TestCaseResult] = Field(default_factory=list)
    validations: List[ValidationResult] = Field(default_factory=list)


class RunResult(BaseModel):
    suite_id: int
    total: int = 0
    passed: int = 0
    failed: int = 0
    blocked: int = 0
    duration_seconds: float = 0.0
