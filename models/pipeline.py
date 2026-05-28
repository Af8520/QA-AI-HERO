from typing import List, Optional

from pydantic import BaseModel, Field

from models.test_case import TestCase


class CopilotResult(BaseModel):
    test_cases: List[TestCase] = Field(default_factory=list)
    suite_id: int = 0
    ado_url: Optional[str] = None


class CompletionInfo(BaseModel):
    suite_id: int
    ado_url: Optional[str] = None
    raw_message: str


class PipelineResult(BaseModel):
    suite_id: int
    us_number: Optional[str] = None
    total: int = 0
    passed: int = 0
    failed: int = 0
    blocked: int = 0
    bugs_opened: List[int] = Field(default_factory=list)
    summary_hebrew: str = ""
