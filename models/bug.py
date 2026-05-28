from typing import List, Optional

from pydantic import BaseModel, Field


class BugReport(BaseModel):
    title: str
    description: str
    severity: str = "Medium"
    test_case_id: str
    ado_test_case_id: Optional[int] = None
    failure_reasons: List[str] = Field(default_factory=list)
    repro_steps: List[str] = Field(default_factory=list)
    suggested_fix: Optional[str] = None
    ado_bug_id: Optional[int] = None
