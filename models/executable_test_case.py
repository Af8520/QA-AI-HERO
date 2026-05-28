"""ExecutableTestCase — הפלט של Smart Compiler, קלט ל-Runner.

מכיל request HTTP מלא ready-to-execute + assertions שייבדקו אחרי הריצה.
ה-Runner לא עושה mutations נוספות — רק שולח את ה-request כמות שהוא.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from models.test_case import ElasticAssertion, KafkaAssertion, ResponseAssertion


class HttpRequestSpec(BaseModel):
    """ה-request המלא שיישלח ב-httpx — כבר מ-rendered, ללא {{vars}} שוב."""
    method: str = "GET"
    url: str
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Optional[Any] = None  # dict | str | None


class ExecutableTestCase(BaseModel):
    test_case_id: str
    ado_test_case_id: Optional[int] = None
    request: HttpRequestSpec
    expected_response: ResponseAssertion = Field(default_factory=ResponseAssertion)
    kafka_assertion: Optional[KafkaAssertion] = None
    elastic_assertion: Optional[ElasticAssertion] = None
    # מטא-דאטה לדיבאג / לוגים
    source_text: Optional[str] = None
    compiler_notes: Optional[str] = None
