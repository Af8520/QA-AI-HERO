"""פרסור test case טקסטואלי מ-ADO לטיפוס TestCase מלא עם assertions.

הסוכן ב-Copilot Studio כותב tests בעברית בטקסט חופשי.
ה-parser הזה משתמש ב-Azure OpenAI כדי לחלץ:
- postman_request_name
- input_overrides
- expected_response (status + JSONPath assertions)
- kafka_assertion
- elastic_assertion
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from config.logging_config import get_logger
from config.settings import settings
from models.postman import PostmanCollection
from models.test_case import (
    ElasticAssertion,
    KafkaAssertion,
    ResponseAssertion,
    TestCase,
    TestStep,
)

log = get_logger(__name__)

PARSER_SYSTEM_PROMPT = """אתה QA structured-extraction agent.
בהינתן טקסט test case בעברית מ-ADO, חלץ אותו למבנה JSON מובנה.

החזר JSON בלבד בפורמט:
{
  "postman_request_name": "string או null",
  "input_overrides": {"key": "value"},
  "expected_response": {"status": 200, "schema_assertions": {"$.field": "expected"}},
  "kafka_assertion": {"topic": "...", "search_term": "...", "expected_value": "..."} או null,
  "elastic_assertion": {"index": "...", "query": "...", "must_not_contain_level": "ERROR"} או null,
  "steps": [{"step": "...", "expected_result": "..."}]
}

כללים:
- אם ה-test case לא דורש Kafka — kafka_assertion: null.
- אם לא דורש Elastic — elastic_assertion: null.
- postman_request_name חייב להיות מתוך הרשימה שתינתן.
- שדות שאינך בטוח בהם — null או {}.
"""


async def parse_test_case_from_text(
    raw_text: str,
    collection: Optional[PostmanCollection],
    ado_test_case_id: Optional[int] = None,
    test_case_id: Optional[str] = None,
) -> TestCase:
    """ממיר טקסט test case חופשי ל-TestCase מובנה."""
    candidates = collection.request_names() if collection else []
    fallback = TestCase(
        test_case_id=test_case_id or f"TC-{ado_test_case_id or 0}",
        ado_test_case_id=ado_test_case_id,
        steps=[TestStep(step=raw_text[:200], expected_result="(לא זוהה)")],
    )

    if not settings.azure_openai_enabled:
        log.warning("parser_no_azure_openai", tc=test_case_id)
        return fallback

    try:
        from openai import AsyncAzureOpenAI  # type: ignore[import-not-found]
    except ImportError:
        log.warning("parser_openai_sdk_missing")
        return fallback

    client = AsyncAzureOpenAI(
        api_key=settings.AZURE_OPENAI_KEY,
        api_version=settings.AZURE_OPENAI_API_VERSION,
        azure_endpoint=settings.AZURE_OPENAI_ENDPOINT,
    )

    user_payload = {
        "test_case_id": test_case_id,
        "ado_test_case_id": ado_test_case_id,
        "test_case_text": raw_text,
        "available_postman_requests": candidates,
    }

    try:
        resp = await client.chat.completions.create(
            model=settings.AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": PARSER_SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        content = resp.choices[0].message.content or "{}"
        data = json.loads(content)
    except Exception as e:
        log.warning("parser_llm_failed", error=str(e), tc=test_case_id)
        return fallback

    return _build_test_case(
        data=data,
        test_case_id=test_case_id or f"TC-{ado_test_case_id or 0}",
        ado_test_case_id=ado_test_case_id,
        candidates=candidates,
    )


def _build_test_case(
    data: Dict[str, Any],
    test_case_id: str,
    ado_test_case_id: Optional[int],
    candidates: List[str],
) -> TestCase:
    pm_name = data.get("postman_request_name")
    if pm_name and candidates and pm_name not in candidates:
        # ניסיון התאמה case-insensitive
        match = next((c for c in candidates if c.lower() == str(pm_name).lower()), None)
        pm_name = match  # מותר None

    expected_response = None
    er = data.get("expected_response") or {}
    if er:
        expected_response = ResponseAssertion(
            status=int(er.get("status") or 200),
            schema_assertions=er.get("schema_assertions") or {},
        )

    kafka = None
    ka = data.get("kafka_assertion")
    if ka and isinstance(ka, dict) and ka.get("topic"):
        kafka = KafkaAssertion(
            topic=ka["topic"],
            search_term=ka.get("search_term") or "",
            expected_value=ka.get("expected_value"),
        )

    elastic = None
    ea = data.get("elastic_assertion")
    if ea and isinstance(ea, dict) and ea.get("index"):
        elastic = ElasticAssertion(
            index=ea["index"],
            query=ea.get("query") or "",
            must_not_contain_level=ea.get("must_not_contain_level") or "ERROR",
        )

    raw_steps = data.get("steps") or []
    steps: List[TestStep] = []
    for s in raw_steps:
        if not isinstance(s, dict):
            continue
        steps.append(
            TestStep(
                step=str(s.get("step") or ""),
                expected_result=str(s.get("expected_result") or ""),
            )
        )

    return TestCase(
        test_case_id=test_case_id,
        ado_test_case_id=ado_test_case_id,
        postman_request_name=pm_name,
        input_overrides=data.get("input_overrides") or {},
        expected_response=expected_response,
        kafka_assertion=kafka,
        elastic_assertion=elastic,
        steps=steps,
    )
