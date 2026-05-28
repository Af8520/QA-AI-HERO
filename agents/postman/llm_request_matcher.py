"""התאמת test case name -> Postman request name דרך Azure OpenAI.

אם השם בדיוק תואם — לא קורא ל-LLM (חיסכון).
אחרת — שולח רשימת השמות + תיאור ה-tc, מבקש את השם המדויק.
"""

from __future__ import annotations

import json
from typing import List, Optional

from config.logging_config import get_logger
from config.settings import settings
from models.postman import PostmanCollection

log = get_logger(__name__)


async def match_request_name(
    test_case_id: str,
    test_case_description: str,
    collection: PostmanCollection,
    suggested_name: Optional[str] = None,
) -> Optional[str]:
    """מחזיר שם request מדויק מתוך ה-collection, או None אם לא נמצאה התאמה."""
    if not collection.requests:
        return None

    # 1. exact match על השם המוצע
    if suggested_name:
        exact = collection.find_by_name(suggested_name)
        if exact:
            return exact.name

    # 2. exact match על test_case_id
    if test_case_id:
        exact = collection.find_by_name(test_case_id)
        if exact:
            return exact.name

    # 3. heuristic: קונטיינס ב-description
    candidates = collection.request_names()
    desc_lower = (test_case_description or "").lower()
    for name in candidates:
        if name.lower() in desc_lower:
            return name

    # 4. fallback: LLM (אם זמין)
    if not settings.azure_openai_enabled:
        log.warning("llm_matcher_no_azure_openai", tc=test_case_id)
        return candidates[0] if candidates else None

    return await _llm_pick(test_case_id, test_case_description, candidates)


async def _llm_pick(tc_id: str, description: str, candidates: List[str]) -> Optional[str]:
    try:
        from openai import AsyncAzureOpenAI  # type: ignore[import-not-found]
    except ImportError:
        log.warning("openai_sdk_missing")
        return candidates[0] if candidates else None

    import httpx
    http_client = httpx.AsyncClient(verify=settings.VERIFY_SSL, timeout=60.0)
    client = AsyncAzureOpenAI(
        api_key=settings.AZURE_OPENAI_KEY,
        api_version=settings.AZURE_OPENAI_API_VERSION,
        azure_endpoint=settings.AZURE_OPENAI_ENDPOINT,
        http_client=http_client,
    )
    system = (
        "אתה QA assistant. בחר את שם ה-Postman request שהכי מתאים לתסריט הבדיקה. "
        "החזר JSON בלבד: {\"name\": \"...\"} או {\"name\": null} אם אין התאמה."
    )
    user = json.dumps(
        {"test_case_id": tc_id, "description": description, "candidates": candidates},
        ensure_ascii=False,
    )
    try:
        resp = await client.chat.completions.create(
            model=settings.AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            temperature=0,
        )
        content = resp.choices[0].message.content or "{}"
        data = json.loads(content)
        name = data.get("name")
        if name and name in candidates:
            return name
    except Exception as e:
        log.warning("llm_match_failed", error=str(e))
    return candidates[0] if candidates else None
