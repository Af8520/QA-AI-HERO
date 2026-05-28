"""SmartCompiler — ליבת Phase B.

ממיר תסריט בעברית מ-ADO ל-`ExecutableTestCase` ready-to-execute, ב-LLM call יחיד.

קלט:
- raw_ado_test_case: dict עם {id, title, text} מ-ADO
- spec_md: מסמך אפיון structured (אופציונלי, ה-attachment של ה-suite)
- collection: PostmanCollection עם כל ה-templates

פלט: ExecutableTestCase עם request מלא + assertions.

Fallback: אם LLM נכשל / JSON לא תקני / Azure OpenAI לא זמין —
משתמש ב-test_case_parser.py הישן + Postman executor כדי לבנות request בסיסי.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib.parse import quote, urlsplit, urlunsplit

from agents.postman.llm_request_matcher import match_request_name
from agents.postman.postman_executor import _build_body, _build_headers, render
from config.logging_config import get_logger
from config.settings import settings
from models.executable_test_case import ExecutableTestCase, HttpRequestSpec
from models.postman import PostmanCollection, PostmanRequest
from models.test_case import (
    ElasticAssertion,
    KafkaAssertion,
    ResponseAssertion,
)

log = get_logger(__name__)


def _coerce_to_string(value: Any) -> Optional[str]:
    """ממיר ערך ל-string. אם זה dict/list — serialize ל-JSON.

    LLMs לפעמים מחזירים שדות 'string' כאובייקטים מורכבים (כמו Elastic query כ-dict).
    במקום להיכשל ב-Pydantic validation — נסדרל לJSON כדי שהאסרשן יהיה לפחות readable.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    return str(value)


def _normalize_url(url: Optional[str]) -> Optional[str]:
    """מבטיח URL-encoding ל-path ו-query (חשוב לתווים שאינם ASCII כמו עברית).

    דוגמה: 'http://api/x?action_code=ש' → 'http://api/x?action_code=%D7%A9'
    httpx בדרך כלל עושה את זה אוטומטית, אבל לפעמים יכשל על URLs שהורכבו ידנית
    עם תווים מיוחדים. כדי להיות בטוחים — נעשה normalize כאן.
    """
    if not url or not isinstance(url, str):
        return url
    try:
        parts = urlsplit(url)
        # safe= משאיר תווים מבניים (/, =, &) כפי שהם
        new_path = quote(parts.path, safe="/-._~%!$&'()*+,;=:@")
        new_query = quote(parts.query, safe="=&-._~%!$'()*+,;:@/")
        return urlunsplit((parts.scheme, parts.netloc, new_path, new_query, parts.fragment))
    except Exception as e:
        log.warning("url_normalize_failed", url=url, error=str(e))
        return url


SYSTEM_PROMPT = """אתה QA Test Compiler עבור מחלקת ESB במכבי.
תפקידך: לקבל תיאור תסריט בעברית + מסמך אפיון + Postman request template,
ולהחזיר request HTTP מלא execute-ready.

קלט:
1. SPEC_MD — מסמך אפיון מובנה (endpoints, fields, validation rules, Kafka, logging)
2. TEST_CASE — תיאור תסריט בעברית מ-ADO (מה לבדוק)
3. POSTMAN_TEMPLATE — JSON של ה-request ה"happy path" המקורי

פלט: JSON בלבד בפורמט:
{
  "test_case_id": "string",
  "request": {
    "method": "GET|POST|PUT|DELETE|...",
    "url": "https://full-url-with-resolved-vars",
    "headers": {"key": "value"},
    "body": {...} | "string" | null
  },
  "expected_response": {
    "status": 200,
    "schema_assertions": {"$.field": "expected_value_or_type"}
  },
  "kafka_assertion": {"topic": "...", "search_term": "...", "expected_value": "..."} | null,
  "elastic_assertion": {"index": "...", "query": "...", "must_not_contain_level": "ERROR"} | null,
  "compiler_notes": "string קצר שמסביר מה שונה מהtemplate"
}

כללים:
- קח את POSTMAN_TEMPLATE כבסיס. שמור על הכל אלא אם התסריט דורש שינוי ספציפי.
- "ערך לא תקין מסוג X" → ספק ערך שאינו תואם את הסכימה (string במקום int, וכד').
- "השמט שדה X" → הסר את השדה מה-body.
- "חורג מהטווח" → ספק ערך מקסימום+1 או מינימום-1 לפי SPEC.
- "תרחיש שלילי כללי" → ספק שדה ריק / null במקום ערך תקין.
- ה-`expected_response.status` חייב להיות מתאים לסוג הכשל הצפוי לפי SPEC.
- אם ה-SPEC מגדיר Kafka/log חובה — הוסף את ה-assertion המתאים. אחרת null.
- החזר JSON תקני בלבד, ללא טקסט נלווה.
"""


# מצב LLM-only: אין Postman template — ה-LLM חייב לחלץ URL+method+body מטקסט התסריט/SPEC
SYSTEM_PROMPT_NO_TEMPLATE = """אתה QA Test Compiler עבור מחלקת ESB במכבי.
תפקידך: לחלץ request HTTP מלא **רק** מטקסט התסריט (ואם קיים — גם ממסמך האפיון).
אין לך Postman template — אתה בונה את ה-request מאפס.

קלט:
1. SPEC_MD — מסמך אפיון מובנה (יכול להיות ריק)
2. TEST_CASE — תיאור תסריט בעברית מ-ADO. **הסוכן שכתב אותו אמור היה לכלול ב-steps את ה-URL והפרטים**

החזר JSON בלבד בפורמט:
{
  "test_case_id": "string",
  "request": {
    "method": "GET|POST|PUT|DELETE|PATCH",
    "url": "https://full-url",
    "headers": {"key": "value"},
    "body": {...} | "string" | null
  },
  "expected_response": {
    "status": 200,
    "schema_assertions": {"$.field": "expected_value_or_type"}
  },
  "kafka_assertion": null,
  "elastic_assertion": null,
  "compiler_notes": "string קצר שמסביר מאיפה חולץ ה-request"
}

מבנה ה-test case — חשוב להבין:
התסריט מכיל בדרך כלל **כמה steps**, מסוגים שונים:
- **"שלח X ל-URL"** (send) → קריאת HTTP אמיתית
- **"וודא ש..." / "בדוק ש..." / "verify"** → לא קריאה חדשה — אסרשן על התשובה הקודמת או בדיקת Kafka/Elastic

כללי תרגום:
1. **בחר request יחיד** — קח את ה-step ה**ראשון** מסוג "send" (שלח/POST/GET/PATCH/...).
   - אם יש כמה steps של "send" באותו test case (למשל PATCH ואז GET לאימות) — קח את הראשון, ורשום ב-compiler_notes:
     "Multi-call test: only first call executed. Second call: [...]"
2. **steps של "וודא"** — אל תיצור request חדש! במקום זה:
   - "וודא שתשובה מכילה שדה X = Y" / "וודא שכל שדות boolean..." → הוסף ל-`expected_response.schema_assertions`:
     לדוגמה: `{"$.is_shabatical": {"type": "boolean"}}` או `{"$.patientId": {"value": "123"}}`
   - "וודא שלוג X נכתב ב-Elastic" / "וודא לוג Start ולוג End" → `elastic_assertion`:
     `{"index": "esb-logs-*", "query": "<context based on spec>", "must_not_contain_level": "ERROR"}`
   - "וודא שמסר נכתב ל-Kafka topic Y" → `kafka_assertion`:
     `{"topic": "<topic>", "search_term": "<id>", "expected_value": "<value or null>"}`
   - "וודא ש-Header X = Y בתשובה" → אין JSONPath ל-headers; רשום ב-compiler_notes "Header assertion: X=Y" (יוצג בלוג; runner עתידי יוסיף תמיכה)

3. **expected_status** — קח מה-step הראשון. "סטטוס 200" → 200, "סטטוס 400" → 400.

4. **URL** — חייב להתחיל ב-`http://` או `https://`. אם הסוכן רשם תווים שאינם ASCII (עברית, סוגריים מיוחדים) — השאר כפי שהם, ה-runner יקודד אוטומטית.

5. **body** — בדרך כלל JSON שמופיע ב-step אחרי "עם body:". פרסר אותו כ-object. אם הוא לא valid JSON, השאר כ-string.

6. **headers** — אם יש "MAC-UserID: X" או "Content-Type: ..." ב-step → הוסף ל-headers. אם POST/PATCH עם JSON body וב-spec לא מצוין אחרת → הוסף Content-Type: application/json.

7. אם **לא ניתן** לחלץ URL מהטקסט — החזר request.url=null + compiler_notes "Agent did not provide URL in steps — update agent instructions".

8. החזר JSON תקני בלבד, ללא טקסט נלווה.
"""


class SmartCompiler:
    def __init__(
        self,
        spec_md: Optional[str],
        collection: Optional[PostmanCollection],
        env_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        self.spec_md = spec_md or ""
        self.collection = collection
        self.env_vars = env_vars or {}

    async def compile(self, raw_ado_test_case: Dict[str, Any]) -> ExecutableTestCase:
        """ממיר test case יחיד מ-ADO ל-ExecutableTestCase.

        סדר הניסיונות:
        1. אם יש Postman template + LLM → LLM mutates the template
        2. אם יש Postman template אבל אין LLM → render template עם env vars
        3. אם אין template אבל יש LLM → LLM-only mode (חולץ URL מטקסט התסריט)
        4. ברירת מחדל: BLOCKED placeholder
        """
        ado_id = raw_ado_test_case.get("id")
        title = raw_ado_test_case.get("title") or f"TC-{ado_id}"
        text = raw_ado_test_case.get("text") or title

        # 1) בחר Postman template
        template = await self._pick_template(test_case_id=title, description=text)

        if template is not None:
            # יש template — מנסה LLM mutation, fallback ל-render בלבד
            if settings.azure_openai_enabled:
                executable = await self._compile_via_llm(
                    test_case_id=title, ado_id=ado_id, text=text, template=template,
                )
                if executable is not None:
                    return executable
                log.warning("compiler_llm_failed_falling_back_to_template_render", tc=title)
            return self._compile_from_template(title, ado_id, text, template, notes="fallback (no LLM)")

        # 2) אין template — אם יש LLM, מנסה LLM-only mode
        log.warning("compiler_no_template", tc=title)
        if settings.azure_openai_enabled:
            executable = await self._compile_llm_only(test_case_id=title, ado_id=ado_id, text=text)
            if executable is not None:
                return executable
            log.warning("compiler_llm_only_failed", tc=title)

        # 3) אין שום דבר — BLOCKED
        return self._fallback_no_template(title, ado_id, text)

    async def _pick_template(self, test_case_id: str, description: str) -> Optional[PostmanRequest]:
        if not self.collection or not self.collection.requests:
            return None
        name = await match_request_name(
            test_case_id=test_case_id,
            test_case_description=description,
            collection=self.collection,
        )
        if not name:
            return None
        return self.collection.find_by_name(name)

    async def _compile_via_llm(
        self,
        test_case_id: str,
        ado_id: Optional[int],
        text: str,
        template: PostmanRequest,
    ) -> Optional[ExecutableTestCase]:
        try:
            from openai import AsyncAzureOpenAI  # type: ignore[import-not-found]
        except ImportError:
            log.warning("compiler_openai_sdk_missing")
            return None

        client = AsyncAzureOpenAI(
            api_key=settings.AZURE_OPENAI_KEY,
            api_version=settings.AZURE_OPENAI_API_VERSION,
            azure_endpoint=settings.AZURE_OPENAI_ENDPOINT,
        )

        # rendered template — אנחנו מסיבים {{vars}} כבר עכשיו, ה-LLM יראה ערכים אמיתיים
        rendered_template = self._render_template(template)

        user_payload = {
            "TEST_CASE": {"id": test_case_id, "ado_id": ado_id, "text": text},
            "SPEC_MD": self.spec_md or "(אין MD זמין — הסתמך רק על TEST_CASE + POSTMAN_TEMPLATE)",
            "POSTMAN_TEMPLATE": rendered_template,
        }

        try:
            resp = await client.chat.completions.create(
                model=settings.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, default=str)},
                ],
                response_format={"type": "json_object"},
                temperature=0,
            )
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
        except Exception as e:
            log.warning("compiler_llm_call_failed", error=str(e), tc=test_case_id)
            return None

        return self._build_executable(
            test_case_id=test_case_id,
            ado_id=ado_id,
            text=text,
            data=data,
            fallback_request=rendered_template,
        )

    async def _compile_llm_only(
        self,
        test_case_id: str,
        ado_id: Optional[int],
        text: str,
    ) -> Optional[ExecutableTestCase]:
        """LLM-only: בונה request מאפס מטקסט התסריט (ללא Postman template).

        מתאים כש-Postman לא הועלה והסוכן כלל URL+method במפורש ב-steps.
        """
        try:
            from openai import AsyncAzureOpenAI  # type: ignore[import-not-found]
        except ImportError:
            log.warning("compiler_llm_only_sdk_missing")
            return None

        client = AsyncAzureOpenAI(
            api_key=settings.AZURE_OPENAI_KEY,
            api_version=settings.AZURE_OPENAI_API_VERSION,
            azure_endpoint=settings.AZURE_OPENAI_ENDPOINT,
        )

        user_payload = {
            "TEST_CASE": {"id": test_case_id, "ado_id": ado_id, "text": text},
            "SPEC_MD": self.spec_md or "(אין MD זמין — חלץ הכל מטקסט ה-TEST_CASE)",
        }

        try:
            resp = await client.chat.completions.create(
                model=settings.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_NO_TEMPLATE},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, default=str)},
                ],
                response_format={"type": "json_object"},
                temperature=0,
            )
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
        except Exception as e:
            log.warning("compiler_llm_only_call_failed", error=str(e), tc=test_case_id)
            return None

        request_data = data.get("request") or {}
        url = request_data.get("url")
        if not url:
            # ה-LLM לא הצליח לחלץ URL — חוזר ל-BLOCKED עם הסבר ברור
            log.warning("compiler_llm_only_no_url", tc=test_case_id)
            return ExecutableTestCase(
                test_case_id=test_case_id,
                ado_test_case_id=ado_id,
                request=HttpRequestSpec(method="GET", url="about:blank"),
                expected_response=ResponseAssertion(status=0),
                source_text=text,
                compiler_notes=data.get("compiler_notes") or "ה-LLM לא הצליח לחלץ URL מטקסט התסריט — וודא שהסוכן כתב URL מלא בכל step",
            )

        # יש URL — בונה Executable
        empty_template = {"method": "GET", "url": url, "headers": {}, "body": None}
        return self._build_executable(
            test_case_id=test_case_id, ado_id=ado_id, text=text, data=data, fallback_request=empty_template,
        )

    def _render_template(self, template: PostmanRequest) -> Dict[str, Any]:
        """ממיר PostmanRequest ל-dict שטוח עם ערכים rendered מ-env vars."""
        url = render(template.url_raw, self.env_vars)
        headers = _build_headers(template, self.env_vars)
        body = _build_body(template, self.env_vars)
        return {
            "name": template.name,
            "method": template.method,
            "url": url,
            "headers": headers,
            "body": body,
        }

    def _build_executable(
        self,
        test_case_id: str,
        ado_id: Optional[int],
        text: str,
        data: Dict[str, Any],
        fallback_request: Dict[str, Any],
    ) -> ExecutableTestCase:
        request_data = data.get("request") or {}
        method = (request_data.get("method") or fallback_request["method"]).upper()
        url = _normalize_url(request_data.get("url") or fallback_request["url"])
        headers = request_data.get("headers") or fallback_request["headers"] or {}
        body = request_data.get("body", fallback_request.get("body"))

        # expected_response
        er_data = data.get("expected_response") or {}
        expected_response = ResponseAssertion(
            status=int(er_data.get("status") or 200),
            schema_assertions=er_data.get("schema_assertions") or {},
        )

        # kafka assertion (optional) — coerce כל שדה ל-string במקרה ש-LLM החזיר dict
        kafka_assertion = None
        ka = data.get("kafka_assertion")
        if ka and isinstance(ka, dict) and ka.get("topic"):
            kafka_assertion = KafkaAssertion(
                topic=_coerce_to_string(ka.get("topic")),
                search_term=_coerce_to_string(ka.get("search_term") or ""),
                expected_value=_coerce_to_string(ka.get("expected_value")) if ka.get("expected_value") is not None else None,
            )

        # elastic assertion (optional) — coerce query (LLM נוטה להחזיר אותו כ-dict)
        elastic_assertion = None
        ea = data.get("elastic_assertion")
        if ea and isinstance(ea, dict) and ea.get("index"):
            elastic_assertion = ElasticAssertion(
                index=_coerce_to_string(ea["index"]),
                query=_coerce_to_string(ea.get("query") or ""),
                must_not_contain_level=_coerce_to_string(ea.get("must_not_contain_level")) or "ERROR",
            )

        return ExecutableTestCase(
            test_case_id=test_case_id,
            ado_test_case_id=ado_id,
            request=HttpRequestSpec(method=method, url=url, headers=headers, body=body),
            expected_response=expected_response,
            kafka_assertion=kafka_assertion,
            elastic_assertion=elastic_assertion,
            source_text=text,
            compiler_notes=data.get("compiler_notes"),
        )

    def _compile_from_template(
        self,
        test_case_id: str,
        ado_id: Optional[int],
        text: str,
        template: PostmanRequest,
        notes: str,
    ) -> ExecutableTestCase:
        rendered = self._render_template(template)
        return ExecutableTestCase(
            test_case_id=test_case_id,
            ado_test_case_id=ado_id,
            request=HttpRequestSpec(
                method=rendered["method"],
                url=rendered["url"],
                headers=rendered["headers"],
                body=rendered["body"],
            ),
            expected_response=ResponseAssertion(status=200),
            source_text=text,
            compiler_notes=notes,
        )

    def _fallback_no_template(
        self, test_case_id: str, ado_id: Optional[int], text: str
    ) -> ExecutableTestCase:
        # אין דרך לבנות request אמיתי — נחזיר placeholder שיסומן BLOCKED ב-runner
        return ExecutableTestCase(
            test_case_id=test_case_id,
            ado_test_case_id=ado_id,
            request=HttpRequestSpec(method="GET", url="about:blank"),
            expected_response=ResponseAssertion(status=0),
            source_text=text,
            compiler_notes="לא נמצא Postman request מתאים — לא ניתן להריץ",
        )
