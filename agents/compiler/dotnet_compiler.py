"""DotNetCompiler — מהדר תסריט .NET בעברית לרצף actions של Kafka/Couchbase.

מבנה זהה ל-SmartCompiler:
- Regex-first עבור 3 patterns ברורים (publish/wait kafka/wait couchbase)
- LLM-only fallback עם system prompt ייעודי כש-regex לא מספיק

הסוכן ב-Copilot Studio של .NET מצופה לכתוב כל step כ-action מפורש:
  "פרסם ל-topic X את {...}"
  "ודא שמסר הגיע ל-topic Y עם field=value"
  "ודא שמסמך נכתב ל-Couchbase bucket Z עם key=K"
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional

from agents.compiler.smart_compiler import _make_openai_client
from config.logging_config import get_logger
from config.settings import settings
from models.dotnet_test_case import (
    CouchbaseWaitAction,
    DotNetAction,
    DotNetExecutableTestCase,
    KafkaPublishAction,
    KafkaWaitAction,
)

log = get_logger(__name__)


# ============================================================
# Regex patterns
# ============================================================

# "פרסם ל-topic X את {JSON}" / "publish to topic X with {JSON}"
_PUBLISH_PATTERN = re.compile(
    r"(?:פרסם|publish|שלח)\s+(?:ל[-\s]?)?topic\s+([A-Za-z0-9_.\-]+)"
    r".*?(?:את|with|with payload|payload[:\s])\s*(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})",
    re.IGNORECASE | re.DOTALL,
)

# "ודא שמסר הגיע ל-topic Y" / "verify message arrived at topic Y"
_KAFKA_WAIT_PATTERN = re.compile(
    r"(?:ודא|וודא|verify|assert|המתן|wait)\s+(?:ש?מסר|message)\s+(?:הגיע|arrived|מגיע)\s+"
    r"(?:ל[-\s]?)?topic\s+([A-Za-z0-9_.\-]+)",
    re.IGNORECASE,
)

# "ודא שמסמך נכתב ל-Couchbase bucket Z [עם key=K]"
_COUCHBASE_WAIT_PATTERN = re.compile(
    r"(?:ודא|וודא|verify|assert)\s+(?:ש?מסמך|document)\s+"
    r"(?:נכתב|written|exists)\s+(?:ל[-\s]?)?(?:Couchbase\s+)?bucket\s+([A-Za-z0-9_.\-]+)"
    r"(?:.*?key\s*[=:]\s*([A-Za-z0-9_.\-]+))?",
    re.IGNORECASE | re.DOTALL,
)

# "scope X.collection Y" אופציונלי
_SCOPE_COLLECTION_PATTERN = re.compile(
    r"scope\s+([A-Za-z0-9_.\-]+)(?:.*?collection\s+([A-Za-z0-9_.\-]+))?",
    re.IGNORECASE,
)

# "key X" / "key=X" / "key: X"
_KEY_PATTERN = re.compile(r"key\s*[=:]\s*([A-Za-z0-9_.\-]+)", re.IGNORECASE)

# "field X = Y" / "with X = Y" — מיועד ל-expected_fields
_EXPECTED_FIELD_PATTERN = re.compile(
    r"(?:field|שדה|with)\s+([A-Za-z_][\w.\-]*)\s*[=:]\s*([^\s,;\)\}]+)",
    re.IGNORECASE,
)

# "timeout N" / "תוך N שניות" / "תוך N seconds"
_TIMEOUT_PATTERN = re.compile(
    r"(?:timeout|תוך|בתוך)\s*[:=]?\s*(\d+)\s*(?:שניות|seconds|sec|s)?",
    re.IGNORECASE,
)


def _clean_value(v: str) -> str:
    v = (v or "").strip().rstrip(",;.\"'")
    if len(v) >= 2 and (
        (v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")
    ):
        v = v[1:-1]
    return v


def _extract_expected_fields(text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for m in _EXPECTED_FIELD_PATTERN.finditer(text):
        name = m.group(1)
        value = _clean_value(m.group(2))
        if name.lower() in {"topic", "bucket", "scope", "collection", "key"}:
            continue
        out[name] = value
    return out


def _extract_timeout(text: str, default: int) -> int:
    m = _TIMEOUT_PATTERN.search(text)
    if not m:
        return default
    try:
        return int(m.group(1))
    except ValueError:
        return default


def _try_regex_extract(text: str) -> List[DotNetAction]:
    """מנסה לחלץ רצף actions מטקסט בעברית. ריק → לא נמצא כלום."""
    actions: List[DotNetAction] = []
    if not text:
        return actions

    # 1. Publish
    for m in _PUBLISH_PATTERN.finditer(text):
        topic = m.group(1)
        body_str = m.group(2)
        try:
            value: Any = json.loads(body_str)
        except json.JSONDecodeError:
            value = body_str
        actions.append(KafkaPublishAction(topic=topic, value=value))

    # 2. Kafka wait
    for m in _KAFKA_WAIT_PATTERN.finditer(text):
        topic = m.group(1)
        # ננסה לחלץ expected_fields מהקטע הקרוב (~120 תווים אחרי)
        end_pos = m.end()
        snippet = text[end_pos:end_pos + 200]
        expected = _extract_expected_fields(snippet)
        timeout = _extract_timeout(snippet, settings.KAFKA_DEFAULT_TIMEOUT_SECONDS)
        actions.append(KafkaWaitAction(topic=topic, expected_fields=expected, timeout_seconds=timeout))

    # 3. Couchbase wait
    for m in _COUCHBASE_WAIT_PATTERN.finditer(text):
        bucket = m.group(1)
        key = m.group(2)
        end_pos = m.end()
        snippet = text[end_pos:end_pos + 250]
        expected = _extract_expected_fields(snippet)
        timeout = _extract_timeout(snippet, settings.COUCHBASE_DEFAULT_TIMEOUT_SECONDS)
        scope = None
        collection = None
        sc_match = _SCOPE_COLLECTION_PATTERN.search(snippet)
        if sc_match:
            scope = sc_match.group(1)
            collection = sc_match.group(2)
        # fallback ל-key אם לא נתפס ב-pattern הראשי
        if not key:
            km = _KEY_PATTERN.search(snippet)
            if km:
                key = km.group(1)
        actions.append(
            CouchbaseWaitAction(
                bucket=bucket,
                scope=scope,
                collection=collection,
                key=key,
                expected_fields=expected,
                timeout_seconds=timeout,
            )
        )

    return actions


def _text_mentions_dotnet_action(text: str) -> bool:
    """True אם הטקסט מאזכר את אחת ה-actions — אז regex אמור היה לתפוס משהו."""
    return bool(
        re.search(r"\b(topic|bucket|kafka|couchbase|פרסם|מסר|מסמך)\b", text or "", re.IGNORECASE)
    )


# ============================================================
# Placeholder stripping — defensive post-processing על תשובת LLM
# ============================================================

# תבניות שמסמנות שה-LLM "המציא" ערך/שדה במקום להשתמש ב-template
_PLACEHOLDER_VALUE_PATTERNS = re.compile(
    r"^(MISSING|TBD|TO\s*BE\s*FILLED|<\s*placeholder|requires\s+clarification|N/?A)\b",
    re.IGNORECASE,
)
_PLACEHOLDER_KEY_PATTERNS = re.compile(
    r"^(MISSING_|TODO_|PLACEHOLDER_|UNKNOWN_)",
    re.IGNORECASE,
)


def _is_placeholder_value(v) -> bool:
    if isinstance(v, str):
        return bool(_PLACEHOLDER_VALUE_PATTERNS.match(v.strip()))
    return False


def _is_placeholder_key(k) -> bool:
    if isinstance(k, str):
        return bool(_PLACEHOLDER_KEY_PATTERNS.match(k))
    return False


def _strip_placeholders(obj) -> int:
    """מסיר recursive מ-dict/list כל value שהוא placeholder string, וכל key
    שמתחיל ב-prefix של placeholder. מחזיר את מספר המסירות (לדיבוג).
    """
    removed = 0
    if isinstance(obj, dict):
        for k in list(obj.keys()):
            v = obj[k]
            if _is_placeholder_key(k) or _is_placeholder_value(v):
                obj.pop(k, None)
                removed += 1
                continue
            removed += _strip_placeholders(v)
    elif isinstance(obj, list):
        # מסיר items שהם placeholder, ועובר לעומק על שאר
        keep = []
        for item in obj:
            if _is_placeholder_value(item):
                removed += 1
                continue
            removed += _strip_placeholders(item)
            keep.append(item)
        obj[:] = keep
    return removed


# ============================================================
# LLM fallback
# ============================================================

SYSTEM_PROMPT_DOTNET = """אתה QA Test Compiler עבור מחלקת אינטגרציה .NET במכבי.

המחלקה בודקת Workers שמעבירים מידע: Kafka → Kafka או Kafka → Couchbase.
תסריט בדיקה טיפוסי כולל 2-3 actions:
  1. פרסום מסר ל-source topic (מטריג את ה-Worker)
  2. המתנה למסר ב-target topic, או המתנה למסמך ב-Couchbase
  3. אסרשנים על שדות

קלט: טקסט תסריט בעברית (steps + expected_result מהסוכן).

החזר JSON בלבד בפורמט:
{
  "test_case_id": "string",
  "actions": [
    {"kind": "kafka_publish", "topic": "string", "key": "optional", "value": {...}, "headers": {...} | null},
    {"kind": "kafka_wait", "topic": "string", "match": {...}, "expected_fields": {...}, "timeout_seconds": 30, "expect_no_message": false},
    {"kind": "couchbase_wait", "bucket": "string", "scope": "...", "collection": "...", "key": "...", "query": "...", "expected_fields": {...}, "timeout_seconds": 30}
  ],
  "expected_status": 200,
  "compiler_notes": "string קצר"
}

כללים:
- actions לפי הסדר הטבעי של ה-step.
- "פרסם" / "שלח" → kafka_publish. ה-value הוא ה-payload (JSON dict).
- "ודא שמסר הגיע ל-topic" → kafka_wait.
- "ודא שמסמך נכתב ל-Couchbase" → couchbase_wait. אם key ידוע — הכנס. אם לא, אפשר query N1QL.
- expected_fields — שדות שהמסר/מסמך צריך להכיל (עם הערכים הצפויים).
- timeout_seconds ברירת מחדל 30 אלא אם התסריט אומר אחרת.
- שמור על שמות topics/buckets/keys בדיוק כפי שמופיעים בטקסט (case-sensitive).
- ★ תרחיש שלילי (negative test): אם התסריט בודק ש**לא** עובר מסר ל-target (type_code שגוי, תאריך
  ישן, סינון), קבע expect_no_message=true על KafkaWaitAction. אז timeout = PASS,
  ומסר שיגיע = FAIL.
- אם תסריט מעורפל — החזר actions ריק + compiler_notes מסביר.
- החזר JSON תקני בלבד, ללא טקסט נלווה.
"""


# ★ Prompt חדש כש-payload templates זמינים מסוכן Payload Builder.
# ה-LLM ממזג template + תסריט לערכים מדויקים.
SYSTEM_PROMPT_DOTNET_WITH_TEMPLATES = """אתה QA Test Compiler עבור מחלקת אינטגרציה .NET במכבי.

המחלקה בודקת Workers שמעבירים מידע: Kafka → Kafka או Kafka → Couchbase.

קלט:
1. TEST_CASE — תסריט בעברית. מתאר אילו שדות לשנות ולאיזה ערך, מה הצפוי בתוצאה.
2. PAYLOAD_TEMPLATES — JSON templates מלאים פר action_type (create, delete, update, ...).
   ★ אלה מבני ה-payload המלאים שצריך לשלוח. הם כוללים headers + root + _data וכל
   השדות הנדרשים. השתמש בהם כבסיס ואל תשמיט שדות.
3. FIELD_CATALOG — מילון שדות עם type/format/required/notes. השתמש לאימות התסריט.
4. SOURCE_TOPIC + TARGET_TOPIC — שמות ה-topics לפרסום וקבלה.

תפקידך לכל test case:
1. זהה את action_type מהתסריט ("פתח", "create", "מחק", "delete" וכדומה).
2. קח את הtemplate המתאים מ-PAYLOAD_TEMPLATES — זה הבסיס המלא.
3. ★ זהה אילו שדות התסריט אומר לדרוס (לדוגמה "type_code=99918", "referral_date=2024-01-01").
   החל את הדריסות **רק על השדות שהתסריט מציין**. כל שאר השדות נשארים מה-template.
4. צור KafkaPublishAction עם topic=SOURCE_TOPIC, value=ה-template אחרי הדריסות (JSON מלא).
5. צור KafkaWaitAction עם topic=TARGET_TOPIC (או CouchbaseWaitAction אם התסריט מזכיר Couchbase).
6. אם התסריט הוא תרחיש שלילי (הערך שגוי, תאריך ישן, סינון, "אין להפיץ", "לא יגיע") —
   קבע expect_no_message=true על ה-wait. אז timeout = PASS.
7. expected_fields של ה-wait — שדות במסר ה-target שצריך לאמת (תוצאות העשרה/המרה).

החזר JSON בלבד:
{
  "test_case_id": "string",
  "actions": [
    {"kind": "kafka_publish", "topic": "<SOURCE_TOPIC>", "value": {... template מלא עם דריסות ...}},
    {"kind": "kafka_wait", "topic": "<TARGET_TOPIC>", "expected_fields": {...}, "timeout_seconds": 30, "expect_no_message": false}
  ],
  "expected_status": 200,
  "compiler_notes": "string קצר — אילו דריסות הוחלו ולמה"
}

כללי כתיבה חשובים:
- ★ אל תקצר את ה-template. ה-value של kafka_publish חייב להכיל את **כל** השדות
  שמופיעים ב-template (headers + root + _data), עם דריסות בלבד היכן שהתסריט אומר.
- שמור על מבנה ה-template (nested objects) כפי שהוא.
- אם התסריט אומר "ערך לא תקין" / "שדה ריק" — הכנס את הערך הלא תקין בדיוק (גם אם זה
  string במקום int) כדי לבדוק validation בצד ה-Worker.
- שמור על case-sensitive בשמות topics ו-fields.
- החזר JSON תקני בלבד, ללא טקסט נלווה.

★★★ איסורים מוחלטים — קריטי שלא תפר אותם ★★★
1. **אסור** להוסיף שדות חדשים שלא קיימים ב-template. אם התסריט מאזכר שדה שאינו ב-template,
   רשום ב-compiler_notes ("test case mentions field X not in template") **אבל אל תוסיף אותו ל-value**.
2. **אסור** להחזיר ערכים מסוג "MISSING", "MISSING - ...", "TBD", "TO BE FILLED",
   "<placeholder>", "requires clarification" וכדומה. **לעולם**. אם אינך יודע מה הערך:
   - השאר את הערך מה-template כפי שהוא, או
   - אם אין ערך ב-template — השמט את השדה לגמרי, או
   - אם השדה חובה לפי ה-FIELD_CATALOG — השתמש בערך ריק "" / null / 0 (לפי ה-type).
3. **אסור** להמציא שמות שדות עם prefix כמו "MISSING_id", "TODO_X", "PLACEHOLDER_Y".
   השדות חייבים להיות בדיוק כפי שהם ב-template.
4. אם התסריט עמום מדי לבצע דריסות מדויקות — החזר את ה-template כפי שהוא (בלי דריסות)
   ורשום זאת ב-compiler_notes. **עדיף payload לא מדויק מאשר payload עם placeholders**.
"""


class DotNetCompiler:
    """מהדר תסריט .NET → DotNetExecutableTestCase עם רצף actions.

    שני מצבים:
    - regex-only (לא מועברים templates): מחלץ actions מתסריט קריא, ה-value הוא placeholder.
    - templates-mode (★ מומלץ): מקבל templates + field_catalog מסוכן Payload Builder.
      ה-LLM מוצב כממזג — קח template, החל דריסות מהתסריט, פלוט payload מלא ומדויק.
    """

    def __init__(
        self,
        spec_md: Optional[str] = None,
        payload_templates: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.spec_md = spec_md or ""
        self.payload_templates = payload_templates  # full Payload Builder response

    @property
    def has_templates(self) -> bool:
        return bool(self.payload_templates and self.payload_templates.get("templates"))

    async def compile(self, raw_ado_test_case: Dict[str, Any]) -> DotNetExecutableTestCase:
        ado_id = raw_ado_test_case.get("id")
        title = raw_ado_test_case.get("title") or f"TC-{ado_id}"
        text = raw_ado_test_case.get("text") or title

        # ★ Templates mode: LLM ממזג template + תסריט. עדיף על regex כי ה-payload מלא ומדויק.
        if self.has_templates and settings.azure_openai_enabled:
            llm_result = await self._compile_with_templates(
                test_case_id=title, ado_id=ado_id, text=text,
            )
            if llm_result is not None and llm_result.actions:
                return llm_result
            log.warning("dotnet_compiler_templates_mode_failed_falling_back", tc=title)

        # 0) Regex-first (mode הישן)
        regex_actions = _try_regex_extract(text)
        text_mentions = _text_mentions_dotnet_action(text)
        kinds = {a.kind for a in regex_actions}
        regex_sufficient = bool(regex_actions) and (
            "kafka_publish" in kinds or "kafka_wait" in kinds or "couchbase_wait" in kinds
        )
        if regex_sufficient:
            return DotNetExecutableTestCase(
                test_case_id=title,
                ado_test_case_id=ado_id,
                actions=regex_actions,
                source_text=text,
                compiler_notes=f"extracted via regex — {len(regex_actions)} actions (no templates)",
            )

        # 1) LLM fallback ללא templates
        if settings.azure_openai_enabled and text_mentions:
            llm_result = await self._compile_via_llm(test_case_id=title, ado_id=ado_id, text=text)
            if llm_result is not None and llm_result.actions:
                return llm_result
            log.warning("dotnet_compiler_llm_failed", tc=title)

        # 2) BLOCKED placeholder
        log.warning("dotnet_compiler_no_actions", tc=title, has_text=bool(text), mentions=text_mentions)
        return DotNetExecutableTestCase(
            test_case_id=title,
            ado_test_case_id=ado_id,
            actions=[],
            source_text=text,
            compiler_notes="לא ניתן לחלץ actions — וודא שהסוכן רושם publish/wait מפורש",
        )

    async def _compile_with_templates(
        self,
        test_case_id: str,
        ado_id: Optional[int],
        text: str,
    ) -> Optional[DotNetExecutableTestCase]:
        """★ ממזג template + תסריט. ה-LLM מקבל את template + field_catalog ומפיק actions מדויקים."""
        try:
            client = _make_openai_client()
        except ImportError:
            log.warning("dotnet_compiler_openai_sdk_missing")
            return None

        pt = self.payload_templates or {}
        user_payload = {
            "TEST_CASE": {"id": test_case_id, "ado_id": ado_id, "text": text},
            "SOURCE_TOPIC": pt.get("source_topic"),
            "TARGET_TOPIC": pt.get("target_topic"),
            "PAYLOAD_TEMPLATES": pt.get("templates") or {},
            "FIELD_CATALOG": pt.get("field_catalog") or {},
        }

        try:
            resp = await client.chat.completions.create(
                model=settings.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_DOTNET_WITH_TEMPLATES},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, default=str)},
                ],
                response_format={"type": "json_object"},
                temperature=0,
            )
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
        except Exception as e:
            log.warning("dotnet_compiler_templates_llm_failed", error=str(e), tc=test_case_id)
            return None

        # ★ Post-process: מסנן placeholders שה-LLM החזיר בכל מקרה (גם בניגוד להוראות).
        # יש LLMs שמתעקשים להחזיר "MISSING - ..." או דומה. מסירים את אלה לפני שליחה ל-Kafka.
        _strip_placeholders(data)

        return self._parse_llm_response(test_case_id, ado_id, text, data, source_label="templates")

    async def _compile_via_llm(
        self,
        test_case_id: str,
        ado_id: Optional[int],
        text: str,
    ) -> Optional[DotNetExecutableTestCase]:
        try:
            client = _make_openai_client()
        except ImportError:
            log.warning("dotnet_compiler_openai_sdk_missing")
            return None

        user_payload = {
            "TEST_CASE": {"id": test_case_id, "ado_id": ado_id, "text": text},
            "SPEC_MD": self.spec_md or "(אין MD זמין — חלץ הכל מטקסט ה-TEST_CASE)",
        }

        try:
            resp = await client.chat.completions.create(
                model=settings.AZURE_OPENAI_DEPLOYMENT,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT_DOTNET},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False, default=str)},
                ],
                response_format={"type": "json_object"},
                temperature=0,
            )
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
        except Exception as e:
            log.warning("dotnet_compiler_llm_call_failed", error=str(e), tc=test_case_id)
            return None

        return self._parse_llm_response(test_case_id, ado_id, text, data, source_label="LLM")

    def _parse_llm_response(
        self,
        test_case_id: str,
        ado_id: Optional[int],
        text: str,
        data: Dict[str, Any],
        source_label: str = "LLM",
    ) -> Optional[DotNetExecutableTestCase]:
        """ממיר תשובת LLM (raw dict) ל-DotNetExecutableTestCase tegnerated."""
        raw_actions = data.get("actions") or []
        parsed_actions: List[DotNetAction] = []
        for a in raw_actions:
            if not isinstance(a, dict):
                continue
            kind = a.get("kind")
            try:
                if kind == "kafka_publish":
                    parsed_actions.append(KafkaPublishAction(**a))
                elif kind == "kafka_wait":
                    parsed_actions.append(KafkaWaitAction(**a))
                elif kind == "couchbase_wait":
                    parsed_actions.append(CouchbaseWaitAction(**a))
            except Exception as e:
                log.warning("dotnet_action_parse_failed", kind=kind, error=str(e))

        if not parsed_actions:
            return None

        return DotNetExecutableTestCase(
            test_case_id=test_case_id,
            ado_test_case_id=ado_id,
            actions=parsed_actions,
            expected_status=int(data.get("expected_status") or 200),
            source_text=text,
            compiler_notes=data.get("compiler_notes") or f"extracted via {source_label}",
        )
