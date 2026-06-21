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
    {"kind": "kafka_wait", "topic": "string", "key_contains": "<מזהה מה-publish לזיהוי המסר שלנו>", "match": {...}, "expected_fields": {"header.x":"...", "_data.parameters.0.y":"..."}, "timeout_seconds": 30, "expect_no_message": false},
    {"kind": "couchbase_wait", "bucket": "string", "scope": "...", "collection": "...", "key": "...", "query": "...", "expected_fields": {...}, "timeout_seconds": 30}
  ],
  "expected_status": 200,
  "compiler_notes": "string קצר"
}

כללים:
- actions לפי הסדר הטבעי של ה-step.
- "פרסם" / "שלח" → kafka_publish. ה-value הוא ה-payload (JSON dict).
- "ודא שמסר הגיע ל-topic" → kafka_wait. אם התסריט מציין KEY בפורמט (entity::מזהה::קוד) — קבע
  key_contains = המזהה ששלחת ב-publish (member_id/technical_id/entity_id לפי האפיון), כדי לתפוס את
  המסר שלנו ב-topic משותף. expected_fields עם dotted paths לערכים המומרים; דלג על GUID/תאריכים.
- "ודא שמסמך נכתב ל-Couchbase" → couchbase_wait. אם key ידוע — הכנס. אם לא, אפשר query N1QL.
- expected_fields — שדות שהמסר/מסמך צריך להכיל (עם הערכים הצפויים).
- timeout_seconds ברירת מחדל 30 אלא אם התסריט אומר אחרת.
- שמור על שמות topics/buckets/keys בדיוק כפי שמופיעים בטקסט (case-sensitive).
- ★ תרחיש שלילי (negative test): אם התסריט בודק ש**לא** עובר מסר ל-target (type_code שגוי, תאריך
  ישן, סינון), קבע expect_no_message=true על KafkaWaitAction. אז timeout = PASS,
  ומסר שיגיע = FAIL.
- ★★★ **ודא לוג / Elastic / "לוג הצלחה/שגיאה"**: אין תמיכה ב-.NET כרגע. **אל תיצור action**
  עבור צעד כזה — ובמיוחד **אל תיצור kafka_wait מזויף** (הוא יעבור על מסר אקראי וייתן PASS שקרי).
  דלג על הצעד ורשום ב-compiler_notes ("דילגנו על אימות לוג — לא נתמך ב-.NET").
- אם תסריט מעורפל — החזר actions ריק + compiler_notes מסביר.
- החזר JSON תקני בלבד, ללא טקסט נלווה.
"""


# ★ Prompt חדש כש-payload templates זמינים מסוכן Payload Builder.
# ה-LLM ממזג template + תסריט לערכים מדויקים.
SYSTEM_PROMPT_DOTNET_WITH_TEMPLATES = """אתה QA Test Compiler עבור מחלקת אינטגרציה .NET במכבי.

המחלקה בודקת Workers שמעבירים מידע: Kafka → Kafka או Kafka → Couchbase.

קלט:
1. TEST_CASE — תסריט בעברית. מתאר אילו שדות לשנות ולאיזה ערך, מה הצפוי בתוצאה.
2. ★★★ SOURCE_SAMPLE (אם סופק — **קודם לכל השאר!**) — מסר אמיתי מהטופיק מקור (FHIR Bundle / כל מבנה).
   ★★★ **אם SOURCE_SAMPLE לא null → אל תשחזר אותו! ה-runner ישתמש בו כבסיס כפי-שהוא.** אתה מחזיר
   רק `source_overrides` — מפה קטנה של הדריסות שהתסריט מציין: `{"<נתיב מלא או שם-שדה>": <ערך>}`.
   ה-`value` של ה-kafka_publish יכול להיות `{}` (הרנר ממלא אותו מהדוגמה ומחיל את הדריסות).
   זה מונע שחזור שגוי/חתוך של מסר ענק (14KB).
3. PAYLOAD_TEMPLATES — JSON templates פר action_type. **בשימוש רק כש-SOURCE_SAMPLE הוא null.**
   (פורמט MACKAF: headers + root + _data.)
4. FIELD_CATALOG — מילון שדות עם type/format/required/notes. לאימות.
5. SOURCE_TOPIC + TARGET_TOPIC — שמות ה-topics לפרסום וקבלה.
6. TARGET_ENTITY_TYPE + TARGET_EXAMPLE (אופציונלי) — מבנה מסר ה-target + ה-KEY format והשדות המומרים.
   הסתמך עליהם לזיהוי שדה ה-KEY ולבניית match/expected_fields.
7. KEY_BUILT_FROM (אופציונלי) — נתיבי-המקור שה-target KEY בנוי מהם (entity_id/member_id). השדה
   המזהה הייחודי. (ה-runner מזריק לו ערך ייחודי אוטומטית — אתה רק כלול אותו ב-correlation/expected.)
8. TRANSFORMATIONS (אופציונלי) — מיפוי שדות source→target (gender M→"זכר").

תפקידך לכל test case:
1. זהה את action_type מהתסריט ("פתח", "create", "מחק", "delete" וכדומה).
2. ★ זהה אילו שדות התסריט אומר לדרוס (לדוגמה "type_code=99918", "category.coding.code=M_PAT_HPV").
3. ★ **אם SOURCE_SAMPLE סופק** → החזר את הדריסות כ-`source_overrides` (מפה: נתיב→ערך) **ו-`value:{}`**.
   הרנר בונה את המסר מהדוגמה + הדריסות. **אם SOURCE_SAMPLE הוא null** → קח את ה-template המתאים
   מ-PAYLOAD_TEMPLATES, החל עליו את הדריסות, ושים אותו ב-`value` המלא (כמו קודם).
4. צור KafkaPublishAction עם topic=SOURCE_TOPIC (value מלא במצב template, או {} + source_overrides במצב sample).
5. צור KafkaWaitAction עם topic=TARGET_TOPIC (או CouchbaseWaitAction אם התסריט מזכיר Couchbase).
6. אם התסריט הוא תרחיש שלילי (הערך שגוי, תאריך ישן, סינון, "אין להפיץ", "לא יגיע") —
   קבע expect_no_message=true על ה-wait. אז timeout = PASS.

★★★ זיהוי המסר שלנו ב-target (correlation) — קריטי, format-agnostic ★★★
ה-target topic משותף — הרבה מסרים לא קשורים (verifyhub, user_login_status...). חייבים לזהות
את *המסר שלנו*. ★ ה-correlation הראשי הוא ה-**KEY**: ה-runner מזריק **ערך ייחודי לריצה** לשדה
ה-`KEY_BUILT_FROM` (במקור) וממלא אוטומטית `key_contains` בערך הזה. ה-target KEY שה-Worker מפיק
בנוי מאותו שדה → ה-uid הייחודי מופיע ב-key של *המסר שלנו בלבד*. **אינך צריך למלא key_contains** —
ה-runner עושה זאת. זה עובד בכל פורמט (FHIR/MACKAF) כי הוא נשען על KEY_BUILT_FROM ולא על נתיב קשיח.
- ★★★ **match — רק שדות שקיימים בפועל ב-TARGET_EXAMPLE.** אל תוסיף נתיב שאינו ב-TARGET_EXAMPLE —
  הוא יגרום ל-timeout (המסר לא יימצא). שדות מומלצים אם הם קיימים ב-TARGET_EXAMPLE:
  1. `entity_type` (מ-TARGET_ENTITY_TYPE) — דוחה verifyhub/מסרים זרים.
  2. `action` / `root.action` (אם קיים ב-TARGET_EXAMPLE) — דוחה create כשציפינו ל-delete.
  למשל (MACKAF): `"match": {"entity_type":"child_development", "root.action":"create"}`.
  אם TARGET_EXAMPLE ריק או לא ידוע — השאר `match` ריק והסתמך על ה-KEY (key_contains שה-runner ממלא).
- ★★★ **אל תכניס נתיב member_id קשיח** כמו `_data.parameters.0.member_id` אלא אם הוא **קיים בפועל**
  ב-TARGET_EXAMPLE. ה-uid הייחודי כבר מצוי ב-KEY → ה-match הוא רק סינון משני של פורמט המסר.
- key_equals = ה-key המלא רק אם הפורמט (כולל הקוד והסדר) ודאי לחלוטין.

★★★ expected_fields — אמת **רק** את מה שהתסריט מבקש במפורש (לא את כל המסר!) ★★★
- ★★★★ **אסור בתכלית האיסור לאמת את כל שדות TARGET_EXAMPLE.** TARGET_EXAMPLE הוא רק כדי לדעת את
  **הנתיב המדויק** של שדה ולוודא שהוא **קיים** — הוא **אינו** רשימת שדות לאימות. אם תכניס עשרות
  שדות (member_name/request_num/institute/practitioner/...) שהתסריט לא ביקש — הבדיקה תיכשל על ערכים
  לא רלוונטיים. **expected_fields חייב להכיל רק את השדות מצעדי ה"ודא/בדוק" של התסריט — בד"כ 1-4 שדות.**
- ★★★ לכל צעד "ודא שדה X" / "בדוק ש-X=Y" / "ודא טרנספורמציה X" **בלבד** — הוסף את X ל-expected_fields:
  - אם X מומר (מופיע ב-TRANSFORMATIONS) → השתמש ב**ערך המומר** לפי ה-rule (למשל M_PAT_HPV→1, gender M→"זכר").
  - אם התסריט נותן ערך מפורש → השתמש בו.
  - נתיב מדויק לפי TARGET_EXAMPLE, case-sensitive (עם index ל-arrays אם רלוונטי).
- ★★★ **אל תאמת member_id / member_name / request_num / institute / practitioner / תאריכים / scc_message_id /
  member_id_code** אלא אם התסריט ביקש זאת **מפורשות**. ה-member_id במיוחד נדרס לערך ייחודי — אסור לאמת אותו.
- ★★★ **אם התסריט מבקש לאמת שדה שאינו קיים ב-TARGET_EXAMPLE** — אל תכניס אותו; רשום ב-compiler_notes.
  אם אין צעדי "ודא" כלל — השאר `expected_fields` ריק והסתמך על הקורלציה.
- ★★★ **אל תאמת `entity_id` / ה-KEY / `entity_type` כשדה ערך** — אלה ה-**correlation** שכבר מאמת
  ב-match לפי ה-key. אימותם ב-expected_fields הוא כפילות מיותרת ושביר (ה-entity_id מכיל את
  ה-member_id שה-runner דורס לערך ייחודי). זהו ב-match בלבד, לא ב-expected_fields.
- ★★★ **ערך דינמי / מוצפן / לא-צפוי** (pdf_link מוצפן/RSA, ערך מוצפן, GUID, hash, timestamp) —
  **אל תאמת שוויון לערך מסוים.** במקום זה שים את ה-value המיוחד `"__PRESENT__"` — ה-validator
  יבדוק שהשדה **קיים ולא-ריק** (לא ערך ספציפי). למשל: `"_data.parameters.0.pdf_link":"__PRESENT__"`.
- ★★★ **נתיב מדויק לפי TARGET_EXAMPLE.** קח את הנתיב המדויק מ-TARGET_EXAMPLE (למשל אם
  `resource_type` יושב תחת `_data` ולא תחת `_data.parameters` — כתוב `_data.resource_type`).
- ★★★ **אל תאמת metadata של ה-producer** — `header.mac_*`. אינך יודע את ערכיהם.
- ★ **דלג על שדות דינמיים** — message_id=GUID, תאריכים, timestamps (או השתמש ב-__PRESENT__).

★★★ מזהה ייחודי — קריטי לקורלציה (השתמש ב-__UNIQUE_ID__) ★★★
ה-target topic מלא בכפילויות (אותו מסר מ-runs קודמים). כדי לזהות *בדיוק את המסר שלנו*, ה-runner מזריק
ערך ייחודי לכל ריצה, ומחפש אותו במסר היעד (ב-KEY או בגוף). תפקידך: **לסמן היכן יושב המזהה העסקי**:
- ★★★ זהה את שדה המזהה העסקי במקור — ה-member_id / מספר חבר / ת.ז (לרוב הסגמנט האחרון של KEY_BUILT_FROM,
  למשל `identifier.value` ב-FHIR או `member_details.member_id` ב-MACKAF). **שים בו את הערך המילולי
  `"__UNIQUE_ID__"`** (כ-string) ב-`source_overrides` (או ב-`value` במצב template). ה-runner יחליף אותו
  בערך ייחודי אמיתי ויחפש אותו ב-target.
  למשל ב-FHIR: `"source_overrides": {"<נתיב ה-identifier של החבר>": "__UNIQUE_ID__", "<נתיב הקוד>": "M_PAT_HPV"}`.
- ★ **אל תכניס נתיבי member_id קשיחים ל-match/expected_fields** (`_data.parameters.0.member_id`) אלא אם הם
  קיימים בפועל ב-TARGET_EXAMPLE. הקורלציה הייחודית מטופלת ע"י ה-runner דרך __UNIQUE_ID__.
- אם אינך יודע איזה שדה הוא ה-member_id — שים `__UNIQUE_ID__` בשדה שמתאים ל-KEY_BUILT_FROM[0].

החזר JSON בלבד:
{
  "test_case_id": "string",
  "source_overrides": {"<member_id path>": "__UNIQUE_ID__", "<other path>": "<value>"},  // ★ רק כש-SOURCE_SAMPLE סופק
  "actions": [
    {"kind": "kafka_publish", "topic": "<SOURCE_TOPIC>", "value": {}},  // sample → {}; template → value מלא עם דריסות (כולל __UNIQUE_ID__ בשדה ה-id)
    {"kind": "kafka_wait", "topic": "<TARGET_TOPIC>",
     "match": {"entity_type":"<TARGET_ENTITY_TYPE אם קיים ב-TARGET_EXAMPLE>", "root.action":"<create/delete אם קיים>"},
     "expected_fields": {"<נתיב שקיים ב-TARGET_EXAMPLE>":"<ערך מומר/צפוי>"},
     "timeout_seconds": 150, "expect_no_message": false}
  ],
  "expected_status": 200,
  "compiler_notes": "string קצר — אילו דריסות הוחלו, ואילו שדות אומתו ב-target"
}
(שדה ה-member_id = "__UNIQUE_ID__" → ה-runner מזריק ומקשר. key_contains מושמט. match/expected_fields — רק שדות מ-TARGET_EXAMPLE.
SOURCE_SAMPLE → value:{} + source_overrides; אחרת value מלא + השמט source_overrides.)

כללי כתיבה חשובים:
- ★★★ **ודא לוג / Elastic / "לוג הצלחה/שגיאה"**: אין תמיכה ב-.NET כרגע. **אל תיצור action** לצעד
  כזה, ובמיוחד **אל תיצור kafka_wait מזויף** (הוא יעבור על מסר אקראי וייתן PASS שקרי). דלג עליו
  ורשום ב-compiler_notes ("דילגנו על אימות לוג — לא נתמך ב-.NET").
- ★ **מצב SOURCE_SAMPLE**: אל תשחזר את הדוגמה ל-`value` — החזר `value:{}` + `source_overrides`. הרנר
  בונה את המסר מהדוגמה כפי-שהיא (format-agnostic). **מצב template (אין sample)**: ה-`value` חייב להכיל
  את **כל** שדות ה-template עם דריסות בלבד, ולשמור headers+root+_data (MACKAF).
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


def _extract_kbf(pt: Dict[str, Any]) -> Optional[List[str]]:
    """מחלץ key_built_from מתשובת ה-Payload Builder (top-level או target_templates[*])."""
    if not isinstance(pt, dict):
        return None
    top = pt.get("key_built_from")
    if isinstance(top, list) and top:
        return top
    for tmpl in (pt.get("target_templates") or {}).values():
        if isinstance(tmpl, dict) and isinstance(tmpl.get("key_built_from"), list) and tmpl["key_built_from"]:
            return tmpl["key_built_from"]
    return None


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
        sample_messages: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.spec_md = spec_md or ""
        self.payload_templates = payload_templates  # full Payload Builder response
        # ★ מסרי-דוגמה אמיתיים מהטופיק מקור (אם היוזר העלה) — בסיס publish format-agnostic
        self.sample_messages = sample_messages or []

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
            # ★ מבנה ה-target (אם ה-Payload Builder מספק) — לזיהוי שדה ה-KEY ושדות מומרים
            "TARGET_ENTITY_TYPE": pt.get("target_entity_type"),
            "TARGET_EXAMPLE": pt.get("target_example") or pt.get("target_templates"),
            "TRANSFORMATIONS": pt.get("transformations"),
            # ★ מסר-דוגמה אמיתי מהמקור (אם היוזר העלה) — בסיס ה-publish כפי-שהוא (format-agnostic)
            "SOURCE_SAMPLE": (self.sample_messages[0] if self.sample_messages else None),
            "SOURCE_SAMPLES_COUNT": len(self.sample_messages),
            "KEY_BUILT_FROM": _extract_kbf(pt),
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

        executable = DotNetExecutableTestCase(
            test_case_id=test_case_id,
            ado_test_case_id=ado_id,
            actions=parsed_actions,
            expected_status=int(data.get("expected_status") or 200),
            source_text=text,
            compiler_notes=data.get("compiler_notes") or f"extracted via {source_label}",
        )
        # ★ מסר-דוגמה אמיתי → בסיס publish דטרמיניסטי ברנר (ה-LLM מחזיר רק source_overrides קטן,
        # לא משחזר את ה-14KB). אם ה-LLM כן החזיר value מלא — הרנר עדיין דורס בו את הדריסות.
        if self.sample_messages:
            executable.source_sample = self.sample_messages[0]
            ov = data.get("source_overrides")
            executable.source_overrides = ov if isinstance(ov, dict) else {}
        return executable
