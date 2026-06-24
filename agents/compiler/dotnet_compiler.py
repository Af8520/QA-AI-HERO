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


# ★ מודלי reasoning (gpt-5.x / o-series) לא תומכים ב-temperature מותאם (רק ברירת-מחדל=1).
_REASONING_MODEL_RE = re.compile(r"(?:^|[-_/])(?:gpt-?5|o[1-4])\b", re.IGNORECASE)


def _is_concrete_source_path(path: Any) -> bool:
    """True אם ה-source_path הוא נתיב-מקור יחיד שאפשר לדרוס בפועל (ResourceType.field[...].sub).
    False לשדות **נגזרים/מחושבים** שאין להם מקור יחיד: ביטוי concatenation ('a + b'), FIXED/DERIVED,
    או placeholder. override על אלה משחית את המסר — הם לאימות בלבד."""
    if not isinstance(path, str) or not path.strip():
        return False
    p = path.strip()
    if "+" in p:                          # concatenation expression ('family + given[0]')
        return False
    if p.upper().startswith(("FIXED", "DERIVED")):
        return False
    # ★ נתיב סינתטי של ה-Payload Builder ('code__name', 'id__transaction') — אותו מקור עם מיפוי-יעד נוסף,
    # **אינו** שדה אמיתי הניתן לדריסה. override עליו נכשל (השדה לא קיים) → לאימות בלבד (מחושב מה-base).
    if re.search(r"__[A-Za-z]\w*$", p.split(".")[-1]):
        return False
    # ★ defense-in-depth: שרשור שמבוטא ברווח (בלי '+') — יותר מנתיב-עם-נקודה אחד מופרד ברווח.
    # מקור-יחיד תקין הוא token אחד (גם עם פילטר [system=PID] — בלי רווחים בין נתיבים).
    if " " in p and sum(1 for tok in p.split() if "." in tok) > 1:
        return False
    return "." in p or "[" in p          # חייב להיראות כמו נתיב (לא token בודד כמו 'DERIVED')


async def _chat_json(client, model: str, system_prompt: str, user_content: str):
    """קריאת chat-completions שמחזירה JSON, **עמידה להבדלי-מודל**: למודלי gpt-5/o משמיטים temperature
    (הם תומכים רק בברירת-מחדל); ובכל מקרה — אם הקריאה נכשלת על פרמטר לא-נתמך, חוזרים בלי temperature.
    כך אותו קוד עובד גם ל-gpt-4.1-mini (temperature=0) וגם ל-gpt-5.4-mini (בלי temperature)."""
    kwargs: Dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "response_format": {"type": "json_object"},
    }
    if not _REASONING_MODEL_RE.search(str(model or "")):
        kwargs["temperature"] = 0
    try:
        return await client.chat.completions.create(**kwargs)
    except Exception as e:
        msg = str(e).lower()
        if "temperature" in msg or "unsupported" in msg or "max_tokens" in msg:
            kwargs.pop("temperature", None)
            return await client.chat.completions.create(**kwargs)
        raise


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
7. ★ **השמטת שדה (תרחיש "שדה חסר → אובייקט לא נבנה"):** כדי להסיר שדה מהמקור (למשל "אל תשלח ת"ז
   לרופא המפנה") — שים ב-source_overrides את הערך **`"__REMOVE__"`** על נתיב השדה. ה-runner מסיר את
   השדה הספציפי בלבד (לא מרוקן מערכים). **אל תרוקן `identifier: []` ידנית** — זה מוחק את כל המזהים ומשבש.
   דוגמה: `"Practitioner[?(@.id=='referral-id')].identifier[?(@.type.coding.code=='NID')]": "__REMOVE__"`.
   ב-wait, אמת שהאובייקט לא נבנה: `"_data.referral_practitioner": "__ABSENT__"`.
8. ★ **"בנה אובייקט ע"י החלפת code" (למשל הפוך רופא מבצע N → מפנה R):** ב-source_overrides שנה את ה-code
   של ה-PractitionerRole (`PractitionerRole[?(@.code.coding.code=='N')].code.coding[0].code`: "R"),
   ואם נדרש — עדכן/הסר ערכי ה-Practitioner המשויך. (אם הדוגמה לא מכילה PractitionerRole כלל — אי-אפשר
   לבנות; רשום ב-compiler_notes.)

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
- ★★★ לכל צעד "ודא שדה X" / "בדוק ש-X=Y" / "ודא טרנספורמציה X" **בלבד** — הוסף את X ל-expected_fields.
  ★★★★ **קביעת הערך הצפוי — קריטי:**
  1. אם X מומר ע"י **דריסה שהחלת** (קוד מהתסריט, כמו M_PAT_HPV→1, gender M→"זכר") → השתמש בערך המומר.
  2. אם התסריט נותן ערך **מפורש** → השתמש בו.
  3. ★ אחרת — הערך **תלוי-נתונים מ-SOURCE_SAMPLE** (חילוץ/העתקה/resolve של reference, כמו practitioner_id,
     practitioner_name, practitioner_license, member_name, institute) → **חשב את הערך מ-SOURCE_SAMPLE עצמו**
     (לא מ-TARGET_EXAMPLE/template! ערכי ה-template הם דוגמה ולא תואמים את ה-sample האמיתי → כשל שווא).
     אם אינך יכול לחשב בוודאות (FHIR reference מורכב, lookup בין resources) → שים **`"__PRESENT__"`**
     (אימות נוכחות: השדה חולץ וקיים) במקום ערך קונקרטי שגוי.
- ★★★ **לתרחיש "השדה/האובייקט לא אמור להופיע ביעד"** (למשל "referral_practitioner לא נבנה" כשאין רופא-מפנה
  במקור, "ת.ז חסרה → לא נבנה אובייקט") → שים **`"__ABSENT__"`** על השדה/האובייקט. ה-validator יוודא שהוא
  **חסר/ריק** ביעד. למשל: `"_data.referral_practitioner":"__ABSENT__"`. **אל תאמת תת-שדות שלו** (הם יחסרו → כשל שווא).
- ★★★ **אל תאמת member_id / scc_message_id / member_id_code / request_num / תאריכים** אלא אם התסריט ביקש מפורשות.
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

★★★ מזהה ייחודי — אל תטפל בזה! (ה-runner עושה זאת אוטומטית) ★★★
- ★★★★ **אל תשתמש ב-__UNIQUE_ID__ ואל תזריק ערך ייחודי בשום שדה.** ה-runner מייצר אוטומטית KEY ייחודי
  לכל ריצה (על שדה ה-scc_message_id/entity_id) ומקשר לפיו. הזרקת token ידנית **מזיקה** — במיוחד בתרחיש
  שלילי היא דורסת את הערך הלא-תקין שאתה בודק (למשל ת"ז צה"ל) בערך תקין → המסר יתקבל והתרחיש ייכשל.
- ★★★ תפקידך פשוט: **החל רק את הערכים שהתסריט מבקש** (קוד הבדיקה, ערך שלילי, וכו') על השדה הנכון.
  אל תכניס נתיבי member_id קשיחים ל-match/expected_fields אלא אם קיימים ב-TARGET_EXAMPLE.
- ★★★ **מיקוד שדה מדויק (קריטי לתרחישי "כאשר system=X" / "כאשר type.coding.code=Y"):** השתמש בפילטר
  JSONPath כדי לפגוע באלמנט הנכון, **לא** באינדקס מספרי (שעלול לפגוע באלמנט הלא-נכון):
  - "ת.ז כאשר system=PID" → `Patient.identifier[?(@.system=='PID')].value` (לא `identifier[0]`/`identifier[1]`!)
  - "רישיון כאשר type.coding.code=LN" → `Practitioner.identifier[?(@.type.coding.code=='LN')].value`
  ★ **אל תשים סוגריים מרובעים בתוך הפילטר** (לא `type.coding[0].code`) — השתמש ב-`type.coding.code` (יפתר אוטומטית).
  כך לא תשנה בטעות את ה-MRN במקום ה-PID.

החזר JSON בלבד:
{
  "test_case_id": "string",
  "source_overrides": {"<path הערך מהתסריט>": "<value>"},  // ★ רק כש-SOURCE_SAMPLE סופק; רק ערכי התסריט (ללא __UNIQUE_ID__!)
  "actions": [
    {"kind": "kafka_publish", "topic": "<SOURCE_TOPIC>", "value": {}},  // sample → {}; template → value מלא עם דריסות
    {"kind": "kafka_wait", "topic": "<TARGET_TOPIC>",
     "match": {"entity_type":"<TARGET_ENTITY_TYPE אם קיים ב-TARGET_EXAMPLE>", "root.action":"<create/delete אם קיים>"},
     "expected_fields": {"<נתיב שקיים ב-TARGET_EXAMPLE>":"<ערך מ-SAMPLE / __PRESENT__ / __ABSENT__>"},
     "timeout_seconds": 150, "expect_no_message": false}
  ],
  "expected_status": 200,
  "compiler_notes": "string קצר — אילו דריסות הוחלו, ואילו שדות אומתו ב-target"
}
(ה-runner מייצר KEY ייחודי ומקשר לבד — אל תזריק __UNIQUE_ID__. key_contains מושמט. match/expected_fields — רק שדות מ-TARGET_EXAMPLE.
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


# ★★★ Prompt "מעוגן" — בשימוש כשיש transform_index (חוזה source→target מה-Payload Builder).
# ה-LLM **לא** רואה את מסר-הדוגמה, **לא** כותב נתיבי-FHIR, ו**לא** מחשב ערכים. הוא רק מפרש את התסריט
# במונחי שדות-לוגיים. כל מיפוי-הנתיבים וחישוב-הערכים נעשה דטרמיניסטית ב-runner.
SYSTEM_PROMPT_DOTNET_ANCHORED = """אתה QA Test Compiler (.NET, מכבי). המערכת כבר יודעת דטרמיניסטית
איפה כל שדה יושב במקור וביעד (חוזה ה-Worker). תפקידך **רק לפרש את התסריט** למונחים לוגיים — לא לכתוב
נתיבי-JSON/FHIR, לא לחשב ערכים מומרים, לא לשחזר את המסר.

קלט:
- TEST_CASE.text — התסריט בעברית.
- AVAILABLE_FIELDS — רשימת שמות-השדות הלוגיים שמותר להתייחס אליהם (ה-target fields שה-Worker מפיק).
- ACTION_TYPES — סוגי הפעולה האפשריים (create/delete/...).
- TARGET_ENTITY_TYPE.

החזר JSON בלבד במבנה הבא (שום דבר אחר):
{
  "test_case_id": "string",
  "action_type": "<אחד מ-ACTION_TYPES>",
  "verify_all_populated": false,            // true רק אם התסריט אומר "ודא שכל השדות מאוכלסים"
  "overrides": [                            // השדות שהתסריט מבקש *לשנות במקור* (שם-שדה לוגי + הערך מהתסריט)
    {"target_field": "<שם מ-AVAILABLE_FIELDS>", "value": "<הערך מהתסריט, כפי שהוא>"},
    {"target_field": "<שם>", "op": "remove"},            // להשמטת שדה (תרחיש 'שדה חסר → לא לבנות אובייקט')
    {"target_field": "<שם>", "op": "set_first_char", "value": "X"}  // החלף רק את התו הראשון של הערך המקורי
  ],
  "verify": [                               // השדות שהתסריט מבקש *לאמת ביעד*
    {"target_field": "<שם>"},                       // ערך מחושב ע"י המערכת (מיפוי-קוד) או נוכחות
    {"target_field": "<שם>", "expect": "absent"},   // האובייקט/שדה לא אמור להופיע
    {"target_field": "<שם>", "expect": "<ערך literal מהתסריט>"}  // רק אם התסריט נותן ערך מפורש
  ],
  "expect_no_message": false,               // true לתרחיש שלילי ("לא יגיע"/"לא יופץ"/ערך לא-תקין שנדחה)
  "timeout_seconds": 150,
  "compiler_notes": "string קצר"
}

כללים:
- ★★★★ **overrides = כל שדה שהתסריט אומר להגדיר/לשלוח/לשנות/לפסול במקור** — כולל מזהים (ת"ז), קודים,
  וערכים לא-תקינים. תן את **השם הלוגי** של השדה; המערכת ממפה אותו אוטומטית למקור הגולמי ומדלגת בבטחה על
  שדות שאין להם מקור-יחיד (כמו שרשור של כמה שדות). **אינך מסווג שדות** — כשהתסריט משנה ערך-קלט, דְרוֹס אותו.
  **בספק — דְרוֹס, אל תימנע** (שדה שאי-אפשר לדרוס יידחה אוטומטית; שדה שלא נדרס בטעות = תרחיש שנכשל).
- ★ `overrides[].value` = הערך **כפי שהתסריט אומר לשלוח במקור** (למשל קוד M_PAT_HPV, ת"ז לא-תקינה).
  **אל תמיר** (אל תכתוב 1 במקום M_PAT_HPV) — ההמרה נעשית במערכת.
- ★ תרחיש שלילי "ערך לא-תקין" → הוסף override עם הערך הלא-תקין **+** expect_no_message=true.
- ★★ "ספרה/תו ראשון = X" / "מתחיל ב-X" / "קידומת X" / "ת"ז צה"ל (מתחילה ב-2 או 5)" → השתמש ב-
  `{"target_field": "<שדה>", "op": "set_first_char", "value": "X"}`. **אל תפברק ערך מלא** — המערכת תיקח את
  הערך המקורי מהדוגמה ותחליף **רק** את התו הראשון (כך שאר הספרות והאורך התקינים נשמרים). ברוב המקרים זה תרחיש
  שלילי → הוסף גם expect_no_message=true.
- ★ "ודא שכל השדות מאוכלסים" → verify_all_populated=true (אל תפרט שדה-שדה).
- ★ "האובייקט X לא נבנה / לא קיים" → verify עם expect:"absent". אם התסריט גם אומר "אל תשלח את שדה Y"
  → override עם op:"remove" על Y.
- ★★★ השתמש **רק** בשמות מ-AVAILABLE_FIELDS. אם התסריט מאמת **תת-שדות של אובייקט** (למשל
  "practitioner_id/license/name של act_practitioner") ורק האובייקט עצמו (act_practitioner) מופיע
  ב-AVAILABLE_FIELDS — אמת את **האובייקט** (`{"target_field":"act_practitioner"}`), לא את תת-השדות.
  אל תמציא שמות תת-שדה שאינם ברשימה.
- ★ שדה עם **ערכים משורשרים/מרובים** (organ עם ';' וכו') → פשוט `verify` על השדה (בלי expect). המערכת תבנה
  מקור רב-ערכי ותאמת את השרשור המדויק לבד — אל תחשב/תנחש את הערך המשורשר.
- אל תכתוב נתיבים, אינדקסים, או מבני-JSON. שמות לוגיים בלבד. JSON תקני בלבד.
"""


# ★★★ סוכן Source-Builder — בשימוש כשיש transform_index + מסר-דוגמה. בניגוד ל-ANCHORED ה"עיוור",
# הסוכן הזה **רואה את מסר-הדוגמה ואת חוקי-הטרנספורמציה**, ולכן מנמק על כל שדה כמו בודק-אנושי: קורא את
# הערך הנוכחי בדוגמה ומחשב בעצמו את הערך הסופי שצריך לשלוח. הוא מחזיר **עריכות** (לא את המסר המלא).
SYSTEM_PROMPT_SOURCE_BUILDER = """אתה Source-Builder — סוכן QA (.NET, מכבי). לפניך מסר-מקור אמיתי (Bundle),
חוזה-הטרנספורמציות של ה-Worker (source→target + rule), ותסריט-בדיקה. תפקידך: **להחליט אילו עריכות לבצע
במסר-המקור** כדי שהתסריט יתממש — בדיוק כפי שבודק אנושי היה עושה: קורא את הערך הנוכחי בדוגמה, ומחשב את
הערך הסופי לשליחה. אתה **לא** משכתב את כל המסר — רק עריכות נקודתיות.

קלט:
- TEST_CASE.text — התסריט.
- SOURCE_SAMPLE — מסר-המקור האמיתי (קרא ממנו ערכים נוכחיים; **אל תחזיר אותו**).
- TRANSFORMATIONS — מיפוי source_path → {target_field_path, rule}. ה-rule מסביר איך השדה נגזר ביעד
  (קוד-מיפוי "A=1,B=2"; שרשור/concat עם מפריד; ת"ז מתחיל-בספרה; strip; verbatim).
- AVAILABLE_FIELDS — שמות השדות הלוגיים המותרים (target leaves/paths).
- TARGET_ENTITY_TYPE, ACTION_TYPES.

החזר JSON בלבד:
{
  "test_case_id": "string",
  "action_type": "<מ-ACTION_TYPES>",
  "overrides": [                            // עריכות-מקור עם **ערך סופי שחישבת מהדוגמה**
    {"target_field": "<שם לוגי>", "value": "<הערך הסופי לשליחה>"},
    {"target_field": "<שם>", "op": "remove"}
  ],
  "verify_all_populated": false,            // true רק ל"ודא שכל השדות מאוכלסים" (נדיר); אחרת פרט שדות
  "verify": [                               // אימות ביעד — שורה לכל "בדוק/ודא" בתסריט
    {"target_field": "<שדה מ-AVAILABLE_FIELDS>"},                 // שדה מומר → המערכת מחשבת את הערך לבד
    {"target_field": "<שם-שדה כלשהו>", "expect": "<ערך מהתסריט>"}, // שדה שהתסריט נותן לו ערך מפורש (גם אם אינו ב-AVAILABLE_FIELDS, כמו mac_producer_id=75)
    {"target_field": "<שם>", "expect": "absent"}
  ],
  "expect_no_message": false,               // true לתרחיש שלילי (ערך-לא-תקין שנדחה / "לא יגיע")
  "timeout_seconds": 150,
  "compiler_notes": "string קצר"
}

כללים (קריטי):
- ★★★★ **כל "שלח/עם/כאשר field = value" הוא עריכת-מקור — בצע אותה, אל תתעלם כ'הקשר'.** התסריט מתאר את
  מסר-המקור לשליחה. "שלח מסר ... **כאשר** התו הראשון ב-identifier.value הוא 2" = עריכה: קח את הערך הנוכחי
  מהדוגמה (`0999735863`) והחזר עם התו הראשון מוחלף (`2999735863`) — שמור אורך ושאר ספרות, אל תפברק.
- ★★★★ **חשב ערכים סופיים מהדוגמה.** "category.coding.code = M_PAT_HPV" → override value="M_PAT_HPV" (קוד-המקור).
  "name.family=כהן, name.given[0]=יוסי" → שתי עריכות (family, given). "מספר ערכים/משורשר" → ערך/רשימה עם 2+ פריטים.
- ★★★★ **אימות (verify) — שורה לכל "בדוק/ודא" בתסריט, עם הערך המדויק שהתסריט נותן.** מותר לאמת **כל שדה
  שהתסריט מציין** — גם שדות קבועים שאינם ב-AVAILABLE_FIELDS (mac_producer_id=75, mac_sys_name=".NET",
  entity_type=...). תן `expect` עם הערך המדויק מהתסריט. לשדה מומר שב-AVAILABLE_FIELDS בלי ערך מפורש — רק
  {target_field} (המערכת מחשבת). **אל תשתמש ב-verify_all_populated אם התסריט מפרט שדות** — פרט אותם.
- ★ override רק לשדה עם **מקור-יחיד** הניתן לדריסה. שדה מחושב מכמה מקורות (שם מלא) → רק verify. בספק — דְרוֹס.
- ★ תרחיש שלילי (ערך-לא-תקין/צה"ל) → override שמייצר את הערך הלא-תקין + expect_no_message=true.
- ★ "האובייקט X לא נבנה" → verify expect:"absent"; "אל תשלח שדה Y" → override op:"remove" על Y.
- ★ אם התסריט דורש אובייקט/role שאין בדוגמה (רופא מפנה code=R כשיש רק מבצע) → verify על השדה; המערכת תבנה/תמיר לבד.
- ★ אמת **רק** את מה שהתסריט מבקש (תסריט-Header → שדות-Header שהתסריט מפרט בלבד, לא referral/organ לא-קשורים).
- ★ override: שמות מ-AVAILABLE_FIELDS (כדי שהמקור ייפתר). verify: כל שם שהתסריט מציין. שמות לוגיים, JSON תקני בלבד.
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
        transform_index: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.spec_md = spec_md or ""
        self.payload_templates = payload_templates  # full Payload Builder response
        # ★ מסרי-דוגמה אמיתיים מהטופיק מקור (אם היוזר העלה) — בסיס publish format-agnostic
        self.sample_messages = sample_messages or []
        # ★ אינדקס-טרנספורמציות דטרמיניסטי (פאזה 3 ישתמש בו לחוזה LLM מעוגן). None → מסלול ישן.
        self.transform_index = transform_index

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
        # ★★★ סוכן Source-Builder: יש transform_index (+ מסר-דוגמה) → הסוכן **רואה את הדוגמה ואת חוקי-
        # הטרנספורמציה**, ולכן יכול לחשב ערכים סופיים מהדוגמה ("כמו ידנית") ולנמק לפי החוק. הוא מחזיר
        # **עריכות מדויקות** (overrides עם ערכים סופיים) — לא משחזר את ה-Bundle (14KB). המנוע מחיל ומאמת.
        anchored = bool(self.transform_index and self.sample_messages)
        if anchored:
            idx = self.transform_index
            available = list(dict.fromkeys(                       # target paths + leaves, dedup
                list(idx.get("by_target_path") or {}) +
                [k for k, v in (idx.get("by_target_leaf") or {}).items() if v]))
            user_payload = {
                "TEST_CASE": {"id": test_case_id, "ado_id": ado_id, "text": text},
                "TARGET_ENTITY_TYPE": pt.get("target_entity_type"),
                "ACTION_TYPES": list((pt.get("templates") or {}).keys()) or ["create"],
                "AVAILABLE_FIELDS": available,
                # ★ הקשר מלא לסוכן: ה-Bundle האמיתי (לקריאת ערכים נוכחיים + חישוב ערך סופי) וחוקי-הטרנספורמציה
                # (source_path → target + rule) כדי לנמק לפי סוג-החוק (concat/positional/code_map/...).
                "SOURCE_SAMPLE": self.sample_messages[0],
                "TRANSFORMATIONS": idx.get("forward") or pt.get("transformations"),
            }
            system_prompt = SYSTEM_PROMPT_SOURCE_BUILDER
        else:
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
            system_prompt = SYSTEM_PROMPT_DOTNET_WITH_TEMPLATES

        try:
            resp = await _chat_json(client, settings.compiler_deployment, system_prompt,
                                    json.dumps(user_payload, ensure_ascii=False, default=str))
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
        except Exception as e:
            log.warning("dotnet_compiler_templates_llm_failed", error=str(e), tc=test_case_id)
            return None

        if anchored:
            return self._parse_anchored_response(test_case_id, ado_id, text, data)

        # ★ Post-process: מסנן placeholders שה-LLM החזיר בכל מקרה (גם בניגוד להוראות).
        # יש LLMs שמתעקשים להחזיר "MISSING - ..." או דומה. מסירים את אלה לפני שליחה ל-Kafka.
        _strip_placeholders(data)

        return self._parse_llm_response(test_case_id, ado_id, text, data, source_label="templates")

    def _parse_anchored_response(self, test_case_id, ado_id, text, data) -> Optional[DotNetExecutableTestCase]:
        """ממיר את פלט ה-LLM המעוגן (שדות-לוגיים) ל-DotNetExecutableTestCase: ממפה כל override
        ל-source_path מדויק דרך ה-transform_index (בלי ניחוש), מסנתז publish/wait, ושומר verify_spec
        ל-runner. ה-runner יבנה את ה-publish מהדוגמה + הדריסות, ואת expected_fields מ-verify_spec."""
        from agents.runner.dotnet_runner import (_resolve_source_path, _canonical_target_path,
                                                 _SET_FIRST_CHAR_PREFIX, _ENSURE_MULTI_MARKER,
                                                 _strip_synthetic_suffix)
        pt = self.payload_templates or {}
        idx = self.transform_index or {}
        source_overrides: Dict[str, Any] = {}
        notes = [data.get("compiler_notes") or ""]
        for ov in (data.get("overrides") or []):
            if not isinstance(ov, dict):
                continue
            tf = ov.get("target_field")
            src = _resolve_source_path(idx, tf) if tf else None
            if not src:
                notes.append(f"override '{tf}' לא נפתר ל-source (לא ב-transformations/collision) — דולג")
                continue
            # ★ אין מקור-יחיד שאפשר לדרוס (שרשור 'a + b', FIXED/DERIVED, או ביטוי רב-נתיבי) — override
            # כזה היה משחית את המסר (למשל name→"0"). מדלגים בבטחה; שדה כזה לאימות בלבד. (מקור-יחיד קונקרטי,
            # כולל extraction/split כמו member_id←PID, **כן** נדרס — זה מה שמאפשר תרחישי ת"ז לא-תקינה.)
            if not _is_concrete_source_path(src):
                notes.append(f"override '{tf}' → ({src}) אין מקור-יחיד בר-דריסה (שרשור/נגזר) — אימות בלבד. דולג")
                continue
            if ov.get("op") == "remove":
                source_overrides[src] = "__REMOVE__"
                continue
            # ★ op:"set_first_char" — מוטציה-חלקית: המערכת תיקח את הערך המקורי מהדוגמה ותחליף רק את התו
            # הראשון (תרחיש "ספרה ראשונה=X"/"מתחיל ב-X", כמו ת"ז צה"ל) — בלי לפברק ערך ובשמירת האורך.
            if ov.get("op") == "set_first_char":
                ch = str(ov.get("value", ""))[:1]
                if ch:
                    source_overrides[src] = _SET_FIRST_CHAR_PREFIX + ch
                    notes.append(f"מוטציית תו-ראשון '{tf}'→{ch} (נשמר שאר הערך מהדוגמה)")
                continue
            val = ov.get("value")
            # ★ reverse-map: ה-LLM לפעמים נותן את ערך-היעד (2) במקום קוד-המקור (M_PAT_NGC). אם לשדה יש
            # code_map והערך הוא RHS (ערך-יעד) — הופכים אותו חזרה ל-LHS (קוד-המקור), כדי שה-Worker יזהה.
            rule = (idx.get("rules") or {}).get(_canonical_target_path(idx, tf))
            if rule and rule.get("kind") == "code_map":
                cmap = rule.get("map") or {}
                if str(val) not in cmap and str(val) in {str(v) for v in cmap.values()}:
                    src_code = next((k for k, v in cmap.items() if str(v) == str(val)), None)
                    if src_code is not None:
                        notes.append(f"reverse-map '{tf}': {val} → {src_code} (ה-LLM נתן ערך-יעד)")
                        val = src_code
            source_overrides[src] = val

        publish = KafkaPublishAction(topic=pt.get("source_topic") or "", value={})
        expect_no_message = bool(data.get("expect_no_message"))
        wait = KafkaWaitAction(
            topic=pt.get("target_topic") or "",
            timeout_seconds=int(data.get("timeout_seconds") or 150),
            expect_no_message=expect_no_message,
        )
        verify_all = bool(data.get("verify_all_populated"))
        verify_list = data.get("verify") or []
        # ★★★ חילוץ-קוד דטרמיניסטי מהתסריט (סוף ה-variance בתסריטי-קוד): קודי-המקור (M_PAT_HPV...) מופיעים
        # **מילולית** בטקסט ("category.coding.code = M_PAT_HPV"). סורקים מול אוצר-הקודים של כל code_map ומגדירים
        # את שדה-המקור + מוסיפים verify לכל היעדים שניזונים ממנו (code+name) → המנוע מחשב ערך מדויק. אפס תלות
        # ב-LLM, דינמי לכל code_map שה-PB מגדיר. גובר על ה-LLM (הקוד בטקסט = אמת-קרקע).
        by_tp = idx.get("by_target_path") or {}
        for tfp, rule in (idx.get("rules") or {}).items():
            if not (isinstance(rule, dict) and rule.get("kind") == "code_map"):
                continue
            base = _strip_synthetic_suffix(by_tp.get(tfp))
            if not base or not _is_concrete_source_path(base):
                continue
            cmap = rule.get("map") or {}
            for code in sorted(cmap, key=len, reverse=True):       # ארוך-ראשון (M_PAT_HPV לפני HPV)
                if re.search(r"(?<![A-Za-z0-9_])" + re.escape(code) + r"(?![A-Za-z0-9_])", text or ""):
                    if source_overrides.get(base) != code:
                        source_overrides[base] = code
                        notes.append(f"חילוץ-קוד דטרמיניסטי מהתסריט: {base} = {code}")
                    # ודא verify לכל היעדים שניזונים מאותו מקור (examination_type_code + _name)
                    have = {v.get("target_field") for v in verify_list if isinstance(v, dict)}
                    for t, s in by_tp.items():
                        leaf = t.split(".")[-1]
                        if _strip_synthetic_suffix(s) == base and t not in have and leaf not in have:
                            verify_list.append({"target_field": leaf})
                            have.add(leaf)
                    break
        # ★★★ גזירת override מ-verify (תיקון מרכזי): בתרחיש מיפוי-קוד חיובי ("ודא ש-Z_PAT_NGC→2") ה-LLM
        # פולט verify עם ערך-יעד צפוי (2) אבל **שוכח להוסיף override** שמגדיר את הקוד במקור → המקור נשאר
        # ערך-הדוגמה (M_PAT_HIST→3) → הערך הצפוי לא מתקבל. דטרמיניסטית: אם ל-verify יש ערך-יעד של code_map
        # והמקור של אותו שדה עוד לא נדרס — reverse-map את ערך-היעד לקוד-מקור ומזריקים override. דינמי לכל code_map.
        if not expect_no_message:
            for v in verify_list:
                if not isinstance(v, dict):
                    continue
                tf = v.get("target_field")
                if not tf:
                    continue
                src = _resolve_source_path(idx, tf)
                if not src or not _is_concrete_source_path(src) or src in source_overrides:
                    continue
                rule = (idx.get("rules") or {}).get(_canonical_target_path(idx, tf))
                kind = rule.get("kind") if rule else None
                # ★ concatenate: עצם בדיקת השדה = בדיקת השרשור → setup ריבוי-ערכים במקור (ENSURE_MULTI),
                # גם בלי expect מפורש. ה-runner יפיק מפריד ביעד וה-forward יאמת אותו מדויק. דינמי לכל שדה-רשימה.
                if kind == "concatenate":
                    source_overrides[src] = _ENSURE_MULTI_MARKER
                    notes.append(f"setup concatenate '{tf}': __ENSURE_MULTI__ (≥2 ערכים במקור) — דטרמיניסטי")
                    continue
                # code_map/verbatim: גזירת override מ-expect (דורש ערך-יעד מפורש מהתסריט)
                exp = v.get("expect")
                if exp in (None, "", "auto", "compute", "present", "absent", "__PRESENT__", "__ABSENT__"):
                    continue
                if kind == "code_map":
                    cmap = rule.get("map") or {}
                    if str(exp) in cmap:                               # exp הוא כבר קוד-מקור
                        chosen = str(exp)
                    else:                                              # exp הוא ערך-יעד → reverse-map לקוד-מקור
                        srcs = [k for k, vv in cmap.items() if str(vv) == str(exp)]
                        if not srcs:
                            continue
                        # כמה קודים לאותו ערך (M_PAT_NGC/Z_PAT_NGC→2) — בוחרים את זה שבשם-התסריט; אחרת הראשון
                        chosen = next((c for c in srcs if c in (test_case_id or "")), srcs[0])
                elif kind == "verbatim":
                    chosen = str(exp)                                  # verbatim: ערך-המקור == ערך-היעד (urgent=0/1)
                else:
                    continue                                          # derived/lookup — לא ניתן לגזור דטרמיניסטית
                source_overrides[src] = chosen
                notes.append(f"גזירת override מ-verify '{tf}': יעד-צפוי={exp} → מקור={chosen} "
                             f"(ה-LLM לא הוסיף override; דטרמיניסטי)")
        # ★ גארד תרחיש-חיובי-ריק: תרחיש חיובי (לא expect_no_message) בלי overrides, בלי verify, ובלי
        # verify_all_populated → assert ריק → "pass" טריוויאלי שקרי (כמו referral שעבר בלי שנבדק). ברירת-מחדל
        # בטוחה: אמת שכל השדות הממופים מאוכלסים. אם הדוגמה חסרה את המבנה — האימות ייכשל נכון, לא יעבור בשקר.
        if not expect_no_message and not source_overrides and not verify_list and not verify_all:
            verify_all = True
            notes.append("תרחיש חיובי בלי דריסות/אימות מפורש — הוחל verify_all_populated (מניעת pass שקרי)")
        ex = DotNetExecutableTestCase(
            test_case_id=test_case_id,
            ado_test_case_id=ado_id,
            actions=[publish, wait],
            source_text=text,
            compiler_notes=" | ".join(n for n in notes if n) or "anchored",
        )
        ex.source_overrides = source_overrides
        ex.verify_spec = {"verify_all_populated": verify_all, "verify": verify_list}
        if self.sample_messages:
            ex.source_sample = self.sample_messages[0]
        return ex

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
            resp = await _chat_json(client, settings.compiler_deployment, SYSTEM_PROMPT_DOTNET,
                                    json.dumps(user_payload, ensure_ascii=False, default=str))
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
