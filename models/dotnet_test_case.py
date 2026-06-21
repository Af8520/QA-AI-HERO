"""DotNetExecutableTestCase — הפלט של DotNetCompiler, קלט ל-DotNetRunner.

בניגוד ל-ESB שהוא HTTP-locked, בדיקות .NET מורכבות מרצף של actions:
- KafkaPublishAction: פרסום מסר ל-source topic
- KafkaWaitAction: המתנה למסר ב-target topic + אסרשנים על שדות
- CouchbaseWaitAction: המתנה למסמך ב-Couchbase bucket + אסרשנים

ה-Runner מבצע אותן בסדר ומחזיר TestCaseResult תקני (אותה struct כמו ESB),
כך ש-Validator/Reporter עובדים בלי שינוי.
"""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class KafkaPublishAction(BaseModel):
    """פרסום מסר סינתטי ל-source topic — מטריג את ה-Worker."""

    kind: Literal["kafka_publish"] = "kafka_publish"
    topic: str
    key: Optional[str] = None
    value: Any  # JSON-serializable dict / string
    headers: Optional[Dict[str, str]] = None


class KafkaWaitAction(BaseModel):
    """המתנה למסר ב-target topic + אסרשנים על שדות.

    correlation — איך מזהים שזה *המסר שלנו* בתוך topic משותף (verifyhub וכו'):
    - key_contains: תת-מחרוזת ב-key (לרוב המזהה הדינמי — member_id/technical_id/entity_id
      לפי האפיון; המוח ממלא את אותו ערך שהוזרק ל-publish).
    - key_equals: key מדויק (כשהפורמט המלא ידוע).
    - match: שדות ערך (dotted paths) שצריכים להתקיים במסר.
    רשומה תואמת אם *כל* ה-matchers שמולאו מתקיימים.

    expected_fields — שדות (dotted/list paths) המומרים שצריך לאמת ב-target. דינמיים
    (GUID/תאריך/message_id) לא נכנסים לכאן.
    expect_no_message — תרחיש שלילי: timeout = PASS, מסר שמגיע = FAIL.
    """

    kind: Literal["kafka_wait"] = "kafka_wait"
    topic: str
    key_equals: Optional[str] = None
    key_contains: Optional[str] = None
    # ★ קורלציה לפי הערך הייחודי בכל מקום במסר היעד (KEY *או* גוף ה-value). format-agnostic —
    # ה-runner ממלא אותו ב-uid הייחודי. עדיף על key_contains כשה-target KEY אינו נושא את ה-id
    # (למשל FHIR: ה-KEY הוא scc_message_id, וה-uid יושב ב-_data.member_id בגוף).
    value_contains: Optional[str] = None
    match: Dict[str, Any] = Field(default_factory=dict)
    expected_fields: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 30
    expect_no_message: bool = False


class CouchbaseWaitAction(BaseModel):
    """המתנה למסמך ב-Couchbase + אסרשנים.

    אם key ידוע — get ישיר ב-retry loop.
    אחרת — query N1QL כ-fallback.
    """

    kind: Literal["couchbase_wait"] = "couchbase_wait"
    bucket: str
    scope: Optional[str] = None
    collection: Optional[str] = None
    key: Optional[str] = None
    query: Optional[str] = None
    expected_fields: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 30


DotNetAction = Union[KafkaPublishAction, KafkaWaitAction, CouchbaseWaitAction]


class DotNetExecutableTestCase(BaseModel):
    """תסריט .NET רץ עם רצף actions."""

    test_case_id: str
    ado_test_case_id: Optional[int] = None
    actions: List[DotNetAction] = Field(default_factory=list)
    expected_status: int = 200  # סטטוס סינתטי לשמור compat עם הסטטיסטיקה של הריפורטר
    source_text: Optional[str] = None
    compiler_notes: Optional[str] = None
    # ★ נתיבי-המקור שה-target KEY בנוי מהם (מ-Payload Builder key_built_from). ה-runner מזריק
    # לפיהם member_id/entity_id ייחודי — format-agnostic, לא קשיח ל-member_id. ריק → fallback ל-member_id.
    key_built_from: Optional[List[str]] = None
    # ★ מסר-דוגמה אמיתי מהמקור (אם היוזר העלה) — בסיס ה-publish הדטרמיניסטי. ה-runner לוקח אותו
    # כפי-שהוא ומחיל מעליו את source_overrides + ה-id הייחודי בקוד (LLM לא אמין לשחזר 14KB מקונן).
    source_sample: Optional[Dict[str, Any]] = None
    # ★ דריסות התסריט על מסר-הדוגמה: מפת "<path/leaf>": value (למשל {"category.coding.code":"M_PAT_HPV"}).
    source_overrides: Dict[str, Any] = Field(default_factory=dict)
    # ★ נתיב-המקור (לוגי, למשל "MessageHeader.id") שהופך ל-target KEY/entity_id/scc_message_id **verbatim**.
    # מחושב מ-transformations של ה-Payload Builder. ה-runner מזריק לתוכו ערך ייחודי → ה-KEY ביעד ייחודי
    # לכל ריצה → קורלציה מדויקת. עדיף על member_id (שעובר טרנספורמציה ולכן ה-uid המלא לא שורד ביעד).
    key_source_path: Optional[str] = None
