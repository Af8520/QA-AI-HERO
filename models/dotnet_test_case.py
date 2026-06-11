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

    match — פילטר לקליטת המסר הספציפי (תלוי key או field במסר).
    expected_fields — שדות שצריכים להופיע בערך הצפוי.
    """

    kind: Literal["kafka_wait"] = "kafka_wait"
    topic: str
    match: Dict[str, Any] = Field(default_factory=dict)
    expected_fields: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 30


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
