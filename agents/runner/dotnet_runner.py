"""DotNet Runner — מבצע DotNetExecutableTestCase: Kafka publish/wait + Couchbase wait.

תלוי ב-confluent-kafka + couchbase Python SDK. אם הם לא מותקנים (JFrog blocked),
ה-runner מחזיר BLOCKED עם הסבר ברור — לא קורס את הפייפליין.
"""

from __future__ import annotations

import asyncio
import copy
import datetime
import json
import re
import time
import uuid
from typing import Any, Dict, List, Optional

from config.logging_config import get_logger
from config.settings import settings
from models.dotnet_test_case import (
    CouchbaseWaitAction,
    DotNetExecutableTestCase,
    KafkaPublishAction,
    KafkaWaitAction,
)
from models.test_case import StepResult, TestCaseResult, TestStatus

log = get_logger(__name__)

# ★ token שה-compiler שם כערך ה-member_id; ה-runner מחליף אותו בערך ייחודי לכל ריצה,
# כדי שה-key ב-target topic יהיה ייחודי ולא יתנגש עם מסרים של טסטים אחרים/קודמים.
_UNIQUE_TOKEN = "__UNIQUE_ID__"

# ★ markers ב-source_overrides שמסמנים **מחיקת** שדה (לא דריסה) — לתרחיש שלילי "השמט ת"ז/שדה
# → ודא שהאובייקט לא נבנה ביעד". מסיר את השדה הספציפי בלבד (לא מרוקן מערכים שלמים).
_REMOVE_MARKERS = {"__REMOVE__", "__DELETE__", "__OMIT__"}

# ★ marker למוטציה-חלקית: "החלף רק את התו הראשון של הערך המקורי" (תרחיש "ספרה ראשונה=X" / "מתחיל ב-X",
# למשל ת"ז צה"ל). שומר את שאר הערך ואת אורכו מהדוגמה — דינמי לכל שדה, לא תלוי-ספק. פורמט: "__SET_FIRST_CHAR__:X".
_SET_FIRST_CHAR_PREFIX = "__SET_FIRST_CHAR__:"

# ★ marker ל-setup של concatenate: "ודא שלשדה-המקור (רשימה) יש ≥2 ערכים" — כך הטרנספורמציה מפיקה מפריד
# ביעד (organ עם ';'). אם הרשימה כבר ≥2 → no-op. דינמי לכל שדה-רשימה.
_ENSURE_MULTI_MARKER = "__ENSURE_MULTI__"

# sentinel ל-default arg של _filter_match (כדי לתמוך גם ב-pair בודד וגם ב-dict רב-מפתחי)
_SENTINEL = object()


def _israeli_id_check_digit(first8: str) -> str:
    """ספרת-הביקורת (הספרה ה-9) של ת"ז ישראלי לפי 8 הספרות הראשונות: משקלים 1,2,1,2... ;
    מכפלה דו-ספרתית → סכום ספרותיה; check = (10 - sum%10) % 10."""
    total = 0
    for i, ch in enumerate(first8):
        v = int(ch) * (1 if i % 2 == 0 else 2)
        total += v if v < 10 else v - 9
    return str((10 - (total % 10)) % 10)


def _gen_unique_member_id() -> str:
    """מפיק ת"ז ישראלי **תקין** (9 ספרות: 8 + ספרת-ביקורת) וייחודי לריצה. 8 הספרות הראשונות מבוססות-זמן
    (ייחודיות בין טסטים/ריצות שרצים שניות זה מזה), והספרה ה-9 מחושבת באלגוריתם ת"ז — כך שה-Worker (שמאמת
    ת"ז) יקבל את המסר ויפיק פלט ביעד, וה-KEY שנבנה ממנה יהיה ייחודי בכל ריצה."""
    first8 = str(int(time.time() * 1000) % 100_000_000).zfill(8)   # בדיוק 8 ספרות (עם אפסים מובילים)
    return first8 + _israeli_id_check_digit(first8)


def _substitute_token(obj: Any, token: str, value: str) -> Any:
    """מחליף רקורסיבית כל מופע של token (כ-substring) בכל המחרוזות בתוך dict/list/str."""
    if isinstance(obj, dict):
        return {k: _substitute_token(v, token, value) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute_token(v, token, value) for v in obj]
    if isinstance(obj, str):
        return obj.replace(token, value)
    return obj


def _contains_token(obj: Any, token: str) -> bool:
    """True אם token מופיע באיזושהי מחרוזת בתוך המבנה (dict/list/str)."""
    if isinstance(obj, dict):
        return any(_contains_token(v, token) for v in obj.values())
    if isinstance(obj, list):
        return any(_contains_token(v, token) for v in obj)
    if isinstance(obj, str):
        return token in obj
    return False


def _override_nested_field(obj: Any, name: str, uid: str) -> bool:
    """דורס *בכל מקום* (רקורסיבית) שדה dict בשם `name` לערך uid. format-agnostic:
    שם-השדה מגיע מ-key_built_from (member_id/entity_id/...). מחזיר True אם נמצא."""
    found = False
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == name:
                obj[k] = uid(v) if callable(uid) else uid
                found = True
            elif _override_nested_field(v, name, uid):
                found = True
    elif isinstance(obj, list):
        for item in obj:
            if _override_nested_field(item, name, uid):
                found = True
    return found


def _override_dotted_field(d: Dict[str, Any], name: str, uid: str) -> bool:
    """דורס ב-dict שטוח (dotted-path keys) כל key שהסגמנט האחרון שלו == `name` לערך uid.
    משמש ל-match/expected_fields של ה-wait. מחזיר True אם נמצא."""
    found = False
    for k in list(d.keys()):
        if k.split(".")[-1] == name:
            d[k] = uid
            found = True
    return found


def _split_path_segments(path: str) -> List[str]:
    """מפצל נתיב על '.' אבל **לא** בתוך סוגריים מרובעים — כך JSONPath filter כמו
    `identifier[?(@.system=='PID')]` (שמכיל '.') נשאר סגמנט אחד.
    'DiagnosticReport.category[0].coding[0].code' → ['DiagnosticReport','category[0]','coding[0]','code']."""
    return re.findall(r"(?:[^.\[]|\[[^\]]*\])+", str(path))


def _normalize_filter_position(path: str) -> str:
    """מתקן פילטר שה-Payload Builder שם במיקום שגוי — על שדה-עלה סקלרי במקום על ה-list שמכיל אותו.
    `Patient.identifier.value[system=PID]` → `Patient.identifier[system=PID].value`.
    מזיז רק אם הסגמנט האחרון הוא leaf גנרי/סקלרי (value/code/...) עם פילטר (לא אינדקס) — אחרת משאיר."""
    segs = _split_path_segments(path)
    if len(segs) < 2:
        return path
    m = re.match(r"^([^\[]+)(\[[^\]]*\])$", segs[-1])
    if not m:
        return path
    name, bracket = m.group(1), m.group(2)
    if name not in _GENERIC_LEAVES or "=" not in bracket:   # פילטר על leaf סקלרי בלבד (לא [0])
        return path
    segs[-1] = name
    segs[-2] = segs[-2] + bracket                            # מעבירים את הפילטר ל-list שלפני
    return ".".join(segs)


_FILTER_RE = re.compile(r"^\?\(@\.([^=!<>]+)\s*==\s*(.+)\)$")


def _parse_steps(path: str) -> List[Any]:
    """ממיר נתיב (dotted + bracket-index + filter) לרשימת steps:
    str → מפתח dict | int → אינדקס list | dict{k:v,...} → פילטר (בחר אלמנט ב-list שכל ה-pairs מתקיימים).
    תומך ב: a.b, a[0].b, a[?(@.system=='PID')].value (JSONPath), a[system=PID] / a[code=R,version=2]
    (תחביר ה-Payload Builder), a[*]."""
    steps: List[Any] = []
    for tok in re.findall(r"[^.\[\]]+|\[[^\]]*\]", str(path)):
        if tok.startswith("["):
            inner = tok[1:-1].strip()
            m = _FILTER_RE.match(inner)
            if m:                                                       # [?(@.system=='PID')]
                steps.append({m.group(1).strip(): m.group(2).strip().strip("'\"")})
            elif inner.lstrip("-").isdigit():                           # [0]
                steps.append(int(inner))
            elif inner in ("*", "?"):                                   # [*]
                steps.append("*")
            elif "=" in inner and "?(" not in inner:                    # [system=PID] / [code=R,version=2]
                filt: Dict[str, Any] = {}
                for pair in inner.split(","):
                    if "=" in pair:
                        k, val = pair.split("=", 1)
                        filt[k.strip().lstrip("@.")] = val.strip().strip("'\"")
                steps.append(filt or inner)
            else:
                steps.append(inner.strip("'\""))
        elif tok:
            steps.append(tok)
    return steps


def _read_by_path(obj: Any, path: str) -> Any:
    """קורא ערך לפי נתיב (dotted/bracket/index), read-only. _FIELD_MISSING אם לא נמצא.
    משמש להערכת מפתח-פילטר מקונן (`type.coding[0].code`) ב-JSONPath filter."""
    cur = obj
    for step in _parse_steps(path):
        if isinstance(step, dict):
            return _FIELD_MISSING
        if isinstance(step, int):
            if isinstance(cur, dict):
                continue
            if isinstance(cur, list) and -len(cur) <= step < len(cur):
                cur = cur[step]
                continue
            return _FIELD_MISSING
        if isinstance(cur, list):
            cur = cur[0] if cur else None
        if isinstance(cur, dict) and step in cur:
            cur = cur[step]
        else:
            return _FIELD_MISSING
    return cur


def _filter_match(elem: Any, filt: Any, fv: Any = _SENTINEL) -> bool:
    """True אם האלמנט תואם את הפילטר. שתי צורות קריאה:
    - _filter_match(elem, "system", "PID")  — pair בודד.
    - _filter_match(elem, {"system":"ICD","version":"2"})  — **כל** ה-pairs חייבים להתקיים (multi-key).
    תומך במפתח פשוט (system) ובמפתח מקונן (type.coding.code)."""
    if not isinstance(elem, dict):
        return False
    pairs = filt.items() if isinstance(filt, dict) else [(filt, fv)]
    for fk, want in pairs:
        actual = elem.get(fk, _FIELD_MISSING) if ("." not in str(fk) and "[" not in str(fk)) else _read_by_path(elem, fk)
        if actual is _FIELD_MISSING or str(actual) != str(want):
            return False
    return True


def _override_by_path(obj: Any, path: str, value: Any) -> bool:
    """דורס שדה **לפי נתיב מלא** — תומך ב-dotted, bracket-index `[0]`, ו-JSONPath filter
    `[?(@.system=='PID')]` (כולל מפתח מקונן). list עם סגמנט-שם (לא אינדקס/פילטר) → auto-index [0].
    דורס *רק* את השדה בנתיב (מונע דריסת leaf גנרי). מחזיר True אם נמצא ונדרס, False אחרת."""
    steps = _parse_steps(path)
    if not steps:
        return False
    cur = obj
    for i, step in enumerate(steps):
        last = i == len(steps) - 1
        if isinstance(step, dict):                       # פילטר על list (system=='PID', multi-key, וכו')
            if isinstance(cur, dict):                    # ★ object-vs-array: אובייקט בודד שתואם
                if _filter_match(cur, step):
                    continue
                return False
            if not isinstance(cur, list):
                return False
            cur = next((e for e in cur if _filter_match(e, step)), None)
            if cur is None:
                return False
            continue
        if isinstance(step, int):                        # אינדקס מפורש
            # ★ FHIR single-vs-array tolerance: אינדקס על אובייקט בודד (לא מערך) → האובייקט הוא האלמנט.
            # כך 'category.0.coding.0.code' עובד בין אם category הוא [{...}] ובין אם {...}.
            if isinstance(cur, dict):
                if last:
                    return False
                continue
            if not isinstance(cur, list) or not (-len(cur) <= step < len(cur)):
                return False
            if last:
                cur[step] = value(cur[step]) if callable(value) else value
                return True
            cur = cur[step]
            continue
        # step = str (מפתח). list → auto-index [0]
        if isinstance(cur, list):
            if step == "*":
                cur = cur[0] if cur else None
                if cur is None:
                    return False
                continue
            cur = cur[0] if cur else None
        if isinstance(cur, dict) and step in cur:
            if last:
                cur[step] = value(cur[step]) if callable(value) else value
                return True
            cur = cur[step]
        else:
            return False
    return False


# ★ שמות-שדה גנריים שאסור לדרוס לפי leaf בלבד (FHIR מלא בהם) — דורשים הקשר parent (suffix של 2+).
_GENERIC_LEAVES = {"value", "code", "id", "status", "name", "text", "system", "display",
                   "type", "url", "reference", "use", "version", "title", "key"}


def _override_path_anywhere(obj: Any, parts: List[str], value: Any) -> bool:
    """דורס את **המופע הראשון** ב-tree שבו הנתיב היחסי `parts` נפתר (החל מאותו node). מחזיר True
    אם נמצא ונדרס. ★ single-match בכוונה: ה-key_built_from של FHIR הוא נתיב לוגי וסיומת כמו
    'identifier.value' מופיעה ב-*כל* resource — דריסת כולם תשחית את המסר (request_num/institute/...
    היו הופכים ל-uid). דורסים אחד בלבד; ה-token (__UNIQUE_ID__) הוא המנגנון המדויק המועדף."""
    if _override_by_path(obj, ".".join(parts), value):
        return True
    if isinstance(obj, dict):
        for v in obj.values():
            if _override_path_anywhere(v, parts, value):
                return True
    elif isinstance(obj, list):
        for it in obj:
            if _override_path_anywhere(it, parts, value):
                return True
    return False


def _fhir_resources_of_type(bundle: Any, rtype: str) -> List[Dict[str, Any]]:
    """מחזיר את כל ה-dicts עם resourceType==rtype **בכל מקום** במבנה (FHIR) — רקורסיבי, לא רק
    ב-Bundle.entry[].resource ברמה העליונה. ★ קריטי: מסר Kafka/REST-Proxy עוטף לרוב את ה-Bundle
    ({'value': <bundle>} / payload / records[...]); חיפוש רק ברמה העליונה היה מחמיץ את ה-resource
    ואז ה-suffix-fallback היה כותב ל-resource הראשון התואם (Observation גם הוא בעל category!) במקום
    ל-DiagnosticReport — והערך נשאר M_PAT_HIST. רקורסיה מבטיחה שה-override נוחת ב-resource הנכון."""
    out: List[Dict[str, Any]] = []

    def walk(o: Any) -> None:
        if isinstance(o, dict):
            if o.get("resourceType") == rtype:
                out.append(o)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(bundle)
    return out


def _override_field_smart(obj: Any, path: str, value: Any) -> bool:
    """דריסת שדה format-agnostic ובטוחה מ-leaf גנרי. ה-paths מ-key_built_from/source_overrides
    הם לוגיים (ResourceType.field.subfield) ולא נתיבי-JSON ליטרליים, לכן:
    1. ★ ResourceType-aware: אם הסגמנט הראשון הוא resourceType ב-Bundle → מנווט ל-resource הנכון
       ומחיל את שאר הנתיב שם (כך 'DiagnosticReport.category[0].coding[0].code' פוגע ב-DiagnosticReport,
       לא ב-category הראשון האקראי — Observation/ServiceRequest יכולים גם הם להחזיק category).
    2. סיומות הולכות ומתקצרות (אורך 2+, parent.leaf) בכל מקום ב-tree.
    3. fallback ל-leaf בודד — **רק** לשם ספציפי (member_id/...), לא גנרי (value/code/id)."""
    path = _normalize_filter_position(path)      # identifier.value[system=PID] → identifier[system=PID].value
    parts = _split_path_segments(path)           # bracket-aware (לא שובר JSONPath filter)
    if not parts:
        return False
    # 1) ResourceType-aware
    rtype = re.sub(r"\[[^\]]*\]", "", parts[0])
    rest = ".".join(parts[1:])
    # ★ פילטר על ה-resource עצמו (PractitionerRole[code=R]) — דורסים **רק** ב-resource התואם, בלי
    # suffix-fallback (שיפגע ב-resource אחר, code=N). כך override על referral_practitioner לא נוחת ב-act.
    first_flt = next((s for s in _parse_steps(parts[0]) if isinstance(s, dict)), None)
    if rest and first_flt is not None:
        for res in (r for r in _fhir_resources_of_type(obj, rtype) if _filter_match(r, first_flt)):
            if _override_by_path(res, rest, value):
                return True
        return False
    resources = _fhir_resources_of_type(obj, rtype) if rest else []
    if resources:
        for res in resources:
            if _override_by_path(res, rest, value):
                return True
        # התאמת resourceType אך הנתיב לא נפתר באף resource → ננסה suffix כ-fallback
    # 2) סיומות
    for i in range(len(parts) - 1):
        sub = parts[i:]
        if len(sub) < 2:
            break
        if _override_path_anywhere(obj, sub, value):
            return True
    # 3) leaf בודד — שם-השדה האחרון (בלי ברקטים). רק לשם ספציפי (לא value/code גנרי).
    leaf = re.sub(r"\[[^\]]*\]", "", parts[-1])
    if leaf and leaf not in _GENERIC_LEAVES:
        return _override_nested_field(obj, leaf, value)
    return False


def _value_by_path(obj: Any, path: str) -> Any:
    """קריאת ערך לפי נתיב (dotted/bracket/filter) — read-only mirror של _override_by_path.
    _FIELD_MISSING אם לא נמצא. תומך ב-[0]/[?(@.k=='v')]/[k=v] ובסלחנות single-vs-array."""
    steps = _parse_steps(path)
    if not steps:
        return _FIELD_MISSING
    cur = obj
    for i, step in enumerate(steps):
        last = i == len(steps) - 1
        if isinstance(step, dict):
            if isinstance(cur, dict):
                if _filter_match(cur, step):
                    continue
                return _FIELD_MISSING
            if not isinstance(cur, list):
                return _FIELD_MISSING
            cur = next((e for e in cur if _filter_match(e, step)), None)
            if cur is None:
                return _FIELD_MISSING
            continue
        if isinstance(step, int):
            if isinstance(cur, dict):
                if last:
                    return _FIELD_MISSING
                continue
            if not isinstance(cur, list) or not (-len(cur) <= step < len(cur)):
                return _FIELD_MISSING
            cur = cur[step]
            continue
        if isinstance(cur, list):
            cur = cur[0] if cur else None
        if isinstance(cur, dict) and step in cur:
            cur = cur[step]
        else:
            return _FIELD_MISSING
    return cur


def _value_path_anywhere(obj: Any, parts: List[str]) -> Any:
    """מחזיר את הערך של המופע הראשון ב-tree שבו הנתיב היחסי `parts` נפתר. _FIELD_MISSING אם אין."""
    v = _value_by_path(obj, ".".join(parts))
    if v is not _FIELD_MISSING:
        return v
    if isinstance(obj, dict):
        for vv in obj.values():
            r = _value_path_anywhere(vv, parts)
            if r is not _FIELD_MISSING:
                return r
    elif isinstance(obj, list):
        for it in obj:
            r = _value_path_anywhere(it, parts)
            if r is not _FIELD_MISSING:
                return r
    return _FIELD_MISSING


def _read_field_smart(obj: Any, path: str) -> Any:
    """קריאה format-agnostic — mirror read-only של _override_field_smart (ResourceType-aware → suffix →
    leaf). מחזיר את הערך או _FIELD_MISSING. משמש לבדיקת קיום מקור בדוגמה (verify_all מדלג על שדה שמקורו חסר)."""
    path = _normalize_filter_position(path)
    parts = _split_path_segments(path)
    if not parts:
        return _FIELD_MISSING
    rtype = re.sub(r"\[[^\]]*\]", "", parts[0])
    rest = ".".join(parts[1:])
    # ★ פילטר על ה-resource עצמו (PractitionerRole[code=R]) — מחייבים התאמה ו**אין** suffix-fallback (אחרת
    # נתפוס resource אחר, code=N). כך 'יש רופא מפנה?' נענה נכון: אם אין code=R → _FIELD_MISSING.
    first_flt = next((s for s in _parse_steps(parts[0]) if isinstance(s, dict)), None)
    if rest:
        resources = _fhir_resources_of_type(obj, rtype)
        if first_flt is not None:
            for res in (r for r in resources if _filter_match(r, first_flt)):
                v = _value_by_path(res, rest)
                if v is not _FIELD_MISSING:
                    return v
            return _FIELD_MISSING
        for res in resources:
            v = _value_by_path(res, rest)
            if v is not _FIELD_MISSING:
                return v
    for i in range(len(parts) - 1):
        sub = parts[i:]
        if len(sub) < 2:
            break
        v = _value_path_anywhere(obj, sub)
        if v is not _FIELD_MISSING:
            return v
    leaf = re.sub(r"\[[^\]]*\]", "", parts[-1])
    if leaf and leaf not in _GENERIC_LEAVES:
        r = _get_nested_field(obj, leaf)
        if r is not None:
            return r
    return _FIELD_MISSING


def _ensure_filtered_resource(bundle: Any, src_path: str) -> Optional[str]:
    """★ ממיר resource קיים שהתסריט דורש בצורה אחרת: אם ל-src_path יש פילטר-resource בסגמנט הראשון
    (PractitionerRole[code=R]) שאינו תואם אף resource — **משנה את הדיסקרימינטור של resource קיים**
    מאותו סוג (code: N→R) **במקום**, לא מוסיף חדש. באפיון זה זה או/או (רופא מבצע *או* מפנה, לא שניהם) —
    בדיוק כמו override על כל שדה אחר. דינמי לכל סוג/פילטר — אפס hardcode. מחזיר תיאור (לוג) אם הומר, אחרת None.
    *לא* ממיר אם אין resource קיים, או אם כבר קיים resource תואם את הפילטר."""
    parts = _split_path_segments(src_path)
    if not parts:
        return None
    flt = next((s for s in _parse_steps(parts[0]) if isinstance(s, dict)), None)
    if not flt:                                       # אין פילטר-resource בסגמנט הראשון → לא רלוונטי
        return None
    rtype = re.sub(r"\[[^\]]*\]", "", parts[0])
    existing = _fhir_resources_of_type(bundle, rtype)
    if any(_filter_match(r, flt) for r in existing):  # כבר קיים תואם → אין מה להמיר
        return None
    if not existing:                                  # אין resource קיים להמיר
        return None
    target = existing[0]
    old = ",".join(f"{k}={target.get(k)}" for k in flt)
    for k, v in flt.items():                          # מחליף את הדיסקרימינטור in-place (code: N→R)
        target[k] = v
    flt_desc = ",".join(f"{k}={v}" for k, v in flt.items())
    return f"{rtype}[{old}]→[{flt_desc}]"


def _remove_by_path(obj: Any, path: str) -> bool:
    """מוחק שדה/אלמנט לפי נתיב (dotted/bracket/filter). מנווט ל-parent ומסיר את הסגמנט האחרון:
    מפתח dict → del; אינדקס/פילטר על list → הסרת אלמנט(ים). מחזיר True אם הוסר. לתרחישי 'השמט ת"ז'."""
    steps = _parse_steps(path)
    if not steps:
        return False
    cur = obj
    for step in steps[:-1]:                          # נווט ל-parent
        if isinstance(step, dict):
            if isinstance(cur, dict):
                if _filter_match(cur, step):
                    continue
                return False
            if not isinstance(cur, list):
                return False
            cur = next((e for e in cur if _filter_match(e, step)), None)
        elif isinstance(step, int):
            if isinstance(cur, dict):
                continue
            cur = cur[step] if isinstance(cur, list) and -len(cur) <= step < len(cur) else None
        else:
            if isinstance(cur, list):
                cur = cur[0] if cur else None
            cur = cur.get(step) if isinstance(cur, dict) else None
        if cur is None:
            return False
    last = steps[-1]
    if isinstance(last, dict):                       # פילטר → הסרת אלמנטים תואמים מ-list
        if isinstance(cur, list):
            before = len(cur)
            cur[:] = [e for e in cur if not _filter_match(e, last)]
            return len(cur) < before
        return False
    if isinstance(last, int):
        if isinstance(cur, list) and -len(cur) <= last < len(cur):
            del cur[last]
            return True
        return False
    if isinstance(cur, list):
        cur = cur[0] if cur else None
    if isinstance(cur, dict) and last in cur:
        del cur[last]
        return True
    return False


def _remove_path_anywhere(obj: Any, parts: List[str]) -> bool:
    """מסיר את המופע הראשון ב-tree שבו הנתיב היחסי `parts` נפתר."""
    if _remove_by_path(obj, ".".join(parts)):
        return True
    if isinstance(obj, dict):
        for v in obj.values():
            if _remove_path_anywhere(v, parts):
                return True
    elif isinstance(obj, list):
        for it in obj:
            if _remove_path_anywhere(it, parts):
                return True
    return False


def _remove_field_smart(obj: Any, path: str) -> bool:
    """מסיר שדה/אלמנט — ResourceType-aware + סיומת (כמו _override_field_smart, אך מחיקה).
    לתרחיש 'השמט ת"ז → לא לבנות אובייקט' (__REMOVE__ ב-source_overrides)."""
    path = _normalize_filter_position(path)
    parts = _split_path_segments(path)
    if not parts:
        return False
    rtype = re.sub(r"\[[^\]]*\]", "", parts[0])
    rest = ".".join(parts[1:])
    resources = _fhir_resources_of_type(obj, rtype) if rest else []
    if resources:
        for res in resources:
            if _remove_by_path(res, rest):
                return True
    for i in range(len(parts) - 1):
        sub = parts[i:]
        if len(sub) < 2:
            break
        if _remove_path_anywhere(obj, sub):
            return True
    return False


def _inject_source_id(value_obj: Any, id_path: Optional[str], id_name: str, uid: str) -> bool:
    """מזריק את ה-uid לשדה ה-id במסר המקור (format-agnostic, בטוח מ-leaf גנרי). id_path מ-key_built_from;
    אם אין — fallback ל-id_name (member_id/entity_id) לפי leaf, ובלבד שאינו שם גנרי."""
    if id_path:
        return _override_field_smart(value_obj, id_path, uid)
    if id_name and id_name not in _GENERIC_LEAVES:
        return _override_nested_field(value_obj, id_name, uid)
    return False


def _resolve_logical_holder(obj: Any, path: str):
    """מאתר את ה-dict שמחזיק את השדה האחרון של נתיב לוגי, + שם-השדה. תומך בשני סגנונות:
    1. FHIR: 'MessageHeader.id' → מאתר ב-Bundle.entry את ה-resource עם resourceType==MessageHeader,
       ואז מנווט לשדה (id). 2. dotted רגיל: 'root.entity_id' → ניווט ישיר.
    מחזיר (holder_dict, field_name) או (None, None)."""
    parts = path.split(".")
    # FHIR Bundle: הסגמנט הראשון הוא resourceType בתוך entry[].resource
    if isinstance(obj, dict) and isinstance(obj.get("entry"), list):
        for entry in obj["entry"]:
            res = entry.get("resource") if isinstance(entry, dict) else None
            if isinstance(res, dict) and res.get("resourceType") == parts[0]:
                holder = res
                for seg in parts[1:-1]:
                    nxt = holder.get(seg) if isinstance(holder, dict) else None
                    if isinstance(nxt, list):
                        nxt = nxt[0] if nxt else None
                    if not isinstance(nxt, dict):
                        holder = None
                        break
                    holder = nxt
                if isinstance(holder, dict):
                    return holder, parts[-1]
    # fallback: dotted path ישיר על obj
    holder = obj
    for seg in parts[:-1]:
        nxt = holder.get(seg) if isinstance(holder, dict) else None
        if isinstance(nxt, list):
            nxt = nxt[0] if nxt else None
        if not isinstance(nxt, dict):
            return None, None
        holder = nxt
    if isinstance(holder, dict) and parts[-1] in holder:
        return holder, parts[-1]
    return None, None


# ============================================================
# ★ עיגון דטרמיניסטי של transformations — parser חוקים + מיפוי שדה-לוגי
# ============================================================

# ★ מפריד-זוגות יכול להיות ',' **או** ';' (ה-Payload Builder משתמש ב-';': "A=1; B=2"). LHS/RHS לא כוללים מפריד.
_CODE_MAP_TOKEN = re.compile(r"\s*([^=,;→>]+?)\s*(?:==|=|->|→)\s*([^,;]+?)\s*(?:[,;]|$)")


def _detect_concat_sep(rule: str) -> str:
    """מזהה את תו-המפריד של concatenate מתוך טקסט-החוק: מפריד מצוטט ('...;...'), או 'separated by ;' /
    'join with ;' / 'delimiter ;', או תו-מפריד בודד שמופיע בחוק. ברירת-מחדל ';'."""
    m = re.search(r"(?:sep(?:arator)?|delimiter|with|by|מופרד[ים]*\s*ב|מפריד)\s*['\"]?([;,|/])['\"]?", rule, re.I)
    if m:
        return m.group(1)
    for ch in (";", "|", ","):
        if ch in rule:
            return ch
    return ";"


def _parse_transform_rule(rule: Any, src_path: Optional[str] = None) -> Dict[str, Any]:
    """מפענח חוק-טרנספורמציה (טקסט חופשי מה-Payload Builder) לתבנית בטוחה בלבד. מזהה רק תבניות ברורות;
    כל השאר → 'derived' (→ __PRESENT__ באימות, ללא רגרסיה). מחזיר {'kind': ..., ...params}:
    - code_map: 'A=1, B=2' / 'A→1' / 'A/B=1'. RHS סקלרי קצר בלבד.
    - verbatim: 'verbatim'/'copy'/'same'.
    - concatenate: שרשור של **שדה-מקור-יחיד (רשימה)** במפריד → {sep}. forward = sep.join(list).
    - concat_multi: ה-source_path הוא ביטוי רב-נתיבי ('a + b') → לא בר-דריסה, forward שביר → presence.
    - strip: 'strip leading zeros' → {what:'leading_zeros'}. forward = lstrip('0').
    - positional: 'first char/digit'/'split'/'ספרה ראשונה' → setup הוא set_first_char.
    - fixed: 'FIXED <const>' עם קבוע סקלרי → {value}. forward = const.
    src_path משמש להבחנת concatenate (מקור-יחיד) מ-concat_multi (ביטוי '+')."""
    if not isinstance(rule, str) or not rule.strip():
        return {"kind": "derived", "map": None}
    low = rule.strip().lower()
    if low in ("verbatim", "copy", "copy as-is", "as-is", "same", "passthrough", "pass-through"):
        return {"kind": "verbatim", "map": None}
    # code_map — מנסים ראשון (RHS סקלרי קצר)
    matches = _CODE_MAP_TOKEN.findall(rule)
    cmap: Dict[str, str] = {}
    ok = bool(matches)
    for lhs, rhs in matches:
        rhs = rhs.strip().strip("'\"")
        # RHS = ערך-יעד קצר (קוד/שם): מתירים עד 2 רווחים ("מעבדות חוץ") ו-'/' ("PAP/HPV"), אבל לא ביטוי/משפט.
        if not rhs or len(rhs) > 32 or rhs.count(" ") > 2 or "+" in rhs or "(" in rhs:
            ok = False
            break
        for token in str(lhs).split("/"):
            token = token.strip().strip("'\"")
            if token:
                cmap[token] = rhs
    if ok and cmap:
        return {"kind": "code_map", "map": cmap}
    # concat_multi — ה-source עצמו הוא ביטוי רב-נתיבי (family + given[0]) → לא בר-חישוב/דריסה
    if isinstance(src_path, str) and "+" in src_path:
        return {"kind": "concat_multi", "map": None}
    # concatenate — שדה-מקור-יחיד (רשימה) המשורשר במפריד
    if any(k in low for k in ("concat", "join", "separated", "delimit", "שרשור", "משורשר", "מופרד")):
        return {"kind": "concatenate", "sep": _detect_concat_sep(rule), "map": None}
    # strip leading zeros
    if ("zero" in low or "אפס" in low) and any(k in low for k in ("strip", "remove", "trim", "leading", "הסר", "מוביל")):
        return {"kind": "strip", "what": "leading_zeros", "map": None}
    # positional / split (ה-setup = set_first_char)
    if any(k in low for k in ("first char", "first digit", "split", "ספרה ראשונה", "תו ראשון", "קידומת")):
        return {"kind": "positional", "map": None}
    # fixed — קבוע סקלרי
    mfx = re.match(r"(?:fixed|const(?:ant)?|קבוע)\s*[:=]?\s*['\"]?([^\s'\"]{1,24})['\"]?$", rule.strip(), re.I)
    if mfx:
        return {"kind": "fixed", "value": mfx.group(1), "map": None}
    return {"kind": "derived", "map": None}


# ★ ה-Payload Builder מקודד "עוד target מאותו source" כ-'realpath__suffix' (code__name→examination_type_name,
# id__transaction→mac_transaction_id). זה **אינו** נתיב אמיתי במסר — זה אותו מקור בדיוק עם מיפוי-יעד אחר.
_SYNTH_SUFFIX_RE = re.compile(r"__[A-Za-z]\w*$")


def _strip_synthetic_suffix(path: Optional[str]) -> Optional[str]:
    """מסיר '__suffix' סינתטי מהסגמנט האחרון → הנתיב האמיתי. 'category[0].coding[0].code__name' →
    'category[0].coding[0].code'; 'MessageHeader.id__transaction' → 'MessageHeader.id'. נתיב רגיל ללא שינוי."""
    if not isinstance(path, str) or not path:
        return path
    segs = _split_path_segments(path)
    if segs:
        segs[-1] = _SYNTH_SUFFIX_RE.sub("", segs[-1])
    return ".".join(segs)


def _resolve_source_path(transform_index: Optional[Dict[str, Any]], ref: str) -> Optional[str]:
    """ממפה שדה-לוגי (target_field_path מלא או leaf) ל-source_path המדויק מה-transformations.
    by_target_path (מדויק) → by_target_leaf (None על collision) → None. **בלי ניחוש.**"""
    if not transform_index or not ref:
        return None
    bp = transform_index.get("by_target_path") or {}
    if ref in bp:
        return bp[ref]
    bl = transform_index.get("by_target_leaf") or {}
    return bl.get(str(ref).split(".")[-1])


def _canonical_target_path(transform_index: Optional[Dict[str, Any]], ref: str) -> str:
    """מחזיר את ה-target_field_path המלא לשדה-לוגי (אם ref הוא leaf — מוצא את הנתיב המלא). אחרת ref כפי-שהוא."""
    if not transform_index:
        return ref
    bp = transform_index.get("by_target_path") or {}
    if ref in bp:
        return ref
    for p in bp:
        if p.split(".")[-1] == ref:
            return p
    return ref


def _compute_expected(transform_index: Optional[Dict[str, Any]], target_field: str,
                      applied_overrides: Dict[str, Any], source_sample: Any = None) -> Any:
    """מחשב את ערך-היעד הצפוי לשדה — דטרמיניסטית, forward לפי סוג-החוק, מהערך-מקור ה**אפקטיבי**:
    override אם דרסנו, אחרת הערך מהדוגמה (`_read_field_smart`). כך גם תרחיש בלי דריסה מאומת מדויק.
    - code_map → map[src] (לא-במפה → __PRESENT__).   - verbatim → src.
    - concatenate(sep) → sep.join(list) (אם רשימת-סקלרים).  - strip(leading_zeros) → src.lstrip('0').
    - fixed → הקבוע.   - concat_multi/lookup/derived/positional → __PRESENT__ (לא ניתן לחשב מדויק בבטחה)."""
    rules = (transform_index or {}).get("rules") or {}
    tfp = _canonical_target_path(transform_index, target_field)
    rule = rules.get(tfp) or {}
    kind = rule.get("kind")
    src = _resolve_source_path(transform_index, target_field)
    base_src = _strip_synthetic_suffix(src) if src else None        # 'code__name' → 'code' (אותו מקור אמיתי)
    # ★ ערך-המקור האפקטיבי: override (על ה-src או על ה-base האמיתי), אחרת הערך מהדוגמה (מ-base). כך
    # examination_type_name (מקור סינתטי 'code__name') מחושב מאותו ערך-מקור כמו examination_type_code.
    ov = None
    for k in (src, base_src):
        if k and k in (applied_overrides or {}):
            ov = (applied_overrides or {})[k]
            break
    is_marker = isinstance(ov, str) and ov.startswith(("__REMOVE__", "__DELETE__", "__OMIT__",
                                                       _SET_FIRST_CHAR_PREFIX, _ENSURE_MULTI_MARKER))
    if ov is not None and not is_marker:
        eff = ov
    elif base_src and source_sample is not None:
        read = _read_field_smart(source_sample, base_src)
        eff = read if read is not _FIELD_MISSING else None
    else:
        eff = None

    if kind == "code_map" and eff is not None:
        mapped = (rule.get("map") or {}).get(str(eff))
        return mapped if mapped is not None else "__PRESENT__"
    if kind == "verbatim" and eff is not None:
        return eff
    if kind == "concatenate" and isinstance(eff, list) and eff and all(not isinstance(x, (dict, list)) for x in eff):
        return rule.get("sep", ";").join(str(x) for x in eff)
    if kind == "strip" and rule.get("what") == "leading_zeros" and isinstance(eff, (str, int)):
        return str(eff).lstrip("0") or "0"
    if kind == "fixed" and rule.get("value") is not None:
        return rule.get("value")
    return "__PRESENT__"


# ★ שדות שלעולם אינם בני-אימות שוויון: ה-KEY/זהות/metadata של ה-Worker (משתנים פר-ריצה/הודעה).
_NON_ASSERTABLE_LEAVES = {"scc_message_id", "entity_id", "message_id",
                          "mac_correlation_id", "mac_transaction_id", "timestamp_sequence"}
_MEMBER_LEAVES = {"member_id", "member_id_code"}


def _sanitize_expected_fields(expected: Dict[str, Any], strip_member: bool) -> List[str]:
    """מסיר מ-expected_fields שדות שאינם בני-אימות שוויון (KEY/זהות/metadata) — הם משתנים פר-ריצה
    (ה-uid הייחודי) או מטא-דאטה של ה-Worker. מחזיר את רשימת הנתיבים שהוסרו. משנה את ה-dict in-place.
    strip_member=True (מסלול מסר-דוגמה) מסיר גם member_id (עובר טרנספורמציה → לא אמין לאימות)."""
    drop = set(_NON_ASSERTABLE_LEAVES)
    if strip_member:
        drop |= _MEMBER_LEAVES
    removed: List[str] = []
    for k in list(expected.keys()):
        leaf = k.split(".")[-1]
        # ★ מסירים רק את השדות התנודתיים המפורשים (KEY/זהות/correlation/transaction) — **לא** כל mac_*:
        # שדות-Header קבועים (mac_producer_id/mac_sys_name/mac_channel...) הם בני-אימות שוויון ותסריט ה-Header
        # מאמת אותם במפורש. (mac_correlation_id/mac_transaction_id התנודתיים כבר ב-_NON_ASSERTABLE_LEAVES.)
        if leaf in drop:
            del expected[k]
            removed.append(k)
    return removed


def _make_key_unique(value_obj: Any, key_source_path: str, uid: str) -> Optional[str]:
    """מזריק ערך ייחודי לשדה-המקור שהופך ל-target KEY (verbatim) → ה-KEY ביעד ייחודי לכל ריצה.
    שומר על מבנה הערך: מחליף את **רצף הספרות הראשון** ב-uid (SCC-TST.128...→SCC-TST.<uid>...),
    כך שהסיומת (HISTO.final.0) והפורמט נשמרים. מחזיר את הערך החדש, או None אם השדה לא נמצא.

    ★ filter-aware: משתמש ב-_read_field_smart/_override_field_smart (שתומכים בפילטרים כמו
    `_data.identifier[type=NI].value`), ולא ב-_resolve_logical_holder הפשוט — כי לרוב שדה-המקור שבונה
    את ה-KEY הוא איבר במערך identifier לפי type (הת"ז), לא נתיב-נקודות פשוט."""
    cur = _read_field_smart(value_obj, key_source_path)
    if cur is _FIELD_MISSING or not isinstance(cur, (str, int)):
        return None
    s = str(cur)
    if uid in s:                       # כבר מכיל את ה-uid (ה-token כבר הוזרק כאן) → לא לדרוס שוב
        return s
    new = re.sub(r"\d+", uid, s, count=1) if re.search(r"\d", s) else f"{s}.{uid}"
    if _override_field_smart(value_obj, key_source_path, new):
        return new
    return None


def _get_nested_field(obj: Any, name: str) -> Optional[str]:
    """מחזיר את ערך השדה `name` הראשון (כ-str) במבנה מקונן, בלי לשנות. None אם אין."""
    if isinstance(obj, dict):
        if name in obj:
            return str(obj[name])
        for v in obj.values():
            r = _get_nested_field(v, name)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _get_nested_field(item, name)
            if r is not None:
                return r
    return None


def _primary_id_path(key_built_from: Optional[List[str]]) -> Optional[str]:
    """כמו _primary_id_field, אבל מחזיר את ה-**נתיב המלא** (לא רק ה-leaf) של ה-id הראשי.
    משמש להזרקת ה-uid לפי נתיב מדויק (מונע דריסת leaf גנרי כמו 'value' בכל ה-FHIR Bundle).
    דוגמה: ['ServiceRequest.identifier.value','DiagnosticReport.status'] → 'ServiceRequest.identifier.value'."""
    if not key_built_from:
        return None
    for path in key_built_from:
        seg = str(path).split(".")[-1]
        if seg and not (seg.endswith("_code") or seg == "code"):
            return str(path)
    return str(key_built_from[0]) or None


def _primary_id_field(key_built_from: Optional[List[str]]) -> Optional[str]:
    """מ-key_built_from (נתיבי-מקור שה-target KEY בנוי מהם) בוחר את השדה הראשי — הסגמנט האחרון
    של הנתיב הראשון שאינו *_code/code (הקוד הוא לא המזהה הייחודי). None → אין → fallback ל-member_id.
    דוגמה: ['_data.member_details.member_id','_data.member_details.member_id_code'] → 'member_id'.
    משמש ל-leaf-fallback ול-dotted-keys של match/expected_fields."""
    if not key_built_from:
        return None
    for path in key_built_from:
        seg = str(path).split(".")[-1]
        if seg and not (seg.endswith("_code") or seg == "code"):
            return seg
    return str(key_built_from[0]).split(".")[-1] or None


class DotNetRunner:
    name = "dotnet"

    def __init__(self) -> None:
        self._log_entries: List[Dict[str, Any]] = []

    def _log(self, action: str, status: str, message: str) -> None:
        """מוסיף רשומת log פר-action עם זמן אמיתי. ה-pipeline משדר/מתמיד."""
        self._log_entries.append({
            "ts": datetime.datetime.now().strftime("%H:%M:%S"),
            "action": action,
            "status": status,   # info | success | warn | error
            "message": message,
        })

    def _apply_source_sample(self, executable: DotNetExecutableTestCase) -> bool:
        """★ בונה את מסר ה-publish דטרמיניסטית מתוך מסר-הדוגמה האמיתי + דריסות התסריט.
        סיבה: ה-LLM לא אמין לשחזר מסר ענק (FHIR Bundle 14KB) עם הזרקה לשדה מקונן — הוא מחזיר את
        הדוגמה כפי-שהיא בלי הדריסות. לכן הרנר לוקח את הדוגמה כבסיס ומחיל את source_overrides בקוד.
        no-op (False) אם אין source_sample → המסלול הישן (value מה-LLM) ללא שינוי."""
        sample = executable.source_sample
        if not sample:
            return False
        pub = next((a for a in executable.actions if isinstance(a, KafkaPublishAction)), None)
        if pub is None:
            return False
        pub.value = copy.deepcopy(sample)
        overrides = executable.source_overrides or {}
        # ★★ המרת resource: אם התסריט דורש resource עם פילטר (PractitionerRole[code=R]) שאינו בדוגמה —
        # **משנים את הדיסקרימינטור של הקיים** (code: N→R) במקום, כי באפיון זה זה או/או. דורשים: overrides
        # שאינם מחיקה + verify שאינו absent (התסריטים ש"שולחים" את ה-resource). דינמי לכל סוג/פילטר.
        idx = executable.transform_index or {}
        spec = executable.verify_spec or {}
        required: set = {p for p, v in overrides.items()
                         if not (isinstance(v, str) and v.strip() in _REMOVE_MARKERS)}
        for vv in (spec.get("verify") or []):
            if not isinstance(vv, dict):
                continue
            exp = vv.get("expect")
            if exp in _ABSENT_MARKERS or exp == "absent":
                continue
            s = _resolve_source_path(idx, vv.get("target_field")) if vv.get("target_field") else None
            if s:
                required.add(s)
        for s in required:
            built = _ensure_filtered_resource(pub.value, s)
            if built:
                self._log("SOURCE", "info", f"הומר resource: {built} (התסריט דורש אותו והדוגמה הכילה גרסה אחרת — או/או)")
        # ★ אימות-החלה: משווים את המסר *לפני ואחרי* כל דריסה. הפונקציה יכולה להחזיר True בלי לשנות כלום
        # (למשל write ל-leaf שלא קיים, או value שכבר שווה) — מה ש*נראה* כהצלחה אך משאיר את הערך המקורי.
        # זה היה הבאג הסמוי: ה-source_path מהטרנספורמציות לא תאם את מבנה מסר-הדוגמה → הערך נשאר M_PAT_HIST.
        failed: List[str] = []
        for path, val in overrides.items():
            before = json.dumps(pub.value, ensure_ascii=False, sort_keys=True, default=str)
            # ★ __REMOVE__ → מחיקת השדה/האלמנט (לתרחיש שלילי "השמט ת"ז → לא לבנות אובייקט"),
            # ולא דריסה לערך ריק. מסיר רק את השדה הספציפי, לא מרוקן מערכים שלמים.
            is_remove = isinstance(val, str) and val.strip() in _REMOVE_MARKERS
            # ★ מוטציה-חלקית "ספרה ראשונה=X": לוקחים את הערך **המקורי מהדוגמה** ומחליפים רק את התו הראשון,
            # תוך שמירת שאר הספרות והאורך (ת"ז צה"ל = 10 ספרות, התו הראשון הוא הקוד). קריטי: לא לפברק ערך.
            is_set_first = isinstance(val, str) and val.startswith(_SET_FIRST_CHAR_PREFIX)
            # ★ __ENSURE_MULTI__: setup ל-concatenate — אם שדה-המקור (רשימה) קצר מ-2, מוסיף ערך שני (עותק
            # של הראשון) כך שהטרנספורמציה תפיק מפריד ביעד. אם כבר ≥2 → no-op מוצלח.
            is_ensure_multi = isinstance(val, str) and val == _ENSURE_MULTI_MARKER
            if is_set_first:
                ch = val[len(_SET_FIRST_CHAR_PREFIX):]
                fn = lambda old, _c=ch: (_c + str(old)[1:]) if (old is not None and str(old)) else _c
                ok = _override_field_smart(pub.value, path, fn)
                verb = f"מוטציה תו-ראשון→{ch}"
            elif is_ensure_multi:
                cur = _read_field_smart(pub.value, path)
                if isinstance(cur, list) and len(cur) >= 2:
                    ok = True                               # כבר רב-ערכי (no-op מוצלח)
                elif isinstance(cur, list) and len(cur) == 1:
                    cur.append(copy.deepcopy(cur[0]))       # מוסיף ערך שני (הרשימה היא reference במסר)
                    ok = True
                else:
                    ok = False
                verb = "ensure-multi (≥2 ערכים)"
            elif is_remove:
                ok = _remove_field_smart(pub.value, path)
                verb = "השמטה"
            else:
                ok = _override_field_smart(pub.value, path, val)
                verb = f"דריסה ={val}"
            changed = json.dumps(pub.value, ensure_ascii=False, sort_keys=True, default=str) != before
            if ok and changed:
                self._log("SOURCE", "info", f"{verb} {path} ✓")
            elif ok and not changed:
                self._log("SOURCE", "warn",
                          f"{verb} {path} — דווח הצלחה אך המסר לא השתנה (הערך כבר היה זהה, או נכתב למקום ריק)")
            else:
                failed.append(path)
                self._log("SOURCE", "error",
                          f"{verb} {path} — ❌ השדה לא נמצא במסר-הדוגמה! הערך המקורי נשאר. "
                          f"ה-source_path בטרנספורמציות אינו תואם את מבנה המסר שהעלית.")
        if failed:
            self._log("SOURCE", "error",
                      f"⚠ {len(failed)}/{len(overrides)} דריסות לא הוחלו (המסר נשלח עם הערכים המקוריים) — "
                      f"התרחיש ייכשל. נתיבים: {', '.join(failed)}")
        self._log("SOURCE", "success" if not failed else "warn",
                  f"בסיס publish ממסר-דוגמה אמיתי + {len(overrides) - len(failed)}/{len(overrides)} דריסות הוחלו")
        return True

    def _apply_verify_spec(self, executable: DotNetExecutableTestCase) -> None:
        """★ עיגון דטרמיניסטי של האימות: בונה expected_fields מ-verify_spec (לוגי, מה-LLM) + transform_index.
        verify_all_populated → __PRESENT__ לכל target_paths; verify[] → ערך מחושב (code_map)/__PRESENT__/__ABSENT__/literal.
        אחר כך _sanitize_expected_fields מסיר KEY/זהות/metadata. no-op אם אין verify_spec/transform_index."""
        spec = executable.verify_spec
        idx = executable.transform_index
        if not spec or not idx:
            return
        waits = [a for a in executable.actions if isinstance(a, KafkaWaitAction)]
        if not waits:
            return
        overrides = executable.source_overrides or {}
        # ★ ה-forward (ערך-יעד מדויק) ובדיקת מקור-קיים חייבים לקרוא את **המקור האפקטיבי שנשלח** — pub.value
        # *אחרי* _apply_source_sample (דריסות + setup כמו ENSURE_MULTI / המרת N→R), לא את הדוגמה המקורית.
        _pub = next((a for a in executable.actions if isinstance(a, KafkaPublishAction)), None)
        sample = (_pub.value if (_pub and _pub.value) else None) or executable.source_sample
        expected: Dict[str, Any] = {}
        absent_src: List[str] = []
        if spec.get("verify_all_populated"):
            for tp in idx.get("target_paths") or []:
                # ★ דלג על נתיב-יעד עם wildcard ('_data.*.practitioner_name') — אינו שדה ממשי בר-אימות.
                if "*" in str(tp):
                    continue
                # ★ דלג על שדה ש**מקורו אינו בדוגמה** (למשל referral_practitioner כשאין PractitionerRole code=R) —
                # ה-Worker לא יפיק אותו, ואסור להכשיל "ודא הכל מאוכלס" על שדה שכלל לא קיים בקלט. רק למקור
                # קונקרטי-יחיד (לא שרשור 'a + b' — כזה כן מופק מהדוגמה). דינמי לכל אפיון.
                src = _resolve_source_path(idx, tp)
                concrete = bool(src) and "+" not in src and " " not in src and ("." in src or "[" in src)
                if sample is not None and concrete and src not in overrides \
                        and _read_field_smart(sample, src) is _FIELD_MISSING:
                    absent_src.append(tp)
                    continue
                expected[tp] = "__PRESENT__"
        if absent_src:
            self._log("VERIFY", "info",
                      f"דולגו {len(absent_src)} שדות ב-verify_all (מקורם אינו במסר-הדוגמה → ה-Worker לא יפיק "
                      f"אותם): {', '.join(absent_src)}")
        skipped: List[str] = []
        for v in spec.get("verify") or []:
            if not isinstance(v, dict):
                continue
            tf = v.get("target_field")
            if not tf:
                continue
            key = _canonical_target_path(idx, tf)
            exp = v.get("expect")
            explicit = (exp in _ABSENT_MARKERS or exp == "absent"
                        or (exp not in (None, "", "auto", "compute", "present") and exp not in _PRESENT_MARKERS))
            # ★ דילוג על שדה לא-פתיר (leaf בודד לא-מוכר/דו-משמעי, כמו practitioner_id שקיים גם ב-act וגם
            #   ב-referral) — כדי לא לייצר כשל-שווא. רק לבדיקת נוכחות/חישוב; absent/literal מפורש נשמר.
            bp = idx.get("by_target_path") or {}
            bl = idx.get("by_target_leaf") or {}
            resolvable = key in bp or "." in str(key) or bool(bl.get(str(key)))
            if not explicit and not resolvable:
                skipped.append(str(tf))
                continue
            rule = (idx.get("rules") or {}).get(key)
            rkind = rule.get("kind") if rule else None
            # סוגים שאי-אפשר לחשב מהם ערך מדויק בבטחה → אימות נוכחות בלבד (לא לסמוך על literal של ה-LLM)
            noncomputable = rkind in (None, "derived", "concat_multi", "positional", "lookup")
            if exp in _ABSENT_MARKERS or exp == "absent":
                expected[key] = "__ABSENT__"
            elif exp in _PRESENT_MARKERS or exp == "present":
                expected[key] = "__PRESENT__"
            elif exp not in (None, "", "auto", "compute"):
                # יש literal מהתסריט. לשדה **בר-חישוב** (code_map/verbatim/concatenate/strip/fixed) מעדיפים את
                # הערך ה**מחושב** מהמקור האפקטיבי (מדויק) על ה-literal של ה-LLM (שעלול להיות ניחוש — organ);
                # לשדה לא-בר-חישוב (derived/concat_multi/lookup) — נוכחות בלבד (member_name='טסט טסט חדש').
                if noncomputable:
                    expected[key] = "__PRESENT__"
                else:
                    computed = _compute_expected(idx, tf, overrides, sample)
                    expected[key] = computed if computed != "__PRESENT__" else exp
            else:
                expected[key] = _compute_expected(idx, tf, overrides, sample)
        removed = _sanitize_expected_fields(expected, strip_member=bool(executable.source_sample))
        if skipped:
            self._log("VERIFY", "warn",
                      f"דולגו {len(skipped)} שדות-אימות לא-פתירים (לא ב-transformations / leaf דו-משמעי): "
                      f"{', '.join(skipped)} — אמת את האובייקט המלא במקום (למשל act_practitioner).")
        n_pre = sum(1 for x in expected.values() if x == "__PRESENT__")
        for w in waits:
            w.expected_fields = dict(expected)
        self._log("VERIFY", "info",
                  f"עוגן אימות דטרמיניסטי: {len(expected)} שדות ({n_pre} נוכחות), הוסרו {len(removed)} זהות/metadata")

    def _apply_unique_id(self, executable: DotNetExecutableTestCase) -> Optional[str]:
        """מזריק member_id ייחודי לריצה — *באותו ערך* במסר המקור, בקורלציה וב-expected_fields,
        כך שה-Worker מפיק key ייחודי ב-target ואנחנו תופסים בדיוק את המסר שלנו (וטסט שלילי לא
        תופס מסר של טסט קודם). דטרמיניסטי לחלוטין — דורס את כל שדות ה-member_id בקוד, בלי תלות
        בכך שה-LLM שם את ה-token __UNIQUE_ID__ במקום הנכון (הוא לא אמין בהזרקה לשדה מקונן במקור).
        no-op אם אין שדה member_id בכלל."""
        token = _UNIQUE_TOKEN
        # ★ format-agnostic: שם-שדה המזהה מ-key_built_from (entity_id/member_id/...); fallback member_id
        id_name = _primary_id_field(executable.key_built_from) or "member_id"
        # ★ נתיב מלא של ה-id (אם key_built_from סופק) — להזרקה לפי נתיב מדויק במקור, כדי לא לדרוס
        # leaf גנרי (FHIR מלא ב-`value`). None → fallback ל-leaf-override לפי id_name.
        id_path = _primary_id_path(executable.key_built_from)
        uid_target = _gen_unique_member_id()          # ה-form הנקי (ללא אפסים) — מה שה-Worker מפיק
        # ★ תלוי-בקשה: רק אם ה-id *בתסריט* מתחיל באפסים (התסריט בודק הסרת אפסים) — נשלח במקור עם
        # אפסים מובילים. אחרת id רגיל. מדלגים על הבדיקה ל-leaf גנרי (FHIR — לא רלוונטי, ולא אמין).
        orig_id = None
        if id_name not in _GENERIC_LEAVES:
            orig_id = next((_get_nested_field(a.value, id_name) for a in executable.actions
                            if isinstance(a, KafkaPublishAction)
                            and _get_nested_field(a.value, id_name) is not None), None)
        wants_leading_zeros = bool(orig_id) and len(orig_id) > 1 and orig_id[0] == "0"
        uid_source = uid_target.zfill(9) if wants_leading_zeros else uid_target
        src_set = False
        token_seen = False
        key_set = False
        new_key_val = None
        waits: List[KafkaWaitAction] = []
        for action in executable.actions:
            if isinstance(action, KafkaPublishAction):
                had_token = _contains_token(action.value, token)                    # ה-LLM שם __UNIQUE_ID__ בשדה
                if had_token:
                    token_seen = True
                action.value = _substitute_token(action.value, token, uid_source)   # token → uid (קודם — מונע uid כפול ב-KEY)
                # ★★★ ראשי: הזרקה לשדה-המקור שהופך ל-target KEY **verbatim** (scc_message_id/entity_id/
                # ת"ז ב-identifier). זה השדה שעובר ליעד ובונה את ה-KEY → ה-uid שורד שלם ב-KEY → קורלציה
                # מדויקת ו-KEY ייחודי בכל ריצה. פועל בכל מצב (template **וגם** sample) — קודם היה מוגבל
                # ל-source_sample, וזה גרם ל-KEY לחזור על ערך ה-template בין ריצות. no-op אם ה-token כבר הזריק.
                if executable.key_source_path:
                    nk = _make_key_unique(action.value, executable.key_source_path, uid_target)
                    if nk is not None:
                        key_set, new_key_val = True, nk
                # ★ הזרקת נתיב **רק כשאין token ולא הוזרק שדה KEY** — נמנעים מדריסת-יתר ומשדה מקור מיותר.
                if not had_token and not key_set and _inject_source_id(action.value, id_path, id_name, uid_source):
                    src_set = True
                if action.key and token in action.key:
                    action.key = action.key.replace(token, uid_source)
            elif isinstance(action, KafkaWaitAction):
                action.match = _substitute_token(action.match, token, uid_target)  # legacy MACKAF (member_id במ-match)
                # ★ expected: אם ה-LLM השאיר __UNIQUE_ID__ ב-assert (טעות — למשל ב-practitioner_id) →
                # אל תאמת את ה-uid של ה-runtime (אינו הערך האמיתי). המר ל-__PRESENT__ (אימות נוכחות).
                # שדה ה-id האמיתי (member_id) ידרס ל-uid ע"י _override_dotted_field למטה (לשם ספציפי).
                action.expected_fields = {k: ("__PRESENT__" if isinstance(v, str) and token in v else v)
                                          for k, v in (action.expected_fields or {}).items()}
                # ★ דריסת ה-id ב-match/expected רק לשם ספציפי — לשם גנרי (value/code) מסתמכים על
                # value_contains בלבד (לא דורסים שדה assert לגיטימי שמסתיים ב-'value').
                if id_name not in _GENERIC_LEAVES:
                    _override_dotted_field(action.match, id_name, uid_target)       # יעד (form נקי)
                    _override_dotted_field(action.expected_fields, id_name, uid_target)
                # ★ סינון אסרשנים שאינם בני-אימות: KEY/זהות/metadata. במסלול מסר-דוגמה גם member_id
                # (עובר טרנספורמציה/ייחודי). מונע כשל-שווא כשה-LLM מאמת שדות שלא נתבקשו עם ערך ישן.
                removed = _sanitize_expected_fields(action.expected_fields,
                                                    strip_member=bool(executable.source_sample))
                if removed:
                    self._log("ASSERT", "info",
                              f"הוסרו {len(removed)} אסרשנים לא-בני-אימות (KEY/זהות/metadata): {', '.join(removed)}")
                waits.append(action)
        # ★ קורלציה ראשית = value_contains: ה-uid הייחודי מופיע ב-target *בכל מקום* (KEY או גוף).
        # זה format-agnostic — ה-target KEY של FHIR הוא scc_message_id (לא ה-id שלנו), אבל ה-uid
        # יושב ב-_data.member_id בגוף → value_contains תופס בדיוק את המסר שלנו ולא מסר זר/ישן.
        # תרחיש שלילי: ה-uid לא הופק → אף מסר לא מכילו → timeout → PASS נכון.
        used = src_set or token_seen or key_set
        # legacy: token בתוך key_contains שה-LLM מילא
        for w in waits:
            if w.key_contains and token in (w.key_contains or ""):
                w.key_contains, used = uid_target, True
        if used:
            for w in waits:
                w.value_contains = uid_target          # ★ הקורלציה הייחודית (KEY או גוף)
            if key_set:
                self._log("UNIQUE", "success",
                          f"שדה ה-KEY ({executable.key_source_path}) הוזרק ערך ייחודי → KEY ביעד: "
                          f"{new_key_val} (קורלציה לפי ה-uid {uid_target})")
            elif wants_leading_zeros:
                self._log("UNIQUE", "info", f"{id_name}: מקור={uid_source} (עם אפסים מובילים — "
                                            f"בדיקת הסרה), יעד צפוי={uid_target} (ללא אפסים)")
            else:
                self._log("UNIQUE", "info", f"{id_name} ייחודי לריצה: {uid_target} (קורלציה: ה-id מופיע ב-target)")
        else:
            self._log("UNIQUE", "warn", "לא הוזרק id ייחודי (אין שדה KEY/id במקור / לא זוהה __UNIQUE_ID__) — "
                                        "הקורלציה תתבסס על match בלבד ועלולה לתפוס מסר זר. ודא שה-Payload Builder "
                                        "מחזיר transformation לשדה entity_id/scc_message_id.")
        return uid_target if used else None

    async def execute(self, executable: DotNetExecutableTestCase) -> TestCaseResult:
        """מבצע את רצף ה-actions אחד אחרי השני. status סופי הוא AND של כולם."""
        if not executable.actions:
            return TestCaseResult(
                test_case_id=executable.test_case_id,
                ado_test_case_id=executable.ado_test_case_id,
                status=TestStatus.BLOCKED,
                step_results=[],
                duration_seconds=0.0,
                api_response={"error": executable.compiler_notes or "no actions to execute"},
            )

        started = time.perf_counter()
        step_results: List[StepResult] = []
        observations: List[Dict[str, Any]] = []
        overall_status = TestStatus.PASSED
        error_message: Optional[str] = None
        # ★ run log — נצבר פר TC; ה-pipeline משדר אותו כ-log_line + מתמיד לדיסק.
        self._log_entries = []
        # ★ בסיס publish מ-source_sample + דריסות התסריט (דטרמיניסטי) — *לפני* ה-id, כי ה-id נדרס מעל.
        self._apply_source_sample(executable)
        # ★ עיגון דטרמיניסטי: בונה expected_fields מ-verify_spec + transform_index (לפני ה-id, כדי
        #   שסינון ה-id/metadata יחול). no-op אם אין verify_spec/transform_index (מסלול ישן).
        self._apply_verify_spec(executable)
        # ★ member_id ייחודי לריצה — מחליף __UNIQUE_ID__ ב-publish+wait (key/match/expected_fields)
        self._apply_unique_id(executable)

        def _record(step: StepResult, action, obs):
            nonlocal overall_status, error_message
            step_results.append(step)
            observations.append({"action": action.model_dump(), "observation": obs})
            if step.status == TestStatus.FAILED:
                overall_status = TestStatus.FAILED
                error_message = error_message or step.error_message
            elif step.status == TestStatus.BLOCKED and overall_status != TestStatus.FAILED:
                overall_status = TestStatus.BLOCKED
                error_message = error_message or step.error_message

        actions = executable.actions
        i = 0
        while i < len(actions):
            action = actions[i]
            nxt = actions[i + 1] if i + 1 < len(actions) else None
            # ★ צמד [publish, wait] ב-REST → warm-up: ה-publish רץ אחרי seek-to-end של ה-consumer
            if (isinstance(action, KafkaPublishAction) and isinstance(nxt, KafkaWaitAction)
                    and settings.kafka_rest_enabled):
                try:
                    pub_step, pub_obs, wait_step, wait_obs = await self._publish_then_wait(
                        action, nxt, executable.test_case_id)
                    _record(pub_step, action, pub_obs)
                    _record(wait_step, nxt, wait_obs)
                except Exception as e:
                    log.warning("dotnet_pair_exception", error=str(e))
                    self._log("ERROR", "error", f"חריגה ב-publish+wait: {str(e)[:200]}")
                    bad = StepResult(step="publish+wait", expected_result="completes",
                                     actual_result=f"Exception: {str(e)[:200]}",
                                     status=TestStatus.BLOCKED, error_message=str(e))
                    _record(bad, action, {"error": str(e)})
                i += 2
                continue

            try:
                if isinstance(action, KafkaPublishAction):
                    step, obs = await self._run_kafka_publish(action, executable.test_case_id)
                elif isinstance(action, KafkaWaitAction):
                    step, obs = await self._run_kafka_wait(action)
                elif isinstance(action, CouchbaseWaitAction):
                    step, obs = await self._run_couchbase_wait(action)
                else:
                    step = StepResult(
                        step=f"unknown action: {getattr(action, 'kind', '?')}",
                        expected_result="known action",
                        actual_result="skipped",
                        status=TestStatus.BLOCKED,
                        error_message="unknown action kind",
                    )
                    obs = {"skipped": True}
            except Exception as e:
                log.warning("dotnet_action_exception", kind=getattr(action, "kind", "?"), error=str(e))
                step = StepResult(
                    step=f"{getattr(action, 'kind', '?')}",
                    expected_result="action runs",
                    actual_result=f"Exception: {str(e)[:200]}",
                    status=TestStatus.BLOCKED,
                    error_message=str(e),
                )
                obs = {"error": str(e), "kind": getattr(action, "kind", "?")}

            _record(step, action, obs)
            i += 1

        duration = time.perf_counter() - started

        # api_response בפורמט generic — UI יודע לקרוא {actions, observations}
        api_response: Dict[str, Any] = {
            "status": 200 if overall_status == TestStatus.PASSED else 0,
            "kind": "dotnet",
            "observations": observations,
            "log": self._log_entries,
            "duration_ms": int(duration * 1000),
        }
        if error_message:
            api_response["error"] = error_message

        return TestCaseResult(
            test_case_id=executable.test_case_id,
            ado_test_case_id=executable.ado_test_case_id,
            status=overall_status,
            step_results=step_results,
            duration_seconds=duration,
            api_response=api_response,
        )

    # ============================================================
    # Kafka publish
    # ============================================================

    async def _run_kafka_publish(self, action: KafkaPublishAction, tc_id: str = ""):
        # ★ נרמול topic ל-lowercase (case-sensitive ב-Kafka; ACL בנוי על השם הקטן)
        action.topic = _normalize_topic(action.topic)
        # ★ נרמול ל-wire format: 'header' (יחיד) + שדות root משוטחים לרמה העליונה (בלי מעטפת 'root').
        # המסר האמיתי כך בנוי; בלי זה ה-Worker לא מפרסר את המסר שלנו ולא מפיק פלט.
        wired = _to_wire_message(action.value)
        if wired is not action.value:
            self._log("PUBLISH", "info", "נרמל מבנה ל-wire format (header יחיד + שיטוח root)")
            action.value = wired
        if not settings.kafka_enabled:
            return self._blocked_step(
                f"PUBLISH topic={action.topic}",
                "Kafka not configured (KAFKA_BOOTSTRAP_SERVERS / KAFKA_REST_PROXY_URL ריקים)",
            )

        # key ברירת מחדל: qa_ai_hero_<TC> — מאפשר זיהוי המסר בלוגים/אלסטיק
        key = action.key or f"qa_ai_hero_{_tc_key(tc_id)}"

        # ★ מסלול REST Proxy (מועדף כשמוגדר)
        if settings.kafka_rest_enabled:
            return await self._publish_via_rest(action, key)

        # מסלול native
        try:
            from confluent_kafka import Producer  # type: ignore[import-not-found]
        except ImportError:
            return self._blocked_step(
                f"PUBLISH topic={action.topic}",
                "confluent-kafka package not installed",
            )

        conf = self._kafka_conf()
        producer = Producer(conf)
        value_bytes = self._encode_value(action.value)
        key_bytes = key.encode("utf-8") if key else None
        headers = (
            [(k, v.encode("utf-8")) for k, v in action.headers.items()]
            if action.headers
            else None
        )

        delivery_result: Dict[str, Any] = {}

        def _on_delivery(err, msg):
            if err is not None:
                delivery_result["error"] = str(err)
            else:
                delivery_result["topic"] = msg.topic()
                delivery_result["partition"] = msg.partition()
                delivery_result["offset"] = msg.offset()

        producer.produce(
            topic=action.topic,
            value=value_bytes,
            key=key_bytes,
            headers=headers,
            on_delivery=_on_delivery,
        )
        # flush בעטיפת asyncio.to_thread כדי לא לחסום את ה-event loop
        await asyncio.to_thread(producer.flush, 10)

        if "error" in delivery_result:
            classified = _classify_kafka_error(delivery_result["error"], action.topic, "publish")
            delivery_result["classified"] = classified
            error_friendly = classified["friendly"]
            step = StepResult(
                step=f"PUBLISH topic={action.topic}",
                expected_result="delivered",
                actual_result=f"❌ {error_friendly}",
                status=TestStatus.FAILED,
                error_message=f"{error_friendly}\n→ {classified['recommendation']}",
            )
            return step, delivery_result

        step = StepResult(
            step=f"PUBLISH topic={action.topic}",
            expected_result="delivered",
            actual_result=f"offset={delivery_result.get('offset')}",
            status=TestStatus.PASSED,
            response_dump=delivery_result,
        )
        return step, delivery_result

    async def _publish_via_rest(self, action: KafkaPublishAction, key: str):
        """publish דרך Confluent REST Proxy. אותו shape של StepResult כמו ה-native path."""
        from agents.runner.kafka_rest_client import KafkaRestClient

        self._log("PUBLISH", "info", f"מפרסם ל-topic '{action.topic}' key={key}")
        client = KafkaRestClient()
        result = await client.produce(action.topic, key, action.value, action.headers)

        if "error" in result:
            classified = _classify_kafka_error(result["error"], action.topic, "publish")
            result["classified"] = classified
            self._log("PUBLISH", "error", f"נכשל: {classified['friendly']}")
            step = StepResult(
                step=f"PUBLISH topic={action.topic} (REST)",
                expected_result="delivered",
                actual_result=f"❌ {classified['friendly']}",
                # auth/ACL → BLOCKED (תשתית); שאר → FAILED
                status=TestStatus.BLOCKED if classified["is_fatal_infra"] else TestStatus.FAILED,
                error_message=f"{classified['friendly']}\n→ {classified['recommendation']}",
            )
            return step, result

        self._log("PUBLISH", "success",
                  f"נמסר ל-'{action.topic}' partition={result.get('partition')} offset={result.get('offset')}")
        step = StepResult(
            step=f"PUBLISH topic={action.topic} (REST) key={key}",
            expected_result="delivered",
            actual_result=f"offset={result.get('offset')} partition={result.get('partition')}",
            status=TestStatus.PASSED,
            response_dump=result,
        )
        return step, result

    async def _publish_then_wait(self, pub: KafkaPublishAction, wait: KafkaWaitAction, tc_id: str):
        """★ warm-up: ה-consumer נרשם ועושה seek-to-end, ורק *אחר כך* (ב-on_ready) ה-publish רץ.
        כך אנחנו ממוקמים על סוף ה-target לפני שה-Worker כותב → תופסים את המסר החדש (ולא ישנים).
        מחזיר (pub_step, pub_obs, wait_step, wait_obs).
        """
        pub_holder: Dict[str, Any] = {}

        async def on_ready():
            ts_ms = int(time.time() * 1000)
            pub_holder["step"], pub_holder["obs"] = await self._run_kafka_publish(pub, tc_id)
            return ts_ms

        wait_step, wait_obs = await self._run_kafka_wait(wait, on_ready=on_ready)

        # אם ה-consumer נכשל לפני ה-publish (on_ready לא רץ) — סמן placeholder ל-publish
        if "step" not in pub_holder:
            self._log("PUBLISH", "warn", "ה-publish לא רץ — הקמת ה-consumer נכשלה לפני seek-to-end")
            pub_step = StepResult(
                step=f"PUBLISH topic={pub.topic}", expected_result="delivered",
                actual_result="skipped — consumer setup failed before publish",
                status=TestStatus.BLOCKED, error_message="consumer setup failed before publish",
            )
            pub_obs = {"skipped": True}
        else:
            pub_step, pub_obs = pub_holder["step"], pub_holder["obs"]

        return pub_step, pub_obs, wait_step, wait_obs

    # ============================================================
    # Kafka wait
    # ============================================================

    async def _run_kafka_wait(self, action: KafkaWaitAction, on_ready=None):
        # ★ נרמול topic ל-lowercase (case-sensitive ב-Kafka; ACL בנוי על השם הקטן)
        action.topic = _normalize_topic(action.topic)
        if not settings.kafka_enabled:
            return self._blocked_step(
                f"WAIT topic={action.topic}",
                "Kafka not configured (KAFKA_BOOTSTRAP_SERVERS / KAFKA_REST_PROXY_URL ריקים)",
            )

        group = _resolve_consumer_group()
        corr = []
        if action.key_equals:
            corr.append(f"key={action.key_equals}")
        if action.key_contains:
            corr.append(f"key⊇{action.key_contains}")
        if action.value_contains:
            corr.append(f"uid⊇{action.value_contains} (ב-key או בגוף)")
        if action.match:
            corr.append(f"fields={json.dumps(action.match, ensure_ascii=False)}")
        # ★ רצפת timeout — ה-Worker אסינכרוני (עד דקה-שתיים). early-return כשנמצא match.
        effective_timeout = max(action.timeout_seconds, settings.KAFKA_WAIT_MIN_SECONDS)
        self._log("CONSUME", "info",
                  f"צורך מ-target '{action.topic}' group={group} (timeout {effective_timeout}s) "
                  f"correlation: {', '.join(corr) or '(אין!)'}")
        # ★ אזהרה: key_contains קצר/נפוץ (כמו "0") יתאים גם למסרים זרים (verifyhub)
        if action.key_contains is not None and len(str(action.key_contains).strip()) < 3 and not action.match:
            self._log("CONSUME", "warn",
                      f"key_contains='{action.key_contains}' קצר מדי — עלול להתאים למסרים זרים. "
                      f"מומלץ member_id ייחודי + match על entity_type.")

        candidates: List[Dict[str, Any]] = []
        # ★ מסלול REST Proxy (מועדף כשמוגדר)
        if settings.kafka_rest_enabled:
            from agents.runner.kafka_rest_client import KafkaRestClient
            rich = await KafkaRestClient().consume(
                action.topic, action.match, effective_timeout, group,
                key_equals=action.key_equals, key_contains=action.key_contains,
                value_contains=action.value_contains,
                on_ready=on_ready, skew_ms=settings.KAFKA_TIMESTAMP_SKEW_SECONDS * 1000,
            )
            if rich.get("rest_consumer_unavailable"):
                self._log("CONSUME", "error", "ה-consumer API של ה-REST Proxy לא זמין")
                return self._blocked_step(
                    f"WAIT topic={action.topic} (REST)",
                    "ה-consumer API של ה-REST Proxy לא זמין ({}). בקש מ-admin להפעיל אותו "
                    "(kafka-rest consumer endpoints), או הגדר KAFKA_BOOTSTRAP_SERVERS "
                    "למסלול native.".format(rich.get("detail", "404/501")),
                )
            if "fatal_error" in rich:
                observed: Any = rich  # נושא fatal_error → טופל בהמשך
            else:
                candidates = rich.get("candidates", []) or []
                observed = rich.get("matched")
                asg = rich.get("assign") or {}
                n_parts = asg.get("n_partitions", 0)
                mode = asg.get("mode", "?")
                if n_parts > 0:
                    # describe / configured / probe — consumer נפרד לכל partition = כיסוי מלא
                    self._log("CONSUME", "info",
                              f"assignment: {mode} — {n_parts} partitions (כיסוי מלא, consumer לכל partition)")
                else:
                    self._log("CONSUME", "error",
                              f"assignment: {mode} — נכשל ({asg.get('reason', '')})")
                # ★ כמה records כל partition החזיר בריצה החיה (seek-to-end) — מאתר delivery חסר
                lc = rich.get("live_counts") or {}
                if lc:
                    self._log("CONSUME", "info",
                              "live records לכל partition: " + json.dumps(lc, ensure_ascii=False))
                    # ★ אזהרה מכריעה: partition שקרא 0 חי בעוד אחר קרא הרבה = בעיית כיסוי (warm-up/seek),
                    #   לא בעיית תזמון. בדיוק מה שראינו (רק p3 קרא). התיקון: warm-up-retry + re-anchor.
                    zeros = [p for p, c in lc.items() if not c]
                    if zeros and any(c for c in lc.values()):
                        self._log("CONSUME", "warn",
                                  f"partitions שקראו 0 חי: {zeros} (בעוד אחרים קראו) — בעיית כיסוי seek/warm-up.")
                # ★ scan_meta פר-partition: מאיפה התחלנו לקרוא, HW משוער, וה-offset שהושג — מכריע *איפה*
                #   בדיוק נעצרה הסריקה (start מעל ה-tip? לא הגיע ל-HW? re-anchor הופעל?).
                sm = rich.get("scan_meta") or {}
                for p in sorted(sm.keys()):
                    m = sm[p]
                    extra = f" re-anchor→lookback={m['reanchor_lookback']}" if m.get("reanchor_lookback") else ""
                    self._log("scan", "info",
                              f"p{p} start={m.get('start')} (log_start={m.get('log_start')} "
                              f"hw≈{m.get('hw_est')} lookback={m.get('lookback')}) max_off={m.get('max_off')}{extra}")
                # ★ דיאגנוסטיקת כשל: מה ניתן לקרוא מכל partition (seek-to-beginning) —
                # מכריע בין "בעיית fetch צד-שרת" (partition מחזיר 0/שגיאה) ל-"בעיית תזמון".
                diag = rich.get("diag") or {}
                for p in sorted(diag.keys()):
                    d = diag[p]
                    sys_str = json.dumps(d.get("sys", {}), ensure_ascii=False)
                    self._log("diag", "info",
                              f"p{p} מההתחלה: status={d.get('status')} count={d.get('count')} "
                              f"has_key={d.get('has_key')} sys={sys_str}")
        else:
            try:
                from confluent_kafka import Consumer  # type: ignore[import-not-found]
            except ImportError:
                # native לא זמין — אם יש publish ממתין (on_ready), נריץ אותו כדי לא לדלג עליו
                if on_ready is not None:
                    await on_ready()
                return self._blocked_step(
                    f"WAIT topic={action.topic}",
                    "confluent-kafka package not installed",
                )
            # native אין לו seek-to-end warm-up — נפרסם לפני ה-consume (race נשאר; fallback בלבד)
            if on_ready is not None:
                await on_ready()
            conf = self._kafka_conf()
            conf.update({
                "group.id": group,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            })
            observed = await asyncio.to_thread(
                self._consume_until_match,
                Consumer, conf, action.topic, action.match, effective_timeout,
                action.key_equals, action.key_contains,
            )

        # ★ שגיאת תשתית/ACL → דווח מיד עם הסבר ידידותי
        if isinstance(observed, dict) and "fatal_error" in observed:
            classified = _classify_kafka_error(observed["fatal_error"], action.topic, "consume")
            observed["classified"] = classified
            self._log("CONSUME", "error", classified["friendly"])
            step = StepResult(
                step=f"WAIT topic={action.topic}",
                expected_result="message arrives",
                actual_result=f"❌ {classified['friendly']}",
                status=TestStatus.BLOCKED,
                error_message=f"{classified['friendly']}\n→ {classified['recommendation']}",
            )
            return step, observed

        # ★ logging של ה-candidates + breakdown לפי mac_sys_name — רואים מי כתב ל-target
        self._log("CONSUME", "info", f"נצפו {len(candidates)} מסרים ב-target topic")
        breakdown: Dict[str, int] = {}
        for c in candidates:
            sysname = _extract_sys_name(c.get("value_parsed"))
            breakdown[sysname] = breakdown.get(sysname, 0) + 1
        if breakdown:
            self._log("CONSUME", "info",
                      "breakdown לפי mac_sys_name: " + json.dumps(breakdown, ensure_ascii=False))
        # ★ אילו partitions באמת קראנו — מכריע בין "כיסוי חלקי" ל-"ה-Worker לא הפיק":
        # אם רואים מספר partitions אבל אפס encryption_child_development_worker → בעיית test-data.
        parts_seen = sorted({c.get("partition") for c in candidates if c.get("partition") is not None})
        if parts_seen:
            self._log("CONSUME", "info", f"partitions עם תעבורה (מתוך הנקראים): {parts_seen}")
        if "encryption_child_development_worker" not in breakdown and candidates:
            self._log("CONSUME", "warn",
                      "ה-Worker (encryption_child_development_worker) לא כתב אף מסר ל-target בחלון ההמתנה "
                      "— בדוק את ה-payload שפורסם מול חוקי הסינון (type_code / referral_date / member_id).")
        n_too_old = sum(1 for c in candidates if c.get("too_old"))
        if n_too_old:
            self._log("CONSUME", "info",
                      f"{n_too_old} מסרים נדחו ע\"י timestamp filter (ישנים מ-TC קודם, לפני ה-publish)")
        for c in candidates[:15]:
            sysname = _extract_sys_name(c.get("value_parsed"))
            old_mark = " ⏱too_old" if c.get("too_old") else ""
            self._log("candidate", "info",
                      f"p{c.get('partition')} offset={c.get('offset')} key={c.get('key')} "
                      f"mac_sys_name={sysname}{old_mark}")
        if len(candidates) == 0:
            self._log("CONSUME", "warn",
                      f"לא הגיע אף מסר ל-target תוך {effective_timeout}s — ייתכן שה-Worker איטי/לא הפיק "
                      f"פלט למסר שלנו (בדוק latency/תקינות ה-payload).")

        # תרחיש שלילי: timeout = PASS, מסר שהגיע = FAIL
        if action.expect_no_message:
            if observed is None:
                self._log("MATCH", "success", "לא הגיע מסר (תרחיש שלילי) — תקין")
                step = StepResult(
                    step=f"WAIT NO-MESSAGE topic={action.topic} ({action.timeout_seconds}s)",
                    expected_result="no message (negative test)",
                    actual_result="no message arrived — as expected",
                    status=TestStatus.PASSED,
                    response_dump={"timeout": True, "expected_silence": True, "candidates": candidates},
                )
                return step, {"timeout": True, "expected_silence": True, "candidates": candidates}
            self._log("MATCH", "error", "הגיע מסר למרות שזה תרחיש שלילי")
            step = StepResult(
                step=f"WAIT NO-MESSAGE topic={action.topic}",
                expected_result="no message (negative test)",
                actual_result=f"message arrived (offset={observed.get('offset')}) — should NOT have arrived",
                status=TestStatus.FAILED,
                error_message="unexpected message in negative test",
                response_dump={**observed, "candidates": candidates},
            )
            return step, {**observed, "candidates": candidates}

        # ★ אין שום matcher (לא key ולא value) → לא ניתן לקבוע איזה מסר הוא שלנו → inconclusive
        if (not action.match and not action.key_equals and not action.key_contains
                and not action.value_contains):
            self._log("MATCH", "warn",
                      f"אין correlation (key/match) — לא ניתן לזהות איזה מ-{len(candidates)} המסרים הוא התגובה שלנו. "
                      f"בחר correlation מהלוגים והגדר אותו.")
            step = StepResult(
                step=f"WAIT topic={action.topic}",
                expected_result="message matched",
                actual_result=f"⚠ צרכנו {len(candidates)} מסרים אך אין correlation match",
                status=TestStatus.BLOCKED,
                error_message=f"match ריק — צרכנו {len(candidates)} מסרים מ-target. "
                              f"הגדר correlation field (ראה לוגים) כדי לזהות את התגובה שלנו.",
            )
            return step, {"inconclusive": True, "candidates": candidates, "match": {}}

        if observed is None:
            corr_desc = json.dumps(action.match, ensure_ascii=False)
            if action.value_contains:
                corr_desc = f"uid={action.value_contains} (ב-key/גוף) + {corr_desc}"
            self._log("MATCH", "error",
                      f"לא נמצא מסר תואם ל-{corr_desc} מתוך {len(candidates)} מסרים. "
                      f"אם ה-uid לא הופק ב-target — בדוק שה-__UNIQUE_ID__ הוזרק בשדה ה-id הנכון במקור "
                      f"(\"פתח פרטים\" → 📤 נשלח ל-source).")
            # ★ רמז כיסוי: אם ה-KEY שלנו קיים ביעד (המשתמש רואה אותו) אך לא נמצא — כנראה בעיית כיסוי:
            #   ה-partition שלו לא נסרק. בדוק שמספר ה-partitions הנקראים = המספר האמיתי של ה-topic.
            _asg = (rich.get("assign") if isinstance(rich, dict) else None) or {}
            self._log("CONSUME", "warn",
                      f"נסרקו {_asg.get('n_partitions', '?')} partitions (mode={_asg.get('mode', '?')}). "
                      f"אם אתה רואה את ה-KEY ביעד ב-Kafka אך הוא לא נמצא — ודא שכל ה-partitions של ה-topic "
                      f"נסרקים (KAFKA_TARGET_PARTITIONS = מספר ה-partitions המדויק) וקרא את שורות ה-scan/live.")
            step = StepResult(
                step=f"WAIT topic={action.topic} (timeout {action.timeout_seconds}s)",
                expected_result="message arrived matching " + json.dumps(action.match, ensure_ascii=False),
                actual_result=f"timeout — no matching message (saw {len(candidates)})",
                status=TestStatus.FAILED,
                error_message="message did not arrive within timeout",
            )
            return step, {"timeout": True, "match": action.match, "candidates": candidates}

        # אסרשנים על שדות צפויים
        missing = _check_expected_fields(observed.get("value_parsed") or {}, action.expected_fields)
        if missing:
            self._log("ASSERT", "error", "שדות חסרים/לא תואמים: " + ", ".join(missing))
            step = StepResult(
                step=f"WAIT topic={action.topic}",
                expected_result=json.dumps(action.expected_fields, ensure_ascii=False),
                actual_result=json.dumps(observed.get("value_parsed") or {}, ensure_ascii=False)[:300],
                status=TestStatus.FAILED,
                error_message="missing/mismatched fields: " + ", ".join(missing),
                response_dump={**observed, "candidates": candidates},
            )
            return step, {**observed, "candidates": candidates}

        # ★ שקיפות: מציגים **מה** אומת (שדה=ערך-צפוי) גם במעבר — כדי שיהיה ברור על מה נרשם pass
        # (לא רק "תקין"). מסננים markers/נוכחות לתצוגה קריאה.
        ef = action.expected_fields or {}
        if ef:
            shown = []
            for k, v in ef.items():
                leaf = k.split(".")[-1]
                if isinstance(v, str) and v in _PRESENT_MARKERS:
                    shown.append(f"{leaf}=⟨קיים⟩")
                elif isinstance(v, str) and v in _ABSENT_MARKERS:
                    shown.append(f"{leaf}=⟨נעדר⟩")
                else:
                    shown.append(f"{leaf}={v}")
            self._log("VERIFY", "success", f"אומתו {len(ef)} שדות ביעד: " + ", ".join(shown))
        self._log("MATCH", "success", f"נמצא מסר תואם offset={observed.get('offset')} + שדות תקינים")
        step = StepResult(
            step=f"WAIT topic={action.topic}",
            expected_result="message matched + fields ok",
            actual_result=f"offset={observed.get('offset')} fields_ok",
            status=TestStatus.PASSED,
            response_dump={**observed, "candidates": candidates},
        )
        return step, {**observed, "candidates": candidates}

    @staticmethod
    def _consume_until_match(
        Consumer,
        conf: Dict[str, Any],
        topic: str,
        match: Dict[str, Any],
        timeout_seconds: int,
        key_equals: Optional[str] = None,
        key_contains: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """סינכרוני — רץ ב-thread. polling עד שמתאים או timeout.

        אם נתקלים בשגיאת auth/ACL — מחזירים dict עם 'fatal_error' במקום לחזור על
        השגיאה עד timeout. הרץ יזהה ויעצור את שאר ה-TCs.
        """
        consumer = Consumer(conf)
        try:
            consumer.subscribe([topic])
            deadline = time.monotonic() + timeout_seconds
            while time.monotonic() < deadline:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    err_str = str(msg.error())
                    # אם זו שגיאת תשתית fatal — להחזיר מיד עם הסימן
                    if any(code in err_str for code in FATAL_INFRA_ERROR_CODES):
                        return {"fatal_error": err_str}
                    continue
                raw_value = msg.value()
                try:
                    parsed = json.loads(raw_value.decode("utf-8")) if raw_value else None
                except Exception:
                    parsed = None
                msg_key = msg.key()
                key_str = msg_key.decode("utf-8", errors="replace") if isinstance(msg_key, bytes) else (msg_key or None)
                if key_equals is not None and (key_str or "") != key_equals:
                    continue
                if key_contains is not None and key_contains not in (key_str or ""):
                    continue
                if not _matches(parsed, match):
                    continue
                return {
                    "offset": msg.offset(),
                    "partition": msg.partition(),
                    "topic": msg.topic(),
                    "value_parsed": parsed,
                    "value_raw": raw_value.decode("utf-8", errors="replace") if raw_value else None,
                    "timestamp": msg.timestamp(),
                }
            return None
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    # ============================================================
    # Couchbase wait
    # ============================================================

    async def _run_couchbase_wait(self, action: CouchbaseWaitAction):
        if not settings.couchbase_enabled:
            return self._blocked_step(
                f"COUCHBASE bucket={action.bucket}",
                "Couchbase not configured (COUCHBASE_CONNECTION_STRING empty)",
            )

        try:
            from couchbase.auth import PasswordAuthenticator  # type: ignore[import-not-found]
            from couchbase.cluster import Cluster  # type: ignore[import-not-found]
            from couchbase.options import ClusterOptions  # type: ignore[import-not-found]
        except ImportError:
            return self._blocked_step(
                f"COUCHBASE bucket={action.bucket}",
                "couchbase package not installed",
            )

        observed = await asyncio.to_thread(
            self._poll_couchbase,
            Cluster, ClusterOptions, PasswordAuthenticator, action,
        )

        if observed is None:
            step = StepResult(
                step=f"COUCHBASE bucket={action.bucket} key={action.key} (timeout {action.timeout_seconds}s)",
                expected_result="document exists",
                actual_result="timeout — no document",
                status=TestStatus.FAILED,
                error_message="document did not appear within timeout",
            )
            return step, {"timeout": True}

        missing = _check_expected_fields(observed.get("doc") or {}, action.expected_fields)
        if missing:
            step = StepResult(
                step=f"COUCHBASE bucket={action.bucket} key={action.key}",
                expected_result=json.dumps(action.expected_fields, ensure_ascii=False),
                actual_result=json.dumps(observed.get("doc") or {}, ensure_ascii=False)[:300],
                status=TestStatus.FAILED,
                error_message="missing/mismatched fields: " + ", ".join(missing),
                response_dump=observed,
            )
            return step, observed

        step = StepResult(
            step=f"COUCHBASE bucket={action.bucket} key={action.key}",
            expected_result="doc exists + fields ok",
            actual_result="ok",
            status=TestStatus.PASSED,
            response_dump=observed,
        )
        return step, observed

    @staticmethod
    def _poll_couchbase(Cluster, ClusterOptions, PasswordAuthenticator, action: CouchbaseWaitAction):
        """סינכרוני — רץ ב-thread."""
        auth = PasswordAuthenticator(settings.COUCHBASE_USERNAME or "", settings.COUCHBASE_PASSWORD or "")
        cluster = Cluster(settings.COUCHBASE_CONNECTION_STRING, ClusterOptions(auth))
        try:
            bucket = cluster.bucket(action.bucket)
            if action.scope and action.collection:
                coll = bucket.scope(action.scope).collection(action.collection)
            else:
                coll = bucket.default_collection()
            deadline = time.monotonic() + action.timeout_seconds
            last_error = None
            while time.monotonic() < deadline:
                if action.key:
                    try:
                        result = coll.get(action.key)
                        doc = result.content_as[dict]
                        return {"key": action.key, "doc": doc}
                    except Exception as e:
                        last_error = str(e)
                        time.sleep(1.0)
                        continue
                if action.query:
                    try:
                        rows = list(cluster.query(action.query))
                        if rows:
                            return {"query": action.query, "doc": rows[0]}
                    except Exception as e:
                        last_error = str(e)
                    time.sleep(1.0)
            return None
        finally:
            try:
                cluster.close()
            except Exception:
                pass

    # ============================================================
    # Helpers
    # ============================================================

    @staticmethod
    def _kafka_conf() -> Dict[str, Any]:
        conf: Dict[str, Any] = {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "security.protocol": settings.KAFKA_SECURITY_PROTOCOL,
        }
        if settings.KAFKA_SECURITY_PROTOCOL.startswith("SASL"):
            conf["sasl.mechanism"] = settings.KAFKA_SASL_MECHANISM
            if settings.KAFKA_SASL_USERNAME:
                conf["sasl.username"] = settings.KAFKA_SASL_USERNAME
            if settings.KAFKA_SASL_PASSWORD:
                conf["sasl.password"] = settings.KAFKA_SASL_PASSWORD
        return conf

    @staticmethod
    def _encode_value(value: Any) -> bytes:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False).encode("utf-8")
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")

    @staticmethod
    def _blocked_step(label: str, reason: str):
        step = StepResult(
            step=label,
            expected_result="action runs",
            actual_result=f"BLOCKED: {reason}",
            status=TestStatus.BLOCKED,
            error_message=reason,
        )
        return step, {"blocked": True, "reason": reason}

    # ============================================================
    # Verify (no-op — Kafka/Couchbase verified inside actions)
    # ============================================================
    async def verify_kafka(self, executable) -> Dict[str, Any]:
        return {"skipped": True, "reason": "verification embedded in actions"}

    async def verify_elastic(self, executable) -> Dict[str, Any]:
        return {"skipped": True, "reason": "verification embedded in actions"}


# ============================================================
# Pure helpers (testable without confluent-kafka)
# ============================================================

def _extract_sys_name(value: Any) -> str:
    """מחלץ mac_sys_name מ-value (top-level / header / headers) — לזיהוי איזה worker כתב."""
    if not isinstance(value, dict):
        return "?"
    for container in (value, value.get("header"), value.get("headers")):
        if isinstance(container, dict) and container.get("mac_sys_name"):
            return str(container["mac_sys_name"])
    return "?"


def _parse_group_from_error(err_str: str) -> Optional[str]:
    """מחלץ את שם ה-group מהודעת REST proxy: 'Not authorized to access group: X'."""
    m = re.search(r"access group:\s*([^\"'\}\s]+)", err_str or "", re.IGNORECASE)
    return m.group(1) if m else None


def _resolve_consumer_group() -> str:
    """שם ה-consumer group: אם KAFKA_CONSUMER_GROUP מוגדר → verbatim (ACL literal).
    אחרת PREFIX + suffix אקראי (לסביבות עם prefix ACL או בלי group ACL).
    """
    if settings.KAFKA_CONSUMER_GROUP:
        return settings.KAFKA_CONSUMER_GROUP
    return f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}-{uuid.uuid4().hex[:8]}"


def _to_wire_message(value: Any) -> Any:
    """ממיר מבנה לוגי (headers/root/_data) למבנה ה-wire האמיתי של ההודעה:
    - 'header' (יחיד) במקום 'headers'
    - שדות ה-root משוטחים לרמה העליונה (בלי מעטפת 'root')
    - '_data' נשאר כפי שהוא
    אידמפוטנטי: אם אין 'root' ואין 'headers' — מחזיר את אותו אובייקט בדיוק (no-op).
    """
    if not isinstance(value, dict):
        return value
    if "root" not in value and "headers" not in value:
        return value  # כבר wire (או אין מה להמיר)
    out: Dict[str, Any] = {}
    header = value.get("header", value.get("headers"))
    if header is not None:
        out["header"] = header
    root = value.get("root")
    if isinstance(root, dict):
        out.update(root)  # שיטוח שדות ה-root לרמה העליונה
    for k, v in value.items():
        if k in ("header", "headers", "root", "_data"):
            continue
        out[k] = v
    if "_data" in value:
        out["_data"] = value["_data"]
    return out


def _normalize_topic(topic: str) -> str:
    """שמות topics ב-Kafka הם case-sensitive, ובמכבי הקונבנציה היא תמיד אותיות קטנות.
    ה-Payload Builder לפעמים מחזיר אותיות גדולות (Clicks-referral-streaming) → 403/אין ACL.
    מנרמלים גורף ל-lowercase.
    """
    return (topic or "").strip().lower()


def _tc_key(tc_id: str) -> str:
    """מנקה test_case_id ל-key תקני של Kafka (TC-01 מתוך 'TC-01: ...')."""
    if not tc_id:
        return "unknown"
    m = re.search(r"(TC[\s\-_]*\d+)", tc_id, re.IGNORECASE)
    if m:
        return re.sub(r"[\s_]", "-", m.group(1))
    # fallback — אלפאנומרי בלבד, מוגבל ל-32 תווים
    cleaned = re.sub(r"[^A-Za-z0-9\-]", "_", tc_id)
    return cleaned[:32] or "unknown"


def _matches(value: Optional[Dict[str, Any]], match: Dict[str, Any]) -> bool:
    """True אם value מכיל את כל ה-key:value-pairs ב-match."""
    if not match:
        return True
    if not isinstance(value, dict):
        return False
    for k, expected in match.items():
        if k not in value:
            return False
        if value[k] != expected:
            return False
    return True


_FIELD_MISSING = object()


def _resolve_raw_path(obj: Any, path: str) -> Any:
    """מחלץ ערך לפי dotted path עם list index. _FIELD_MISSING אם לא קיים."""
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            if part in cur:
                cur = cur[part]
            else:
                return _FIELD_MISSING
        elif isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError:
                return _FIELD_MISSING
            if -len(cur) <= idx < len(cur):
                cur = cur[idx]
            else:
                return _FIELD_MISSING
        else:
            return _FIELD_MISSING
    return cur


def _resolve_raw_path_autolist(obj: Any, path: str) -> Any:
    """כמו _resolve_raw_path, אבל אם segment נוחת על list וה-part הבא אינו index מספרי —
    צולל אוטומטית ל-[0]. כך '_data.parameters.member_id' פותר ל-'_data.parameters.0.member_id'
    (ה-LLM נוטה להשמיט את ה-index). _FIELD_MISSING אם לא קיים."""
    cur = obj
    for part in path.split("."):
        if isinstance(cur, list):
            is_index = True
            try:
                int(part)
            except ValueError:
                is_index = False
            if not is_index:          # list + שם-שדה → auto-index ל-[0] ואז המשך
                if not cur:
                    return _FIELD_MISSING
                cur = cur[0]
        if isinstance(cur, dict):
            if part in cur:
                cur = cur[part]
            else:
                return _FIELD_MISSING
        elif isinstance(cur, list):
            try:
                idx = int(part)
            except ValueError:
                return _FIELD_MISSING
            if -len(cur) <= idx < len(cur):
                cur = cur[idx]
            else:
                return _FIELD_MISSING
        else:
            return _FIELD_MISSING
    return cur


def _collect_by_leaf_name(obj: Any, name: str, out: List[Any]) -> None:
    """אוסף את *כל* הערכים של dict-keys ששמם == name, בכל עומק (לתוך out)."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == name:
                out.append(v)
            else:
                _collect_by_leaf_name(v, name, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_by_leaf_name(item, name, out)


def _find_by_leaf_name(obj: Any, name: str) -> Any:
    """fallback אחרון כשה-LLM שם נתיב שגוי — מוצא שדה לפי שמו. ★ רק כשהשם **חד-משמעי** (מופע אחד
    ב-tree): אם הוא מופיע ביותר ממקום אחד (למשל `practitioner_id` תחת referral_practitioner *וגם*
    act_practitioner) — מחזיר _FIELD_MISSING, כדי לא לפתור 'referral_practitioner.practitioner_id'
    בטעות לערך של act_practitioner (false-pass). _FIELD_MISSING אם אין/דו-משמעי."""
    found: List[Any] = []
    _collect_by_leaf_name(obj, name, found)
    return found[0] if len(found) == 1 else _FIELD_MISSING


def _resolve_field_path(obj: Any, path: str) -> Any:
    """כמו _resolve_raw_path, אבל סובלני להבדל logical↔wire, ל-list ללא index, ולנתיב שגוי:
    - 'root.X' שלא נמצא → ננסה 'X' ברמה העליונה (כי root משוטח ב-wire).
    - 'headers.X' שלא נמצא → ננסה 'header.X' (header יחיד ב-wire).
    - 'a.list.field' (list ללא index) → auto-index ל-[0].
    - נתיב שלא נמצא בכלל → fallback גורף לפי שם-השדה האחרון בכל מקום ב-tree.
    ככה אסרשנים עובדים גם כשה-LLM טועה בנתיב המדויק.
    """
    val = _resolve_raw_path(obj, path)
    if val is _FIELD_MISSING and path.startswith("root."):
        val = _resolve_raw_path(obj, path[len("root."):])
    if val is _FIELD_MISSING and path.startswith("headers."):
        val = _resolve_raw_path(obj, "header." + path[len("headers."):])
    # ★ סלחנות list — auto-index ל-[0] (וגם בשילוב עם root./headers.)
    if val is _FIELD_MISSING:
        val = _resolve_raw_path_autolist(obj, path)
    if val is _FIELD_MISSING and path.startswith("root."):
        val = _resolve_raw_path_autolist(obj, path[len("root."):])
    if val is _FIELD_MISSING and path.startswith("headers."):
        val = _resolve_raw_path_autolist(obj, "header." + path[len("headers."):])
    # ★ fallback — סלחנות לטעות-נתיב של ה-LLM, אבל **בלי חציית-אחים** (referral↔act):
    #   1. סיומת parent.leaf (שני סגמנטי-שדה אחרונים) בכל מקום — דורש שה-parent (referral_practitioner)
    #      באמת קיים. כך 'referral_practitioner.practitioner_id' לא נפתר ל-act_practitioner (false-pass).
    #   2. רק לנתיב חד-סגמנטי — leaf בודד חד-משמעי.
    if val is _FIELD_MISSING and "." in path:
        fields = [s for s in path.split(".") if not s.lstrip("-").isdigit()]
        if len(fields) >= 2:
            val = _find_suffix_anywhere(obj, fields[-2:])
        else:
            val = _find_by_leaf_name(obj, fields[-1] if fields else path)
    elif val is _FIELD_MISSING:
        # ★ נתיב חד-סגמנטי (leaf בודד בלי '.', כמו 'mac_producer_id' שהתסריט מאמת) — חיפוש חד-משמעי בכל
        # עומק (גם תחת 'header'/'_data'). כך אסרשנים על שדות שהתסריט נוקב בשמם הפשוט נפתרים. דו-משמעי → missing.
        val = _find_by_leaf_name(obj, path)
    return val


def _find_suffix_anywhere(obj: Any, segs: List[str]) -> Any:
    """מחפש את הערך שבו סיומת-הנתיב `segs` (שמות-שדה) נפתרת, בכל מקום ב-tree (read-only, auto-index).
    מחזיר _FIELD_MISSING אם אין. כך 'parent.leaf' נדרש שה-parent יתקיים — מונע חציית-אחים."""
    v = _read_by_path(obj, ".".join(segs))
    if v is not _FIELD_MISSING:
        return v
    if isinstance(obj, dict):
        for x in obj.values():
            r = _find_suffix_anywhere(x, segs)
            if r is not _FIELD_MISSING:
                return r
    elif isinstance(obj, list):
        for x in obj:
            r = _find_suffix_anywhere(x, segs)
            if r is not _FIELD_MISSING:
                return r
    return _FIELD_MISSING


# ★ marker גורף לאימות *נוכחות* (לא שוויון) — לשדות דינמיים: ערך מוצפן/RSA/GUID/timestamp
# שאי-אפשר לחזות. ה-compiler שם marker כזה כ-value; ה-validator בודק שהשדה קיים ולא-ריק.
_PRESENT_MARKERS = {"__PRESENT__", "__NOT_EMPTY__", "__ANY__", "__ENCRYPTED__"}
# ★ אימות *היעדרות* — לתרחישי "השדה/האובייקט לא אמור להופיע ביעד" (למשל referral_practitioner
# כשאין רופא-מפנה במקור). עובר אם השדה חסר/ריק/null; נכשל אם הוא קיים עם ערך.
_ABSENT_MARKERS = {"__ABSENT__", "__NOT_PRESENT__", "__MISSING__", "__EMPTY__"}


def _is_producer_metadata_key(k: str) -> bool:
    """True ל-header.mac_* / headers.mac_* — metadata של ה-producer (mac_sys_name, mac_producer_name,
    mac_app_*, mac_channel, mac_correlation_id...). אלה *לא* טרנספורמציה תחת-בדיקה, וה-LLM אינו יודע
    את ערכיהם (הם של ה-Worker) → לא לאמת אותם (rest ביטחון; ה-compiler כבר לא אמור לפלוט אותם)."""
    kl = k.lower()
    return kl.startswith("header.mac_") or kl.startswith("headers.mac_")


def _as_number(v: Any) -> Optional[float]:
    """ממיר למספר להשוואה סלחנית (1.0==1, '1'==1, '013'=='13'). None אם לא-מספרי. bool אינו מספר."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip())
        except (ValueError, TypeError):
            return None
    return None


def _values_match(actual: Any, want: Any) -> bool:
    """השוואה **מבנית + סלחנית-מספרית** — מחליפה את str(a)!=str(b) המחמיר שגרם לכשלי-שווא:
    - dict/list/scalar לפי `==` (dict ב-Python בלתי-תלוי-סדר-מפתחות → payer identifier עובר).
    - מספרים לפי ערך (1.0==1, '1'==1, אפסים-מובילים → mac_message_version=1.0 מול 1).
    - אחרת fallback ל-str מנוקה (התנהגות קודמת לשדות טקסט)."""
    if actual == want:
        return True
    na, nw = _as_number(actual), _as_number(want)
    if na is not None and nw is not None and na == nw:
        return True
    return str(actual).strip() == str(want).strip()


def _check_expected_fields(value: Dict[str, Any], expected: Dict[str, Any]) -> List[str]:
    """מחזיר רשימה של שדות שחסרים / לא תואמים. ריק = הכל בסדר.
    מפתח עם נקודה ('root.action', '_data.parameters.0.gender') נחשב dotted path מקונן.
    ★ שדות header.mac_* (metadata של ה-producer) מדולגים — לא הטרנספורמציה הנבדקת.
    """
    issues: List[str] = []
    if not expected:
        return issues
    if not isinstance(value, (dict, list)):
        return list(expected.keys())
    for k, want in expected.items():
        # ★ פותרים כל מפתח דרך _resolve_field_path (תומך dotted + leaf-בודד חד-משמעי בכל עומק, כולל תחת
        # 'header'/'_data'). כך אסרשנים שהתסריט נוקב בשמם הפשוט (mac_producer_id=75) נפתרים ומאומתים.
        # (השדות התנודתיים — mac_correlation_id/mac_transaction_id — כבר הוסרו ב-_sanitize_expected_fields.)
        actual = _resolve_field_path(value, k)
        # ★ אימות היעדרות — השדה לא אמור להופיע ביעד (תרחיש "אובייקט לא נבנה")
        if isinstance(want, str) and want in _ABSENT_MARKERS:
            present_nonempty = actual is not _FIELD_MISSING and str(actual).strip() != "" and actual not in (None, {}, [])
            if present_nonempty:
                issues.append(f"{k} (אמור להיות חסר אך קיים: {actual!r})")
            continue
        # ★ אימות נוכחות (ערך דינמי) — קיים ולא-ריק, ללא בדיקת שוויון
        if isinstance(want, str) and want in _PRESENT_MARKERS:
            if actual is _FIELD_MISSING or str(actual).strip() == "":
                issues.append(f"{k} (missing/empty)")
            continue
        if actual is _FIELD_MISSING:
            issues.append(f"{k} (missing)")
            continue
        if not _values_match(actual, want):
            issues.append(f"{k}={actual!r}≠{want!r}")
    return issues


# ============================================================
# Kafka error classification — מתרגם הודעות סתמיות לעברית עם המלצה
# ============================================================

# שגיאות שמסמנות בעיית תשתית/ACL שלא תתוקן בין TCs — אין טעם להמשיך
FATAL_INFRA_ERROR_CODES = (
    "TOPIC_AUTHORIZATION_FAILED",
    "GROUP_AUTHORIZATION_FAILED",
    "CLUSTER_AUTHORIZATION_FAILED",
    "SASL_AUTHENTICATION_FAILED",
    "_AUTHENTICATION",
    "_AUTHORIZATION",
)


def _classify_kafka_error(err_str: str, topic: str = "", action: str = "publish") -> Dict[str, Any]:
    """מסווג שגיאת Kafka לפי הטקסט שלה. מחזיר dict עם:
    - friendly: הודעה ידידותית (עברית) שמסבירה מה הבעיה
    - recommendation: מה לעשות לפי הסיווג
    - is_fatal_infra: True אם זו בעיית תשתית/ACL שלא תיפתר בין TCs
    - raw: הטקסט המקורי
    """
    s = err_str or ""
    out: Dict[str, Any] = {"raw": s, "is_fatal_infra": False}

    # ★ REST Proxy authorization — HTTP 401/403, "Not authorized", error_code 40301
    is_rest_authz = (
        "HTTP 403" in s or "HTTP 401" in s
        or "Not authorized" in s or "not authorized" in s
        or "40301" in s or "40101" in s
    )
    mentions_group = "group" in s.lower()
    user = settings.KAFKA_SASL_USERNAME or "<your-user>"

    # ★ Group authorization — חייב לבדוק *לפני* topic, כי REST מחזיר 403+group יחד
    if "GROUP_AUTHORIZATION_FAILED" in s or (is_rest_authz and mentions_group):
        bad_group = _parse_group_from_error(s) or settings.KAFKA_CONSUMER_GROUP or settings.KAFKA_CONSUMER_GROUP_PREFIX
        out["friendly"] = (
            f"אין הרשאת ACL ל-consumer group '{bad_group}'. ה-user שלך מזדהה בהצלחה "
            f"אבל לא רשאי להשתמש ב-group הזה."
        )
        out["recommendation"] = (
            f"שתי אפשרויות:\n"
            f"  1) הגדר ב-.env את KAFKA_CONSUMER_GROUP לשם group מדויק שיש לך עליו הרשאה "
            f"(ללא suffix אקראי).\n"
            f"  2) בקש מ-admin: kafka-acls --add --consumer --topic {topic} --group {bad_group} "
            f"--principal User:{user}"
        )
        out["is_fatal_infra"] = True
    elif "TOPIC_AUTHORIZATION_FAILED" in s or is_rest_authz:
        op = "Write" if action == "publish" else "Read"
        out["friendly"] = (
            f"אין הרשאת ACL {op} ל-topic '{topic}'. ה-user שלך מזדהה בהצלחה אבל "
            f"Kafka דוחה את הפעולה."
        )
        out["recommendation"] = (
            f"בקש מ-admin של Kafka להוסיף ACL:\n"
            f"  kafka-acls --add --{('producer' if action == 'publish' else 'consumer')} "
            f"--topic {topic} --principal User:{user}"
            + (f" --group <your-group>" if action == "consume" else "")
        )
        out["is_fatal_infra"] = True
    elif "SASL_AUTHENTICATION_FAILED" in s or "Authentication failed" in s:
        out["friendly"] = "ה-SASL credentials שגויים (username/password לא מתאימים)."
        out["recommendation"] = "בדוק KAFKA_SASL_USERNAME ו-KAFKA_SASL_PASSWORD ב-.env."
        out["is_fatal_infra"] = True
    elif "UNKNOWN_TOPIC_OR_PART" in s:
        out["friendly"] = f"ה-topic '{topic}' לא קיים ב-cluster."
        out["recommendation"] = (
            f"בקש מ-admin ליצור את ה-topic, או שנה את ה-source/target topic ב-Payload Builder."
        )
        out["is_fatal_infra"] = True
    elif "_TRANSPORT" in s or "Connection" in s or "broker" in s.lower():
        out["friendly"] = "לא ניתן להתחבר ל-Kafka broker."
        out["recommendation"] = (
            f"בדוק KAFKA_BOOTSTRAP_SERVERS={settings.KAFKA_BOOTSTRAP_SERVERS!r} ו-VPN/network."
        )
        out["is_fatal_infra"] = True
    else:
        out["friendly"] = s[:200]
        out["recommendation"] = "בדוק את הטקסט המלא של השגיאה."
    return out
