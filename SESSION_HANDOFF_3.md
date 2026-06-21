# QA-AI-Hero — Session Handoff #3 (לסשן 4)

> **קרא לפי הסדר: `SESSION_HANDOFF.md` (סשן 1 — ESB) → `SESSION_HANDOFF_2.md` (סשן 2 — בניית .NET) → הקובץ הזה (סשן 3).**
> סשן 3 לקח את מחלקת .NET מ"תרחישים חיוביים נכשלים" ל-**עובדת end-to-end** (CREATE/DELETE/שלילי עוברים),
> תיקן שרשרת באגים ארוכה ב-consume, וחיזק את ה-correlation/validation. 20 קומיטים (`4a7fdf1`..`2a205e7`).

---

## 0. תקציר במשפט אחד
תרחישי .NET החיוביים נכשלו כי **ה-consume לא קרא את ה-partition הנכון** (שרשרת באגים ב-Confluent
REST Proxy) **וגם** כי ה-`expected_fields`/correlation היו שגויים. שניהם תוקנו; נוסף מנגנון
**member_id ייחודי לכל ריצה** ו**מסרי-דוגמה אמיתיים** כבסיס publish (format-agnostic ל-FHIR ולא רק MACKAF).

---

## 1. ★★★ ארכיטקטורת ה-consume (הכי חשוב להבין — אל תשבור!)

מחלקת .NET צורכת מ-target topic דרך **Confluent REST Proxy** (port 8082, `KafkaRestClient`).
עברנו שרשרת באגים — **כל "תיקון" חשף את הבא**. המצב הסופי (קובץ `agents/runner/kafka_rest_client.py`):

| # | הבעיה שהתגלתה | הפתרון הסופי |
|---|---|---|
| 1 | **subscribe** על ה-group המשותף עם ה-Worker → rebalance → כיסוי **חלקי** של partitions | **לעולם לא subscribe.** consumer **נפרד לכל partition** (`_open_partition_consumer`) |
| 2 | **multi-partition fetch** של ה-proxy החזיר רק partition אחד (verifyhub) | consumer-per-partition פותר; ה-poll סורק את כולם במקביל |
| 3 | seek-to-end "משקר" — מחזיר 200 גם ל-partition לא-קיים | אימות קיום דרך **records-fetch** אמיתי (`_PROBE_FETCH_TIMEOUT_MS`) |
| 4 | **tip-wait שבור** — ה-proxy לא דוחף מסר **בודד** ל-partition שקט בהמתנה בסוף (live=0 לכולם חוץ מה-partition הפעיל) | **לא ממתינים בסוף.** `_find_recent_start` עושה **binary-search ל-HW** + `re-seek` לאופסט ספציפי + קריאת data קיים כל סבב |
| 5 | **retention** — ה-log-start אינו 0; `seek offset=0` = out-of-range → reset ל-tip (ריק) | ה-binary-search מתחיל מ-**log-start אמיתי** (`/positions/beginning`), לא מ-0 |

**מנגנון הקריאה הסופי (`consume()`):** לכל partition → consumer ייעודי → binary-search למצוא את ה-HW
(retention-aware) → seek לאופסט קרוב ל-HW → קריאת data קיים → התקדמות (`starts[p]`) → סריקה עם
matcher. `_diagnose_partitions` (seek-to-beginning) רץ על כשל לאבחון.

### ★★★ עובדה קריטית #1: ה-TIMESTAMP FILTER שבור ב-REST
ה-REST Proxy בפורמט **binary לא מחזיר Kafka timestamp** ברשומות → `rec.get("timestamp")` תמיד `None`
→ ה-timestamp filter (`min_timestamp_ms`) הוא **no-op**. **לכן אי-אפשר לסמוך עליו למניעת תפיסת מסר ישן.**
הפתרון: **member_id ייחודי לכל ריצה** (ראה §2) — מסר ישן לא מכיל את ה-id שלנו.
→ אם רוצים timestamp filter אמיתי → **חייבים native SDK** (`msg.timestamp()`) — ראה §6 "פתוחים".

---

## 2. ★★★ עובדה קריטית #2: member_id ייחודי לכל ריצה (`_apply_unique_id`)

ה-target topic מלא בכפילויות (member_id 555/55 מטסטים קודמים). בלי ייחודיות — תופסים מסר אקראי/ישן.
**הפתרון:** ה-**runner** (`dotnet_runner._apply_unique_id`) מזריק **דטרמיניסטית** member_id ייחודי:
- **לא סומכים על ה-LLM** להזריק token (הוא לא אמין בשדה מקונן במקור) — ה-runner דורס בקוד.
- אותו ערך במקור (publish) ובקורלציה (wait) → ה-Worker מפיק key ייחודי ב-target → תופסים בדיוק את שלנו.
- **תרחישים שליליים** מתאמים על `key_contains=uid` → מחפשים id שלא הופק → timeout → PASS נכון.
- **אפסים מובילים תלוי-בקשה**: רק אם ה-member_id *בתסריט* מתחיל ב-0 (ת.ז) — נשלח במקור עם אפסים
  (`zfill(9)`) והיעד נקי → בודק את הסרת האפסים. `_record_matches` הוא **int-tolerant** (`000123456`≈`123456`).
- **format-agnostic** (חדש בסוף הסשן): שם-השדה מגיע מ-`key_built_from` של ה-Payload Builder
  (entity_id/member_id/...), לא קשיח `member_id`. fallback ל-member_id כשאין.

---

## 3. ה-correlation/validation pipeline — איך PASS נקבע (חשוב!)

תרחיש **חיובי** עובר **רק אם**: (א) `_scan_records` מצא מסר שתואם את ה-correlation
(`key_contains=uid` + `match` על entity_type/action) **וגם** (ב) `_check_expected_fields` עבר על המסר
שנצרך. תרחיש **שלילי** עובר רק אם **לא** הגיע מסר. **לא** מספיק "נשלח ל-source ול-target".

`expected_fields` (`dotnet_compiler` prompt + `dotnet_runner._check_expected_fields`):
- **אמת רק מה שהתסריט מבקש** ("ודא שדה X"), עם הערך המומר (מ-TRANSFORMATIONS), **שקיים ב-TARGET_EXAMPLE**.
- **אל תאמת**: `header.mac_*` (metadata של ה-Worker), `entity_id`/ה-KEY (זה ה-correlation), GUID/תאריכים.
- **ערך דינמי/מוצפן** (pdf_link מוצפן, RSA, hash) → marker `__PRESENT__` = בדיקת **נוכחות**, לא שוויון.
- **validator סלחני (גורף)**: auto list-index (`parameters.member_id`→`.0.`), root./headers. fallback,
  ו-**leaf-name fallback** — נתיב שגוי של ה-LLM נפתר לפי שם-השדה בכל מקום ב-tree.

---

## 4. ★ פיצ'ר מסרי-דוגמה + format-agnostic (סוף הסשן — לבדוק בעבודה!)

המערכת הייתה קשיחה לפורמט MACKAF (`header`+root משוטח+`_data`). אפיון **FHIR** (lab results,
`{resourceType:"Bundle",entry:[...]}`) נשבר — ה-Payload Builder עטף בכוח ב-headers/root → `header:{}` שגוי.

**הפתרון (החלטת היוזר — מסרי דוגמה אמיתיים):**
- היוזר מעלה **מסר/י דוגמה מהטופיק מקור** (כפתור **"📥 מסרי דוגמה מקור"** ב-Phase A,
  endpoint `/upload-sample-messages`, נשמר ב-`session.sample_source_messages`).
- ה-compiler (`SYSTEM_PROMPT_DOTNET_WITH_TEMPLATES`) משתמש ב-`SOURCE_SAMPLE` כ**בסיס ה-publish כפי-שהוא**
  (format-agnostic, בלי עטיפת MACKAF), עם דריסות מהתסריט בלבד. **אם אין דוגמה → fallback ל-Payload Builder.**
- `_to_wire_message` נשאר **no-op** לדוגמה (אין headers/root) — זה בדיוק מה שמונע את עיוות ה-FHIR.
- **unique-id format-agnostic**: ה-pipeline מחלץ `key_built_from` (מ-`target_templates`) ל-`ex.key_built_from`;
  ה-runner מזריק לפי שם-השדה משם (entity_id/member_id).

**⚠ פתוח לבדיקה (סשן 4 מתחיל מכאן):** לא נבדק עדיין בעבודה עם FHIR אמיתי. **בדוק:**
1. ב"פתח פרטים" ה-`📤 נשלח ל-source` = ה-FHIR Bundle **כפי שהוא** (בלי `header:{}`).
2. בלוג `<id_name> ייחודי לריצה` — השם מ-key_built_from (entity_id), לא member_id.
3. **סיכון:** אם `key_built_from` מצביע על שדה בשם **גנרי** (`value` ב-`identifier.value`) — ה-override
   הרקורסיבי לפי שם עלול לדרוס יותר מדי. אם זה קורה → לעבור ל-override **לפי נתיב מדויק** (path-based)
   במקום לפי שם. תראה את ה-key_built_from ב"📦 Templates של Payload Builder".

---

## 5. UI/UX שנוסף בסשן 3
- **"פתח פרטים" מובנה** (`_renderDotnetSteps` ב-index.html): לכל action — 📤 המסר שנשלח ל-source
  (JSON), 📥 המסר שהתקבל מ-target (offset/key) + ✓ השדות שאומתו; JSON גולמי מקופל.
- **אזהרת קובץ גדול**: docx > `_SPEC_MAX_MB`(=4) → הודעה אדומה "נסה PDF" (DirectLine דוחה >~MB ב-400).
- **דילוג על אימות לוגים** (Elastic לא מחובר) — ה-prompt לא יוצר kafka_wait מזויף שייתן PASS שקרי.

---

## 6. ★ מה פתוח לסשן 4

1. **★ לבדוק FHIR end-to-end** (§4) — מסר-דוגמה + format-agnostic. כנראה הצעד הראשון.
2. **native confluent-kafka consumer (stage 2 — תוכנן, לא נבנה)**: ייתן `msg.timestamp()` אמיתי →
   timestamp filter שעובד (היום no-op ב-REST). `KAFKA_CONSUME_TRANSPORT=rest|native`, מאחורי
   try/except ImportError, manual assign של כל ה-partitions (לא subscribe — תיקון 2.4#3), reuse של
   `_scan_records`/matchers. **לא למחוק את REST.** סקריפט `scripts/test_native_consume.py` לבדיקת ACL.
3. **dedup לוגיקת `_extract_key_built_from`** — כפול ב-`dotnet_pipeline.py` וב-`dotnet_compiler._extract_kbf`.
4. **מ-prior sessions (עדיין פתוח)**: Couchbase verify (קוד מוכן, אין credentials); Python ADO Agent
   (create_suite/set_result); DTM sub-department.
5. **לדחוף 20 הקומיטים בעבודה**: `git -c http.sslVerify=false push origin main` (SSL inspection).

---

## 7. קבצים קריטיים של .NET (מצב סוף סשן 3)
| קובץ | תפקיד |
|---|---|
| `agents/runner/kafka_rest_client.py` | ★ consumer-per-partition + binary-search HW + retention + int-tolerant match. **אל תיגע ב-tip-wait** |
| `agents/runner/dotnet_runner.py` | ★ `_apply_unique_id` (format-agnostic + leading-zeros), `_check_expected_fields` (גורף + __PRESENT__ + leaf-fallback), `_to_wire_message` (אידמפוטנטי), `_renderDotnetSteps` data |
| `agents/compiler/dotnet_compiler.py` | ★ prompt — SOURCE_SAMPLE כבסיס, correlation מדויק, expected_fields = מה שהתסריט מבקש, דילוג לוגים |
| `pipeline/dotnet_pipeline.py` | מעביר `sample_messages` + `key_built_from` ל-compiler/executable |
| `models/dotnet_test_case.py` | `DotNetExecutableTestCase.key_built_from` |
| `server/routes.py` | `/upload-sample-messages` + `_parse_messages_json` |
| `server/chat_session.py` | `sample_source_messages` |
| `server/static/index.html` | "📥 מסרי דוגמה מקור", "פתח פרטים" מובנה, אזהרת קובץ גדול |
| `tests/test_dotnet_runner.py`, `test_kafka_rest_client.py`, `test_sample_messages.py` | 115 tests עוברים |

---

## 8. כללי עבודה (חשוב!)
1. **קרא את 3 ה-handoffs ראשון.**
2. **טאב "📜 לוגים של הריצה" = כלי הדיבוג המרכזי** — בקש screenshots ממנו + מ"פתח פרטים" + Offset Explorer.
3. **`py -m pytest tests/`** אחרי כל שינוי (115 tests; ~6 דק', הרץ ב-background).
4. **אחרי כל שינוי קוד — commit + רשימת הקבצים ששונו** (היוזר ביקש זאת במפורש).
5. **לעולם לא commit ל-.env.**
6. **Push בעבודה בלבד**: `git -c http.sslVerify=false push origin main` (SSL inspection ארגוני).
7. **Kafka/ESB/Copilot זמינים רק מרשת מכבי** (מהבית = mock). היוזר מריץ בעבודה ושולח screenshots.
8. **אל תשבור את 7 התיקונים הקריטיים** (סקציה 2.4 ב-handoff 2) ואת **ארכיטקטורת ה-consume** (§1 כאן).
9. **תאימות לאחור**: אין מסרי-דוגמה → Payload Builder כרגיל; אין key_built_from → member_id. child_development
   חייב להמשיך לעבור.

---

## 9. קומיטים של סשן 3 (20, מ-`4a7fdf1` עד `2a205e7`)
`4a7fdf1` partition coverage probe · `caf5067` empty-env tolerance · `b8482c5` partition diag ·
`851a26b` records-fetch validate · `ac922f6` consumer-per-partition · `3805e64` seek-to-beginning diag ·
`fb2319b` concurrent poll · `3bb838b` binary-search HW · `13e99a0` retention log-start ·
`14c0f47` expected_fields + lenient validator · `d609fa7` precise correlation · `9e5663f` no-invent + unique-id ·
`28b581b` deterministic member_id · `aa7142a` UI per-step + skip logs · `76246b4` negative correlation + verify-fields ·
`0b4ec9c` __PRESENT__/leaf-fallback · `c888839` leading-zeros · `aeb4bcd` leading-zeros conditional ·
`5a4a9b7` oversized upload warn · `2a205e7` sample-messages + format-agnostic.

---

## 10. תקציר במשפט אחד (מעודכן)
מחלקת .NET עובדת end-to-end ברשת מכבי: Copilot Studio (Test Case Writer + Payload Builder) → Compile →
publish ל-source דרך REST Proxy → **consume מדויק** (consumer-per-partition + binary-search retention-aware)
עם **member_id ייחודי לכל ריצה** → אימות שדות אמיתי → UI שקוף; פורמטים שאינם MACKAF נתמכים דרך
**מסרי-דוגמה אמיתיים** + key_built_from. נשאר לבדוק FHIR end-to-end ולשקול native SDK ל-timestamp אמיתי.
