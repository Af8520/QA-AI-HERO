# QA-AI-Hero — Session Handoff #2 (לסשן 3)

> **קרא קודם את `SESSION_HANDOFF.md` (סשן 1), ואז את הקובץ הזה (סשן 2).** סשן 1 בנה את ESB.
> סשן 2 בנה את **תת-מחלקת .NET** (Kafka/Couchbase) + שיפורי UX רבים. סשן 3 ממשיך מכאן.

---

## 0. תקציר במשפט אחד

סשן 2 הוסיף **Landing page** לבחירת מחלקה, ובנה את **תת-מחלקת .NET** end-to-end: סוכן Test Case
Writer + סוכן **Payload Builder** (חדש) ב-Copilot Studio → Compiler (gpt-4.1-mini) שממזג
source+target templates → **Kafka REST Proxy** runner שמפרסם ל-source topic, ממתין ל-Worker,
וצורך מ-target topic עם **קורלציה מורכבת** (key + entity_type + timestamp). הזרימה כמעט שלמה;
נשארה בעיית debug אחת פתוחה (כיסוי partitions + תקינות test data — ראה סקציה 7).

---

## 1. מה נבנה בסשן 2 (28 קומיטים, מ-`925d09b` עד `8a1b133`)

### 1.1 UX / מבנה כללי
- **Landing page + hash routing** (`#/`, `#/integration`, `#/integration/esb`, `#/integration/dotnet`):
  6 קוביות מחלקה (אינטגרציה פעילה, השאר "בבנייה"), ואז תת-מחלקות (ESB + dotNet פעילים, DTM בבנייה).
- **תיקון Request/Response viewer** (`addTcDetailBubble`) — היה מציג HTML גולמי, עכשיו DOM ישיר עם כפתור.
- **WebSocket מוגדר ב-.env** (`COPILOT_USE_WEBSOCKET`) + **thinking timer** ("🤔 הסוכן חושב... 0:23").
- **Per-step display בצ'אט** — לכל TC מוצגים ה-steps עם ✓/✗ + סיבה (לא רק "פתח פרטים").

### 1.2 Smart Compiler (ESB) — תיקונים
- **Hybrid regex+LLM**: regex תופס method+url+body+headers; LLM רק כשregex מפספס. ~0 LLM calls כשהסוכן עקבי.
- **mac_user_name + mac_user_id headers** מוזרקים אוטומטית בכל קריאת ESB.

### 1.3 Debug / Observability
- **Phase A debug logging** — 3 ערוצים: terminal (`/debug-log` endpoint), browser console, UI panel.
- **כפתור "📄 JSON של Phase A"** — מציג מה הסוכן החזיר. נשמר ב-`logs/phase_a/`.
- **כפתור "📦 Templates של Payload Builder"** — מציג source+target templates+transformations (מה שעובר למוח). נשמר ב-`logs/payload_builder/`.
- **טאב "📜 לוגים של הריצה"** — לוג מובנה פר-ריצה (run_id), כל פעולה עם timestamp, צבעים, filter, הורדת JSONL. נשמר ב-`logs/runs/<run_id>.jsonl`. ★ זה הכלי המרכזי לדיבוג.

### 1.4 ★ תת-מחלקת .NET (הליבה של סשן 2)
מחלקת .NET בודקת **Workers** שמעבירים מידע Kafka→Kafka (או Kafka→Couchbase) עם **טרנספורמציה**
(למשל `gender: M→"זכר"`). הזרימה:
1. **Phase A**: סוכן Copilot Studio "Test Case Writer" (token endpoint נפרד: `DOTNET_COPILOT_TOKEN_ENDPOINT`) מייצר תסריטים.
2. **Spec auto-capture**: כשהיוזר מעלה אפיון ב-📎 ב-WebChat — אנחנו תופסים אותו אוטומטית (`WEB_CHAT/SET_SEND_BOX_ATTACHMENTS`) ושולחים ל-`/extract-spec` (.NET only).
3. **Payload Builder** (סוכן Copilot Studio שני, `DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT`): מקבל את ה-spec (כקובץ דרך DirectLine `/upload`) ומחזיר JSON עם source `templates` + `target_templates` (כולל `key_built_from`, `dynamic_fields`) + `transformations` + `target_entity_type`.
4. **DotNetCompiler** (המוח): ממזג template + תסריט → `DotNetExecutableTestCase` עם רצף `actions` (kafka_publish, kafka_wait, couchbase_wait).
5. **DotNetRunner**: מפרסם ל-source topic דרך **Kafka REST Proxy**, ממתין ל-Worker (עד 150ש), צורך מ-target topic, מאמת.
6. **Validator/Reporter** — generic, ללא שינוי.

---

## 2. ארכיטקטורת ה-Kafka (חשוב מאוד לסשן 3)

### 2.1 למה REST Proxy ולא native confluent-kafka
- native נכשל על `TOPIC_AUTHORIZATION_FAILED` (ה-principal של ה-end-user חסר ACL).
- **Confluent REST Proxy** (port 8082) עובד עם אותו user/password (הוכח ב-Postman) כי הוא מפרסם
  ב-principal פריבילגי משלו. הכל דרך `httpx` (מכבד `VERIFY_SSL=false`). אין צורך ב-librdkafka.
- `KAFKA_REST_PROXY_URL=https://cnf-cnct01-test:8082`; ה-Basic-Auth = `KAFKA_SASL_USERNAME/PASSWORD`.

### 2.2 Produce
`POST {base}/topics/{topic}` עם `Content-Type: application/vnd.kafka.json.v2+json`,
body `{"records":[{"key": "qa_ai_hero_<TC>", "value": {...}}]}`.

### 2.3 Consume — Confluent REST Proxy v2 consumer API
- format **binary** (לא json! ה-keys ב-target הם strings רגילים שמפילים json format → 408).
- מפענחים base64 בעצמנו: key→string, value→JSON.
- **manual assign של *כל* ה-partitions** (לא subscribe!) — ה-consumer group הקבוע משותף עם
  ה-Worker, ו-subscribe נותן רק חלק מה-partitions. `GET /topics/{topic}` → `POST /assignments` →
  `POST /positions/end` (seek-to-end). fallback ל-subscribe אם אין Describe ACL.

### 2.4 ★ נקודות קריטיות שנפתרו (אל תשבור אותן!)
| נושא | פתרון |
|---|---|
| **topic case-sensitive** | מנרמלים ל-lowercase (`_normalize_topic`) — ה-Payload Builder מחזיר אותיות גדולות → 403 |
| **wire format** | המסר האמיתי הוא `header` (יחיד) + שדות root **ברמה העליונה** (לא עטוף ב-"root") + `_data`. `_to_wire_message()` ממיר את ה-template המקובץ. בלי זה ה-Worker לא מפרסר! |
| **consumer group** | `KAFKA_CONSUMER_GROUP` — שם מדויק (ACL literal), בלי suffix אקראי |
| **async worker** | ה-Worker כותב ל-target תוך עד 1-2 דקות → `KAFKA_WAIT_MIN_SECONDS=150` |
| **temporal correlation** | seek-to-end לפני publish (`on_ready` callback) + timestamp filter (`record.timestamp >= publish_ts - skew`). מונע לתפוס מסר ישן מ-TC קודם. |
| **קורלציה** | `key_contains` = ה-**member_id הייחודי** (לא "0"/code!) + `match: {entity_type: child_development}` (כפול — דוחה verifyhub) |

---

## 3. קבצים קריטיים של .NET (חדשים/שונו בסשן 2)

| קובץ | תפקיד |
|---|---|
| `models/dotnet_test_case.py` | KafkaPublishAction, KafkaWaitAction (key_equals/key_contains/match/expected_fields/expect_no_message), CouchbaseWaitAction, DotNetExecutableTestCase |
| `agents/compiler/dotnet_compiler.py` | ★ המוח. `SYSTEM_PROMPT_DOTNET_WITH_TEMPLATES` — ממזג templates+תסריט. `_strip_placeholders` (מסיר "MISSING") |
| `agents/runner/dotnet_runner.py` | ★ ה-runner. `_to_wire_message`, `_run_kafka_publish`, `_run_kafka_wait`, `_publish_then_wait` (warm-up), `_check_expected_fields` (dotted+list paths), `_classify_kafka_error` |
| `agents/runner/kafka_rest_client.py` | ★ REST Proxy client. `produce`, `consume` (manual assign + seek-to-end + on_ready + timestamp filter), `_scan_records`, `_decode_binary_record` |
| `agents/payload_builder/payload_builder_bridge.py` | DirectLine REST bridge לסוכן Payload Builder (שולח קובץ דרך `/upload`, polling עד JSON) |
| `pipeline/dotnet_pipeline.py` | Phase B עבור .NET. run_id + run_log (SSE `log_line` + jsonl). `_build_payloads` (קורא ל-Payload Builder) |
| `config/settings.py` | KAFKA_REST_PROXY_URL, KAFKA_CONSUMER_GROUP, KAFKA_WAIT_MIN_SECONDS=150, KAFKA_TIMESTAMP_SKEW_SECONDS=10, DOTNET_*_TOKEN_ENDPOINT |
| `server/static/index.html` | Landing, per-step bubbles, Logs tab, Payload Templates modal, spec auto-capture |
| `tests/test_kafka_rest_client.py`, `test_dotnet_runner.py`, `test_dotnet_compiler.py`, `test_payload_merge.py` | 84 tests עוברים |

---

## 4. .env — שדות .NET (להוסיף בעבודה)
```env
DOTNET_COPILOT_TOKEN_ENDPOINT=<Test Case Writer agent — Custom website>
DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT=<Payload Builder agent — Custom website>
DOTNET_PAYLOAD_BUILDER_TIMEOUT_SECONDS=300
KAFKA_REST_PROXY_URL=https://cnf-cnct01-test:8082
KAFKA_REST_USERNAME=      # ריק → fallback ל-SASL
KAFKA_REST_PASSWORD=
KAFKA_SASL_USERNAME=kfk_dotnet_user
KAFKA_SASL_PASSWORD=<...>
KAFKA_CONSUMER_GROUP=worker.k2k.encryption.child_development   # ⚠ group מורשה; אל תשתמש ב-group של ה-Worker בפרודקשן
KAFKA_WAIT_MIN_SECONDS=150
KAFKA_TIMESTAMP_SKEW_SECONDS=10
```

---

## 5. סוכן Payload Builder (Copilot Studio) — instructions עדכניים

הסוכן צריך להחזיר JSON ממוקד (בלי field_catalog ענק שגורם ל-timeout). מבנה:
```json
{ "status":"success", "source_topic":"...", "target_topic":"...", "target_entity_type":"child_development",
  "templates": { "<action>": {"header":{},"root":{},"_data":{}} },
  "target_templates": { "<action>": {"key_format":"...","key_built_from":["_data.member_details.member_id","_data.member_details.member_id_code"],"dynamic_fields":["..."],"header":{},"root":{},"_data":{}} },
  "transformations": { "<source.path|FIXED|DERIVED>": {"target_field_path":"...","rule":"..."} } }
```
★ **חשוב**: `key_built_from` חייב לכלול את שדות ה-SOURCE שה-target KEY בנוי מהם (לרוב member_id).
ה-Compiler אצלנו קורא: `templates`, `target_templates`, `transformations`, `target_entity_type`.
**הוראה קריטית לסוכן**: "Keep output SMALL and FOCUSED — no per-field catalog, no prose."

---

## 6. מצב נוכחי — מה עובד / מה לא (סוף סשן 2)

| ✅/❌ | רכיב |
|---|---|
| ✅ | Landing + routing; כל ה-UX |
| ✅ | ESB flow — ללא שינוי, עובד |
| ✅ | Phase A .NET (Test Case Writer) + spec auto-capture |
| ✅ | Payload Builder מחזיר source+target templates (אחרי שמיקדנו את ה-instructions) |
| ✅ | Compiler ממזג נכון; ה-publish יוצא ב-wire format תקין |
| ✅ | **ה-Worker מפיק מסר ל-target** (`child_development::0::555` נראה ב-Offset Explorer!) |
| ✅ | **TC03 (תרחיש שלילי) עבר** — timestamp filter + expect_no_message עובדים |
| ⚠ | **TC01/TC02 (חיוביים) עדיין נכשלים** — ראה סקציה 7 |
| ❌ | Couchbase verify — קוד מוכן, לא נבדק (אין credentials) |
| ❌ | ADO (create_suite/set_result) — עדיין לא נבנה (פתוח מסשן 1) |

---

## 7. ★★★ הבעיה הפתוחה לסשן 3 (קריטי — תתחיל מכאן)

**הסימפטום:** TC01/TC02 (תרחישים חיוביים) נכשלים עם "no matching message", למרות שה-Worker מפיק
מסר ל-target (נראה ב-Offset Explorer). ה-breakdown בלוגים מראה רק `maccabi_online_backend` (verifyhub).

**שלוש השערות שצריך לאמת — היוזר צריך לשלוח את 2 השורות האלה מטאב הלוגים אחרי ריצה:**

1. **שורת `assignment:`** (הוספנו בקומיט `8a1b133`):
   - `manual — N partitions (כיסוי מלא)` → ה-manual assign עובד. אם N = מספר ה-partitions של ה-topic → קוראים הכל.
   - `subscribe — partial coverage!` → אין Describe ACL ל-`GET /topics`. צריך לבקש מ-admin, או למצוא דרך אחרת לגלות partitions.

2. **שורת `breakdown לפי mac_sys_name:`**:
   - אם מופיע `encryption_child_development_worker` → ה-Worker מפיק ואנחנו קוראים את ה-partition הנכון → הקורלציה תתפוס. אם לא תופס — בעיית key_contains/match.
   - אם **רק** `maccabi_online_backend` (verifyhub) למרות `manual — N partitions` → **ה-Worker לא מפיק למסרים האלה** = בעיית **test data** (ה-member_id ששלחנו לא תקין/לא קיים, או type_code לא 99918, או referral_date ישן מ-2024-01-01 → ה-Worker מסנן). בדוק את ה-publish payload (כפתור "פתח פרטים" → Actions) מול חוקי הסינון באפיון.

3. **correlation שגוי** (תוקן חלקית): היה `key_contains="0"` (member_id_code!). תיקנו prompt שיקח member_id
   ייחודי + יוסיף `match:{entity_type}`. ודא שבריצה הבאה ה-correlation בלוג מראה member_id ארוך + entity_type.

**הסדר המומלץ לסשן 3:**
1. בקש מהיוזר ריצה + screenshot של שורות `assignment:` + `breakdown:` + `correlation:` מטאב הלוגים.
2. לפי זה: אם `subscribe partial` → בעיית Describe ACL. אם `manual` + רק verifyhub → בעיית test-data
   (בדוק filtering rules: type_code=99918, referral_date>2024-01-01). אם נתפס מסר אבל assert נכשל →
   בעיית transformation/expected_fields.

---

## 8. פתוחים נוספים (מסשן 1 + חדשים)
1. **Python ADO Agent** — `create_test_suite` + `add_test_cases_to_suite` + `set_test_case_result` ב-`ado_client.py` (עדיין לא נבנה).
2. **Couchbase verify** — קוד ב-`dotnet_runner._run_couchbase_wait` מוכן, לא נבדק (אין credentials).
3. **Couchbase/Kafka direct (native)** — fallback קיים אבל REST Proxy עדיף.
4. **DTM sub-department** — עדיין "בבנייה".
5. **שאר המחלקות** (קליקס/AS400/CRM/דיגיטל/E2E) — placeholders.

---

## 9. כללי עבודה (חשוב!)
1. **קרא את SESSION_HANDOFF.md + הקובץ הזה ראשון.**
2. **טאב "📜 לוגים של הריצה" הוא כלי הדיבוג המרכזי** — בקש מהיוזר screenshots ממנו.
3. **`py -m pytest tests/`** אחרי כל שינוי קוד (84 tests; ריצה מלאה ~6 דקות — הרץ ב-background).
4. **קבצי הליבה של .NET**: `dotnet_runner.py`, `kafka_rest_client.py`, `dotnet_compiler.py` — שינויים שם משפיעים על הכל.
5. **לעולם לא commit ל-.env.**
6. **Push**: היוזר עובד מאחורי SSL inspection. `git -c http.sslVerify=false push origin main` (היוזר מאשר ידנית). 28 קומיטים בסשן 2.
7. **היוזר עובד גם מבית וגם מעבודה** — Kafka/ESB/Copilot זמינים רק מרשת מכבי.
8. **אל תשבור את ה-7 תיקונים הקריטיים** בסקציה 2.4 (topic lowercase, wire format, manual assign, seek-to-end, timestamp filter, async timeout, double correlation).
9. **שני סוכני Copilot Studio נפרדים ל-.NET**: Test Case Writer + Payload Builder. ה-instructions של ה-Payload Builder בסקציה 5.

---

## 10. Endpoints חדשים (סשן 2)
| Method | Path | תפקיד |
|---|---|---|
| POST | `/debug-log` | לוג מהדפדפן → terminal |
| GET | `/session/{id}/phase-a-json` | ה-JSON של Phase A |
| GET | `/session/{id}/payload-templates` | source+target templates+transformations (.NET) |
| `/session/start?department=esb\|dotnet` | בוחר token endpoint + pipeline לפי מחלקה |

SSE events חדשים: `log_line` (run-log), `tc_detail` כולל `steps[]`.

---

## 11. תקציר ארכיטקטוני
```
Phase A (.NET): Test Case Writer agent → תסריטים
              + Payload Builder agent → source+target templates  (spec auto-captured מ-📎)
                     ↓
Phase B: DotNetCompiler (gpt-4.1-mini) ממזג template+תסריט → actions
                     ↓
DotNetRunner → Kafka REST Proxy:
  publish (wire format) → source topic
  warm-up: manual assign ALL partitions + seek-to-end
  on_ready: publish + capture publish_ts
  poll: timestamp filter + key_contains(member_id) + match(entity_type) → MATCH
                     ↓
Validator + Reporter (generic) → per-step results בצ'אט + Logs tab
```
