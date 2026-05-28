# QA-AI-Hero — Session Handoff (לסשן הבא)

> **מטרת המסמך:** סיכום מקיף של מה שנבנה בסשן הקודם, איפה אנחנו עומדים, ומה הצעדים הבאים. קריאה זו אמורה לתת לסשן הבא של Claude את כל הקונטקסט הנחוץ.

---

## 1. מה זה הפרויקט

מערכת multi-agent להחלפת בודקי QA במכבי. **פיילוט: מחלקת אינטגרציה / ESB**.

ESB מפתחת APIs שמתממשקים ל-Kafka (producer/consumer), Couchbase, ו-SQL Server.
על כל פיתוח חייב להיבדק:
1. שה-API מחזיר תשובה תקינה לפי האפיון (status, schema, ערכי שדות)
2. שה-API כתב/קרא מסר תקין מ-Kafka (אם רלוונטי)
3. שה-API רושם לוגים נכונים ב-Elastic לפי האפיון

**עברנו מ-MVP על-נייר ל-MVP עובד end-to-end (במצב mock) שגם פותח גישה אמיתית ל-Foundry/Copilot Studio.**

---

## 2. ארכיטקטורה דו-שלבית

```
┌────────────────────────────────────────────────────────┐
│  Phase A — Interactive (Copilot Studio Agent)          │
│  המשתמש מדבר עם סוכן ב-WebChat custom canvas:           │
│  1. מעלה מסמך אפיון (📎 בצ'אט)                          │
│  2. סוכן מחזיר טבלת test cases                          │
│  3. אישור / עריכה                                       │
│  4. סוכן מחזיר JSON של test cases                       │
│  5. ה-JS שלנו מזהה JSON אוטומטית → /direct-json         │
└────────────────────────────────────────────────────────┘
                     ↓
┌────────────────────────────────────────────────────────┐
│  Phase B — Automated Pipeline (7 שלבים)                 │
│  1. Pull/use test cases                                 │
│  2. Pull MD attachment from ADO (אם זמין)               │
│  3. SmartCompiler — LLM (gpt-4.1-mini) מפרסר כל tc      │
│     ל-ExecutableTestCase (request מלא)                  │
│  4. Execute requests (httpx) — קריאות HTTP אמיתיות       │
│  5. Verify Kafka + Elastic (Playwright)                 │
│  6. Validate + Bug analysis + human approval            │
│  7. Reporter — סיכום עברית                              │
└────────────────────────────────────────────────────────┘
```

---

## 3. מה עובד עכשיו end-to-end

| שלב | סטטוס | הערות |
|---|---|---|
| Phase A — Custom Canvas (WebChat from CDN) | ✅ עובד | סוכן `crbf3_ESBTestscripter` (No-auth, ללא Tools) |
| Phase A — file upload בצ'אט (📎) | ✅ עובד | WebChat built-in |
| Phase A — auto JSON detection | ✅ עובד | Store middleware תופס DIRECT_LINE/INCOMING_ACTIVITY |
| Phase B trigger אוטומטי | ✅ עובד | מעבר חלק ל-pipeline ברגע שזוהה JSON |
| Phase B — Compile (LLM-only mode) | ✅ עובד | `gpt-4.1-mini` ב-Foundry |
| Phase B — Execute (HTTP אמיתי) | ✅ קוד מוכן | `RUNNER_MODE=esb`, צריך רשת מכבי |
| Phase B — pipeline resilience | ✅ | כל שלב עטוף ב-try/except — תסריט אחד לא מפיל את כולם |
| Phase B — Kafka/Elastic verify | ✅ קוד מוכן | Playwright; דורש credentials |
| Bug agent + ADO | ⚠ חלקי | bug creation עובד; create_suite/update_test_result דורש עוד 100 שורות |

---

## 4. החלטות ארכיטקטוניות שנבחרו (וההיסטוריה שלהן)

### 4.1 למה Custom Canvas ולא iframe או SDK

**ניסיונות שכשלו:**
1. **Microsoft 365 Agents SDK** (Python, msal) — קיבל `AADSTS650057`. ה-App Registration `06e21821-e0d1-4d57-b2ea-5c31bf242e11` חסר הרשאת `CopilotStudio.Copilots.Invoke` על `api.powerplatform.com`. **IT לא הוסיף את ההרשאה — עדיין ממתין.**
2. **Web app iframe (פשוט)** — עם `Microsoft authentication` Microsoft מסירים את ה-Embed code. עם `No authentication`, ה-Tools של הסוכן נכשלים עם `AuthenticationNotConfigured`.
3. **Web app iframe + סוכן stripped-down (ללא Tools)** — עבד אבל יש Cross-Origin (לא יכולנו לקרוא JSON אוטומטית), היוזר היה צריך copy-paste ידני.

**הפיתרון שעובד היום: Custom Canvas עם Bot Framework WebChat (CDN)**
- `webchat.js` נטען מ-CDN, מוטמע ב-HTML שלנו ב-localhost
- אין Cross-Origin → ה-JS שלנו רואה הכל
- `hideUploadButton: false` נותן 📎 לקבצים מובנה
- `DIRECT_LINE/INCOMING_ACTIVITY` middleware תופס תשובות הסוכן → autodetect JSON

### 4.2 למה Foundry במקום Azure OpenAI נפרד

ב-Phase B (Smart Compiler) צריך LLM. **לא לסוכן ספציפי — סתם chat completions** לפרסור טקסט ל-JSON. שתי אופציות:
- **Azure OpenAI נפרד** — דורש credentials מ-IT
- **Foundry של היוזר** ← זה מה שבחרנו (כבר מוגדר)

Endpoint: `https://qa-ai-hero-foundry.services.ai.azure.com/`
Model deployment: `gpt-4.1-mini`

(זה Azure-style endpoint — `AsyncAzureOpenAI` SDK שכבר השתמשנו בו מתחבר אליו ישירות, אין צורך ב-`AsyncOpenAI` נפרד.)

### 4.3 SmartCompiler — דואל מצב: עם Postman vs LLM-only

| מצב | מתי | מה קורה |
|---|---|---|
| Postman + LLM | יש Postman collection + Azure OpenAI | LLM mutates the template לפי התסריט |
| Postman בלבד | יש Postman, אין LLM | רינדור template עם env vars |
| **LLM-only** | אין Postman, יש Azure OpenAI | **המצב הנוכחי** — LLM בונה request מאפס מטקסט התסריט |
| Fallback BLOCKED | אין שום דבר | `url=about:blank`, מסומן BLOCKED |

ה-LLM-only mode דורש שהסוכן יכלול URL+method מלאים ב-steps (עדכון instructions שעשינו).

### 4.4 פתרונות עדינים שיש לדעת עליהם

- **URL encoding לתווים עבריים** (`?action_code=ש`) — נעשה אוטומטית ב-`_normalize_url()` ב-smart_compiler.py
- **dict→string coercion** ל-elastic_assertion.query / kafka_assertion — LLM נוטה להחזיר אובייקטים מובנים במקום strings, `_coerce_to_string()` מטפל
- **"וודא ש..." steps** מתורגמים ע"י ה-LLM-only prompt לאסרשנים (schema/kafka/elastic), לא לקריאות HTTP חדשות
- **Multi-call test cases** (TC-16: PATCH + GET לאימות) — ה-Compiler לוקח רק את ה-call הראשון, מציין ב-`compiler_notes`

---

## 5. סטטוס "פתוח / חוסם"

### חוסם — דורש פעולת IT
- **`CopilotStudio.Copilots.Invoke` ב-App Registration** — ה-IT שכח לטפל בזה. כל זמן שאין — לא יעבוד SDK של Microsoft 365 Agents. **לא חוסם בפועל** כי Custom Canvas עוקף.

### חוסם — דורש רשת פנימית
- **Real ESB API calls** — `RUNNER_MODE=esb` מופעל, אבל ה-URLs (`esb-lb-test.mac.org.il:5555`) זמינים רק ברשת פנימית של מכבי. מהבית = timeout.

### דחוי לעתיד
- **Python ADO Agent** — להוסיף ל-`ado_client.py`:
  - `create_test_suite(name) -> suite_id`
  - `add_test_cases_to_suite(suite_id, raw_cases)`
  - `set_test_case_result(test_case_id, status, comment)`
  
  זה יחליף את ה-Power Automate flow של הסוכן הישן (`crbf3_integrationQaTestGenerator`).

- **Postman Collection mode** — קוד מוכן ב-`agents/postman/`, אבל היוזר עוד לא העלה collection. בינתיים LLM-only mode מספיק.

- **Power Automate flow לסוכן ESBTestscripter** — אופציה למי שרוצה לחזור לזרימה של "סוכן מעלה ל-ADO". פחות מומלץ — Python ADO Agent עדיף ארכיטקטונית.

### Edge cases ידועים
- TC-16-style (2 HTTP calls סדרתיים) — נתפס רק הראשון
- Header assertions ("וודא MAC-StatusSeverity=S-Success") — נרשם ב-compiler_notes, אין JSONPath ל-headers (יוסף בעתיד)
- ל-Kafka/Elastic verify דורש Playwright + credentials → ב-default ידלג

---

## 6. קבצים קריטיים — מה איפה

| קובץ | תפקיד | סטטוס |
|---|---|---|
| `.env` | credentials + מצבים | ⚠ לא ב-git (gitignored) — היוזר ימלא בעבודה |
| `.env.example` | תבנית | ב-git |
| `config/settings.py` | Pydantic Settings | יציב |
| `agents/copilot_bridge/copilot_client.py` | Mock/Real bridges + completion detection | יציב |
| `agents/copilot_bridge/msal_auth.py` | MSAL token (לא בשימוש כרגע — חסום על HR) | יציב |
| `agents/foundry/foundry_writer.py` | Foundry agent client + handshake (לא בשימוש — UI מעדיף Copilot Studio) | יציב |
| **`agents/compiler/smart_compiler.py`** | **ליבת Phase B** — LLM-only mode | **שונה לאחרונה** |
| `agents/postman/*.py` | Postman loader + executor + LLM matcher | יציב |
| `agents/runner/esb_runner.py` | HTTP execution (httpx) + Kafka/Elastic verify | יציב |
| `agents/runner/mock_runner.py` | Mock עם תוצאות רנדומליות | יציב |
| `agents/runner/web_consoles/{confluent,kibana}.py` | Playwright web consoles | קוד מוכן, לא נבדק עם credentials אמיתיים |
| `agents/validator/validator_agent.py` | JSONPath + Kafka + Elastic validations | יציב |
| `agents/bug_agent/{bug_agent,ado_client}.py` | Bug analysis + ADO REST | bug creation עובד; create_suite חסר |
| `agents/parser/test_case_parser.py` | Legacy fallback | יציב |
| `agents/reporter/reporter_agent.py` | סיכום עברית | יציב |
| `models/*.py` | Pydantic models (TestCase, ExecutableTestCase, etc.) | יציב |
| **`pipeline/esb_pipeline.py`** | **Phase B מלא 7-שלבי** עם resilience | **שונה לאחרונה** |
| `server/app.py` | FastAPI app | יציב |
| `server/routes.py` | endpoints | יציב |
| `server/chat_session.py` | session state | יציב |
| **`server/static/index.html`** | **Chat UI + Custom Canvas + WebChat** | **שונה לאחרונה** |
| `main.py` | CLI entry: `--server` / `--test-bridge` / `--test-postman` | יציב |
| `requirements.txt` | תלויות | יציב |
| `tests/*` | 23 unit tests עוברים | יציב |

---

## 7. Endpoints של ה-server

| Method | Path | תפקיד |
|---|---|---|
| GET | `/` | מגיש את index.html |
| GET | `/health` | health check |
| POST | `/session/start` | התחלת session, מחזיר canvas_mode/embed_mode/foundry_enabled |
| POST | `/chat` | proxy ל-bridge (כשלא ב-canvas mode) |
| POST | `/upload-document` | ספק → bridge (במצב standard chat) |
| POST | `/extract-spec` | חילוץ טקסט מקובץ (להעתקה ב-iframe mode) |
| POST | `/upload-postman` | טעינת Postman collection |
| POST | `/complete-phase-a` | embed mode — היוזר מזין suite_id ידנית |
| **POST** | **`/direct-json`** | **★ ה-endpoint הראשי בשימוש** — מקבל test_cases JSON, מטריג Phase B |
| POST | `/foundry/generate-and-run` | מסלול Foundry one-shot (לא בשימוש — נכשל על multi-turn) |
| POST | `/approve-bugs` | אישור פתיחת bugs |
| GET | `/events/{session_id}` | SSE stream של Phase B progress |

---

## 8. מצבי פעולה (לפי .env)

הסדר עדיפויות ב-`/session/start`:
1. **Canvas mode** (★ ראשי): `COPILOT_TOKEN_ENDPOINT` מאוכלס → טוען WebChat
2. **Embed mode**: `COPILOT_WEBCHAT_URL` מאוכלס → iframe
3. **Real bridge**: 4 שדות SDK מאוכלסים → MSAL → Copilot Studio (חסום על HR)
4. **Mock bridge**: ברירת מחדל → multi-turn דמה

`RUNNER_MODE`:
- `mock` — ה-MockRunner מגריל תוצאות (~70% הצלחה)
- `esb` — ה-ESBRunner שולח HTTP אמיתי דרך httpx ← **המצב הנוכחי**

---

## 9. תיעוד נדרש בסוכן Copilot Studio

הוסף ל-Instructions של `crbf3_ESBTestscripter` (Copilot Studio):

```
כללי כתיבה של תסריטי בדיקה ב-steps:

לכל test case, כל step חייב להכיל:
1. שיטת HTTP (GET / POST / PUT / DELETE) — מתוך הסעיף "דוגמאות קריאה" באפיון
2. URL מלא — להעדיף את ESB URL מהאפיון (לא DP URL).
   למשל: http://esb-lb-test.mac.org.il:5555/esbapi/.../providers/{practitioner_id}/...
3. ערכי פרמטרים — אם זה תרחיש שלילי, ציין את הערך הלא תקין במפורש
4. headers נדרשים (אם יש באפיון)
5. body (לבקשות POST/PUT) — JSON או שדה ספציפי
6. expected_status — קוד HTTP הצפוי (200 / 400 / 404 / 500)

לפני שאתה מתחיל ליצור תסריטים:
- שאל את היוזר: "האם יש לך test data (member_id, practitioner_id וכו') לבדיקות חיוביות?"
- אם כן: השתמש בערכים שהוא נתן ב-steps של תרחישים חיוביים (expected 200)
- אם לא: השתמש בערכים מהאפיון או "TEST_DATA_REQUIRED" כ-placeholder

JSON output מבנה:
[{"test_case_id":"...", "steps":[{"step":"שלח METHOD ל-URL_מלא ...", "expected_result":"..."}]}]
```

✅ **כבר נוסף לפי הסוכן.**

---

## 10. סדר עבודה לסשן הבא

1. **לוודא שהפרויקט מוריד מ-GitHub ורץ** (במחשב עבודה, ברשת מכבי):
   ```powershell
   git clone https://github.com/Af8520/QA-AI-HERO.git
   cd QA-AI-HERO\qa-ai-hero
   copy .env.example .env
   # מלא credentials לפי הסקציה למטה
   py -m pip install -r requirements.txt
   py -m playwright install chromium
   py main.py --server
   ```

2. **למלא `.env` במחשב העבודה** (מערכים שצריך):
   ```
   COPILOT_TOKEN_ENDPOINT=https://defaultf4c80c7ce1aa40908a5dc87dde95d0.ee.environment.api.powerplatform.com/powervirtualagents/botsbyschema/crbf3_ESBTestscripter/directline/token?api-version=2022-03-01-preview
   AZURE_OPENAI_ENDPOINT=https://qa-ai-hero-foundry.services.ai.azure.com/
   AZURE_OPENAI_KEY=<from Foundry portal>
   AZURE_OPENAI_DEPLOYMENT=gpt-4.1-mini
   RUNNER_MODE=esb
   ```

3. **לבדוק את ה-flow המלא ברשת מכבי**:
   - localhost:8000 → WebChat נטען
   - להעלות spec → לאשר → לקבל JSON → Phase B → קריאות HTTP אמיתיות ל-ESB
   - לראות בלוג של השרת: `esb_request_start` + `esb_request_done` עם status codes אמיתיים

4. **(אופציונלי) Python ADO Agent** — אם תרצה אחסון אוטומטי ב-ADO:
   - להוסיף 3 שיטות ל-`ado_client.py` (create_suite, add_cases, set_result)
   - לעדכן את ה-pipeline שיקרא אותם אוטומטית
   - לא דורש שינוי בסוכן Copilot Studio

5. **(אופציונלי) Power Automate flow לסוכן** — אם רוצה את האחסון ב-ADO בלי לפתח Python:
   - Tools → Add Power Automate flow (אותו flow מהסוכן הישן)
   - Connections → "Use creator's credentials"
   - אז הסוכן יחזיר גם suite_id וגם JSON

---

## 11. קבצי הקשר נוספים

- **`README.md`** — תיעוד הרצה למשתמש (פשוט)
- **plan file** ב-Claude's plans dir — אם תרצה את ההיסטוריה המלאה של ה-planning בכל סשן
- **קוד**: כל מודול עם docstring בראש שמסביר מה הוא עושה

---

## 12. כללי עבודה לסשן הבא (חשוב!)

1. **קרא את המסמך הזה ראשון** לפני שאתה עושה שינויים
2. **לפני שאתה מתחיל לבנות אופציה חדשה — תבדוק שאופציה קיימת לא עובדת** (היו לנו בסשן הזה הרבה pivots — iframe → SDK → embed → Custom Canvas)
3. **תמיד תוודא שה-tests עוברים** (`cd qa-ai-hero && py -m pytest`) אחרי כל שינוי קוד
4. **קוד שמתעדכן ב-`smart_compiler.py` ו-`esb_pipeline.py`** הם הליבה — שינויים שם משפיעים על הכל
5. **לעולם לא לבצע commit ל-.env** — `.gitignore` כבר מטפל
6. **המשתמש עובד גם מהבית (לא ברשת מכבי) וגם מעבודה** — מסלולים שדורשים VPN/רשת פנימית יעבדו רק מעבודה

---

## 13. תקציר במשפט אחד

נבנתה מערכת QA אוטומטית עם Custom Canvas של Bot Framework WebChat לאינטראקציה עם סוכן Copilot Studio, ו-Smart Compiler מבוסס gpt-4.1-mini שמפרסר test cases חופשיים ל-HTTP requests מבוצעים אמיתית — כל זה ב-localhost:8000 בלי תלות באף שירות חיצוני מורכב.
