# QA-AI-Hero — Maccabi Healthcare (ESB MVP)

מערכת אוטומטית להחלפת בודקי QA. פיילוט: מחלקת אינטגרציה / ESB.

> **לקונטקסט מלא — קרא את [SESSION_HANDOFF.md](SESSION_HANDOFF.md).**

---

## מה זה עושה

1. **Phase A (אינטראקטיבי):** משתמש מעלה מסמך אפיון בצ'אט שלנו (Custom Canvas של Bot Framework WebChat). סוכן Copilot Studio מייצר test cases בעברית עם URL+method מלאים.
2. **Phase B (אוטומטי, 7 שלבים):**
   - Smart Compiler (gpt-4.1-mini) מפרסר כל test case ל-HTTP request מובנה
   - מבצע את הקריאות מול ה-ESB API (`httpx`)
   - מאמת תשובות + Kafka + Elastic
   - מנתח כשלים → פותח bugs ב-ADO (אחרי אישור משתמש)
   - מחזיר סיכום בעברית

---

## דרישות

- **Python 3.9+** (בעבודה: `py` במקום `python`. בדוק עם `py --version`)
- **גישה לרשת מכבי** (לקריאות ESB אמיתיות) — מהבית אפשר רק במצב mock
- **Azure AI Foundry** עם מודל `gpt-4.1-mini` פרוס
- **Copilot Studio agent** (`crbf3_ESBTestscripter`, No-auth, ללא Tools)

---

## התקנה במחשב חדש

```powershell
git clone https://github.com/Af8520/QA-AI-HERO.git
cd QA-AI-HERO\qa-ai-hero

# וירטואלי (מומלץ)
py -m venv .venv
.venv\Scripts\activate

# התקנת תלויות
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
py -m playwright install chromium

# אם requirements.txt נכשל על חבילה כלשהי (במיוחד azure-ai-agents או
# microsoft-agents-copilotstudio-client על Python 3.9) — תשתמש בגרסת ה-core:
# py -m pip install -r requirements-core.txt

# קונפיגורציה
copy .env.example .env
notepad .env   # מלא לפי הסקציה למטה
```

---

## קונפיגורציה מינימלית של `.env`

```env
# Phase A — Custom Canvas (★ מסלול ראשי)
COPILOT_TOKEN_ENDPOINT=https://defaultf4c80c7ce1aa40908a5dc87dde95d0.ee.environment.api.powerplatform.com/powervirtualagents/botsbyschema/crbf3_ESBTestscripter/directline/token?api-version=2022-03-01-preview

# Phase B — Foundry chat completions
AZURE_OPENAI_ENDPOINT=https://qa-ai-hero-foundry.services.ai.azure.com/
AZURE_OPENAI_KEY=<מ-Foundry portal>
AZURE_OPENAI_DEPLOYMENT=gpt-4.1-mini

# Runner mode
RUNNER_MODE=esb     # esb=קריאות אמיתיות (דורש רשת מכבי). mock=דמה
```

לכל ההגדרות — ראה [`.env.example`](.env.example).

---

## הרצה

```powershell
py main.py --server
```

פתח: http://localhost:8000

### Flow מלא:
1. WebChat נטען עם הסוכן `ESB Test scripter`
2. לחץ 📎 בצ'אט → העלה מסמך אפיון (Word/PDF)
3. הסוכן ישאל על test data — תן/דלג
4. הסוכן מציג טבלת test cases → תאשר ("תקין")
5. הסוכן מבקש US 6-ספרות → תיתן
6. הסוכן מחזיר JSON של 20-35 test cases
7. **המערכת מזהה אוטומטית** ועוברת ל-Phase B
8. תראה ב-chat שלנו את 7 השלבים רצים → טבלת תוצאות → דיאלוג אישור bugs

---

## פקודות נוספות

```powershell
py main.py --test-bridge       # בדיקה ישירה של CopilotBridge
py main.py --test-postman <path>   # בדיקת פרסור Postman Collection
py -m pytest tests/             # 23 unit tests
py -m compileall .              # syntax check על כל הקוד
```

---

## מצבי פעולה (RUNNER_MODE)

| Mode | פעילות | מתי להשתמש |
|---|---|---|
| `mock` | מגריל תוצאות (~70% הצלחה) | פיתוח/דמו מהבית, אין רשת מכבי |
| `esb` | קריאות HTTP אמיתיות (httpx) | בעבודה — בודק את ה-API באמת |

ב-`esb` mode השרת ידפיס לכל request:
```
esb_request_start  method=POST url=http://esb-lb-test.mac.org.il:5555/...
esb_request_done   status=200 duration_ms=145
```

---

## ארכיטקטורה

```
┌───────── Phase A (Copilot Studio) ─────────┐
│  WebChat → 📎 spec → טבלה → אישור → JSON   │
└─────────────────────────────────────────────┘
                  ↓ (auto-detect JSON)
┌───────── Phase B (Python pipeline) ────────┐
│  1. Compile (gpt-4.1-mini)                  │
│  2. Execute (httpx → ESB API)               │
│  3. Verify Kafka + Elastic                  │
│  4. Validate + Bugs + ADO                   │
│  5. Report                                  │
└─────────────────────────────────────────────┘
```

תיאור מלא של הרכיבים, הקבצים, ו-flow ההיסטורי — [SESSION_HANDOFF.md](SESSION_HANDOFF.md).

---

## פתרון בעיות

| בעיה | פתרון |
|---|---|
| `webchat.js failed to load` | בדוק חיבור אינטרנט — ה-CDN של Microsoft |
| `AADSTS650057` | App Registration חסר הרשאה — Custom Canvas עוקף, תוודא ש-`COPILOT_TOKEN_ENDPOINT` מאוכלס |
| כל ה-tests מסומנים `about:blank` | חסר `AZURE_OPENAI_*` או הסוכן לא רושם URLs ב-steps |
| Timeout על קריאות ESB | אתה לא ברשת מכבי — VPN או הפעל `RUNNER_MODE=mock` |
| `pip install` נכשל על Hebrew comments | תוודא ש-`requirements.txt` באנגלית בלבד (כבר מתוקן) |

---

## רישיון ופרטיות

פנימי בלבד — Maccabi Healthcare Services.
`.env` עם API keys — לעולם לא ל-git (`gitignored`).
