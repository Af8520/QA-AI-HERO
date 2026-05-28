# Copilot Studio Agent — Integration with QA-AI-Hero Phase B

מסמך זה מתאר את השינויים הנדרשים בסוכן `.NET -QA Test Generator` ב-Copilot Studio
כדי לתמוך בארכיטקטורה החדשה של Phase B (Smart Compiler).

## רקע

Phase B (`pipeline/esb_pipeline.py`) מצפה למצוא **מסמך אפיון מובנה (Markdown)** מצורף
כ-attachment ל-Test Suite ב-ADO. ה-Smart Compiler משתמש בו כדי להבין את ה-API
ולחבר request HTTP מדויק לכל test case.

ללא MD — ה-Compiler עובד עם Postman template + תיאור התסריט בלבד, אבל הדיוק יורד.

## שינוי 1 — System Prompt

הוסף לסוכן בסעיף ה-Instructions/Prompt את הסעיף הבא:

```
בנוסף לתסריטי הבדיקה, בכל פעם שאתה מנתח אפיון API,
הפק מסמך **Markdown מובנה** של ה-API לפי המבנה הבא, וצרף אותו ל-Power Automate Flow
להעלאה כ-attachment ל-Test Suite שייווצר ב-ADO (filename: spec.md).

המבנה הנדרש:

# API Spec: <שם ה-API>

## Endpoint(s)
לכל endpoint:
- **<METHOD> <path>** — תיאור קצר

## Request — Required fields
- `field_name` (type, constraints) — תיאור
  - דוגמה: `member_id` (int, 9 digits, starts with 3) — מספר חבר במכבי

## Request — Optional fields
- `field_name` (type) — תיאור

## Validation rules
- חוקי ולידציה עסקיים ספציפיים שלא משתקפים בטיפוס

## Response (happy path)
- status: 200
- body: schema קצר

## Error responses
- 400 — ולידציה כשלה
- 404 — משאב לא נמצא
- 500 — שגיאת שרת

## Kafka Topics (אם רלוונטי)
- `topic-name` — מתי נכתב + format מסר

## Logging (Elastic)
- **index**: `esb-logs-*`
- **חובה**: log INFO לכל request מוצלח, כולל patientId
- **אסור**: log ERROR אלא אם 5xx
```

## שינוי 2 — Power Automate Flow

ה-Flow הקיים (זה שיוצר את ה-Test Suite ומכניס Test Cases) — להוסיף לו צעד אחד נוסף:

### שלב חדש: Add MD attachment to Test Suite

אחרי ה-`Create Test Suite` ולפני סיום ה-flow:

1. **Action**: Azure DevOps → "Add an attachment to a work item"
2. **פרמטרים**:
   - **Organization**: ${ADO_ORG}
   - **Project**: ${ADO_PROJECT}
   - **Work Item ID**: `outputs('Create_Test_Suite')?['body/id']`  (ה-suite שנוצר)
   - **File Name**: `spec.md`
   - **File Content**: התוצאה של ה-LLM שייצר את ה-MD (variable `spec_markdown` שצריך להגדיר ב-flow)
   - **Content Type**: `text/markdown`

### הזרימה המלאה ב-Flow

```
Trigger (call from Agent topic)
   ↓
Compose: build US folder name
   ↓
Create Test Suite in ADO              ← שלב קיים
   ↓
For each test case:
   Create Test Case work item         ← שלב קיים
   Add to Test Suite                  ← שלב קיים
   ↓
Add Attachment to Test Suite          ← ★ שלב חדש (spec.md)
   ↓
Return success message to agent
```

## שינוי 3 — Topic / Variable

הסוכן צריך להחזיק variable בשם `spec_markdown` שמכיל את ה-MD שנוצר.
פתרון מומלץ:

1. ב-Topic של ניתוח האפיון, אחרי שהסוכן יוצר את ה-test cases JSON,
   הוסף קריאה ל-`Generate text with AI prompt` (action פנימי של Copilot Studio):
   - Input: `spec_content` (תוכן המסמך)
   - Prompt: "Generate structured Markdown for this API spec following the format in the agent instructions"
   - Output → variable `spec_markdown`
2. העבר את `spec_markdown` כ-input ל-Power Automate Flow.

## אימות אחרי הפריסה

1. הרץ את הסוכן ב-**Test pane** (כדי לעקוף בעיות הרשאות)
2. עבור את כל ה-flow עד הצלחה והעלאה ל-ADO
3. ב-ADO → Test Plans → ה-suite שנוצר → טאב **Attachments** → אמור להופיע `spec.md`
4. הורד את ה-`spec.md` והוודא שהמבנה תואם למה שמתואר למעלה

אם הכול תקין → הרץ את `localhost:8000` של QA-AI-Hero, הזן את ה-suite_id, ולחץ
"התחל Phase B". בלוג של Phase B תראה:
```
שלב 2/7 — מושך מסמך אפיון (MD) מ-ADO...
  ✓ נטען MD (XXXX תווים)
שלב 3/7 — מהדר N תסריטים לבקשות HTTP...
  ✓ TC-001 → POST https://api/...
```

## Fallback אם MD לא הועלה

אם ה-Suite לא מכיל `spec.md` (suite ישן מלפני העדכון, או שגיאה ב-flow), Phase B ימשיך
לרוץ אבל ה-Smart Compiler ישתמש רק ב-Postman template + תיאור התסריט. הוא ידפיס
warning מפורש:
```
⚠ אין MD ב-suite — Compiler ירוץ ללא הקשר ספק (דיוק יורד)
```

המערכת לא תיכשל — רק ה-mutations המורכבות יותר עלולות לא להיות מדויקות
("שדה X חייב להתחיל ב-3" בלי המידע הזה — ה-Compiler לא יידע מה להציב).
