"""DotNet Runner — מבצע DotNetExecutableTestCase: Kafka publish/wait + Couchbase wait.

תלוי ב-confluent-kafka + couchbase Python SDK. אם הם לא מותקנים (JFrog blocked),
ה-runner מחזיר BLOCKED עם הסבר ברור — לא קורס את הפייפליין.
"""

from __future__ import annotations

import asyncio
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
        if not action.match and not action.key_equals and not action.key_contains:
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
            self._log("MATCH", "error",
                      f"לא נמצא מסר תואם ל-{json.dumps(action.match, ensure_ascii=False)} מתוך {len(candidates)} מסרים")
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


def _resolve_field_path(obj: Any, path: str) -> Any:
    """כמו _resolve_raw_path, אבל סובלני להבדל logical↔wire ול-list ללא index:
    - 'root.X' שלא נמצא → ננסה 'X' ברמה העליונה (כי root משוטח ב-wire).
    - 'headers.X' שלא נמצא → ננסה 'header.X' (header יחיד ב-wire).
    - 'a.list.field' (list ללא index) → auto-index ל-[0].
    ככה אסרשנים עובדים בין אם המוח פלט root.X/headers.X/בלי index ובין אם המסר ב-wire format.
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
    return val


def _is_producer_metadata_key(k: str) -> bool:
    """True ל-header.mac_* / headers.mac_* — metadata של ה-producer (mac_sys_name, mac_producer_name,
    mac_app_*, mac_channel, mac_correlation_id...). אלה *לא* טרנספורמציה תחת-בדיקה, וה-LLM אינו יודע
    את ערכיהם (הם של ה-Worker) → לא לאמת אותם (rest ביטחון; ה-compiler כבר לא אמור לפלוט אותם)."""
    kl = k.lower()
    return kl.startswith("header.mac_") or kl.startswith("headers.mac_")


def _check_expected_fields(value: Dict[str, Any], expected: Dict[str, Any]) -> List[str]:
    """מחזיר רשימה של שדות שחסרים / לא תואמים. ריק = הכל בסדר.
    מפתח עם נקודה ('root.action', '_data.parameters.0.gender') נחשב dotted path מקונן.
    ★ שדות header.mac_* (metadata של ה-producer) מדולגים — לא הטרנספורמציה הנבדקת.
    """
    issues: List[str] = []
    if not expected:
        return issues
    if not isinstance(value, (dict, list)):
        return [k for k in expected.keys() if not _is_producer_metadata_key(k)]
    for k, want in expected.items():
        if _is_producer_metadata_key(k):
            continue                  # metadata של ה-producer → soft (מדלגים)
        actual = _resolve_field_path(value, k) if "." in k else (
            value.get(k, _FIELD_MISSING) if isinstance(value, dict) else _FIELD_MISSING
        )
        if actual is _FIELD_MISSING:
            issues.append(f"{k} (missing)")
            continue
        if str(actual) != str(want):
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
