"""DotNet Runner — מבצע DotNetExecutableTestCase: Kafka publish/wait + Couchbase wait.

תלוי ב-confluent-kafka + couchbase Python SDK. אם הם לא מותקנים (JFrog blocked),
ה-runner מחזיר BLOCKED עם הסבר ברור — לא קורס את הפייפליין.
"""

from __future__ import annotations

import asyncio
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

        for action in executable.actions:
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

            step_results.append(step)
            observations.append({"action": action.model_dump(), "observation": obs})

            if step.status == TestStatus.FAILED:
                overall_status = TestStatus.FAILED
                error_message = error_message or step.error_message
            elif step.status == TestStatus.BLOCKED and overall_status != TestStatus.FAILED:
                overall_status = TestStatus.BLOCKED
                error_message = error_message or step.error_message

        duration = time.perf_counter() - started

        # api_response בפורמט generic — UI יודע לקרוא {actions, observations}
        api_response: Dict[str, Any] = {
            "status": 200 if overall_status == TestStatus.PASSED else 0,
            "kind": "dotnet",
            "observations": observations,
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

        client = KafkaRestClient()
        result = await client.produce(action.topic, key, action.value, action.headers)

        if "error" in result:
            classified = _classify_kafka_error(result["error"], action.topic, "publish")
            result["classified"] = classified
            step = StepResult(
                step=f"PUBLISH topic={action.topic} (REST)",
                expected_result="delivered",
                actual_result=f"❌ {classified['friendly']}",
                # auth/ACL → BLOCKED (תשתית); שאר → FAILED
                status=TestStatus.BLOCKED if classified["is_fatal_infra"] else TestStatus.FAILED,
                error_message=f"{classified['friendly']}\n→ {classified['recommendation']}",
            )
            return step, result

        step = StepResult(
            step=f"PUBLISH topic={action.topic} (REST) key={key}",
            expected_result="delivered",
            actual_result=f"offset={result.get('offset')} partition={result.get('partition')}",
            status=TestStatus.PASSED,
            response_dump=result,
        )
        return step, result

    # ============================================================
    # Kafka wait
    # ============================================================

    async def _run_kafka_wait(self, action: KafkaWaitAction):
        # ★ נרמול topic ל-lowercase (case-sensitive ב-Kafka; ACL בנוי על השם הקטן)
        action.topic = _normalize_topic(action.topic)
        if not settings.kafka_enabled:
            return self._blocked_step(
                f"WAIT topic={action.topic}",
                "Kafka not configured (KAFKA_BOOTSTRAP_SERVERS / KAFKA_REST_PROXY_URL ריקים)",
            )

        # ★ מסלול REST Proxy (מועדף כשמוגדר)
        if settings.kafka_rest_enabled:
            from agents.runner.kafka_rest_client import KafkaRestClient
            observed = await KafkaRestClient().consume(
                action.topic, action.match, action.timeout_seconds,
                settings.KAFKA_CONSUMER_GROUP_PREFIX,
            )
            # consumer API כבוי ב-deployment → BLOCKED עם הסבר
            if isinstance(observed, dict) and observed.get("rest_consumer_unavailable"):
                return self._blocked_step(
                    f"WAIT topic={action.topic} (REST)",
                    "ה-consumer API של ה-REST Proxy לא זמין ({}). בקש מ-admin להפעיל אותו "
                    "(kafka-rest consumer endpoints), או הגדר KAFKA_BOOTSTRAP_SERVERS "
                    "למסלול native.".format(observed.get("detail", "404/501")),
                )
        else:
            try:
                from confluent_kafka import Consumer  # type: ignore[import-not-found]
            except ImportError:
                return self._blocked_step(
                    f"WAIT topic={action.topic}",
                    "confluent-kafka package not installed",
                )

            group_id = f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}-{uuid.uuid4().hex[:8]}"
            conf = self._kafka_conf()
            conf.update({
                "group.id": group_id,
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            })

            observed = await asyncio.to_thread(
                self._consume_until_match,
                Consumer, conf, action.topic, action.match, action.timeout_seconds,
            )

        # ★ שגיאת תשתית/ACL → דווח מיד עם הסבר ידידותי
        if isinstance(observed, dict) and "fatal_error" in observed:
            classified = _classify_kafka_error(observed["fatal_error"], action.topic, "consume")
            observed["classified"] = classified
            step = StepResult(
                step=f"WAIT topic={action.topic}",
                expected_result="message arrives",
                actual_result=f"❌ {classified['friendly']}",
                status=TestStatus.BLOCKED,  # BLOCKED ולא FAILED — זה issue של תשתית
                error_message=f"{classified['friendly']}\n→ {classified['recommendation']}",
            )
            return step, observed

        # תרחיש שלילי: timeout = PASS, מסר שהגיע = FAIL
        if action.expect_no_message:
            if observed is None:
                step = StepResult(
                    step=f"WAIT NO-MESSAGE topic={action.topic} ({action.timeout_seconds}s)",
                    expected_result="no message (negative test)",
                    actual_result="no message arrived — as expected",
                    status=TestStatus.PASSED,
                    response_dump={"timeout": True, "expected_silence": True},
                )
                return step, {"timeout": True, "expected_silence": True}
            step = StepResult(
                step=f"WAIT NO-MESSAGE topic={action.topic}",
                expected_result="no message (negative test)",
                actual_result=f"message arrived (offset={observed.get('offset')}) — should NOT have arrived",
                status=TestStatus.FAILED,
                error_message="unexpected message in negative test",
                response_dump=observed,
            )
            return step, observed

        if observed is None:
            step = StepResult(
                step=f"WAIT topic={action.topic} (timeout {action.timeout_seconds}s)",
                expected_result="message arrived matching " + json.dumps(action.match, ensure_ascii=False),
                actual_result="timeout — no matching message",
                status=TestStatus.FAILED,
                error_message="message did not arrive within timeout",
            )
            return step, {"timeout": True, "match": action.match}

        # אסרשנים על שדות צפויים
        missing = _check_expected_fields(observed.get("value_parsed") or {}, action.expected_fields)
        if missing:
            step = StepResult(
                step=f"WAIT topic={action.topic}",
                expected_result=json.dumps(action.expected_fields, ensure_ascii=False),
                actual_result=json.dumps(observed.get("value_parsed") or {}, ensure_ascii=False)[:300],
                status=TestStatus.FAILED,
                error_message="missing/mismatched fields: " + ", ".join(missing),
                response_dump=observed,
            )
            return step, observed

        step = StepResult(
            step=f"WAIT topic={action.topic}",
            expected_result="message matched + fields ok",
            actual_result=f"offset={observed.get('offset')} fields_ok",
            status=TestStatus.PASSED,
            response_dump=observed,
        )
        return step, observed

    @staticmethod
    def _consume_until_match(
        Consumer,
        conf: Dict[str, Any],
        topic: str,
        match: Dict[str, Any],
        timeout_seconds: int,
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


def _check_expected_fields(value: Dict[str, Any], expected: Dict[str, Any]) -> List[str]:
    """מחזיר רשימה של שדות שחסרים / לא תואמים. ריק = הכל בסדר."""
    issues: List[str] = []
    if not expected:
        return issues
    if not isinstance(value, dict):
        return list(expected.keys())
    for k, want in expected.items():
        if k not in value:
            issues.append(f"{k} (missing)")
            continue
        actual = value[k]
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

    if "TOPIC_AUTHORIZATION_FAILED" in s or is_rest_authz:
        op = "Write" if action == "publish" else "Read"
        out["friendly"] = (
            f"אין הרשאת ACL {op} ל-topic '{topic}'. ה-user שלך מזדהה בהצלחה אבל "
            f"Kafka דוחה את הפעולה."
        )
        out["recommendation"] = (
            f"בקש מ-admin של Kafka להוסיף ACL:\n"
            f"  kafka-acls --add --{('producer' if action == 'publish' else 'consumer')} "
            f"--topic {topic} --principal User:{settings.KAFKA_SASL_USERNAME or '<your-user>'}"
            + (f" --group {settings.KAFKA_CONSUMER_GROUP_PREFIX}-*" if action == "consume" else "")
        )
        out["is_fatal_infra"] = True
    elif "GROUP_AUTHORIZATION_FAILED" in s:
        out["friendly"] = (
            f"אין הרשאת ACL ל-consumer group. ה-user שלך לא יכול לצרוך עם group "
            f"prefix '{settings.KAFKA_CONSUMER_GROUP_PREFIX}'."
        )
        out["recommendation"] = (
            f"בקש מ-admin להוסיף ACL: "
            f"kafka-acls --add --consumer --topic {topic} --group "
            f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}-* --principal User:{settings.KAFKA_SASL_USERNAME or '<your-user>'}"
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
