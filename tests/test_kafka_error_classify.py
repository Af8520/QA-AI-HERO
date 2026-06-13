"""טסטים לסיווג שגיאות Kafka — מסר ידידותי + early stop בפייפליין."""

from __future__ import annotations

import os

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "")

from agents.runner.dotnet_runner import _classify_kafka_error  # noqa: E402


def test_topic_authorization_failed_publish():
    err = 'KafkaError{code=TOPIC_AUTHORIZATION_FAILED,val=29,str="Broker: ..."}'
    out = _classify_kafka_error(err, topic="Clicks-referral-streaming", action="publish")
    assert out["is_fatal_infra"] is True
    assert "Write" in out["friendly"]
    assert "Clicks-referral-streaming" in out["friendly"]
    assert "kafka-acls" in out["recommendation"]
    assert "--producer" in out["recommendation"]


def test_topic_authorization_failed_consume():
    err = "KafkaError{code=TOPIC_AUTHORIZATION_FAILED,...}"
    out = _classify_kafka_error(err, topic="Patient-raw", action="consume")
    assert out["is_fatal_infra"] is True
    assert "Read" in out["friendly"]
    assert "--consumer" in out["recommendation"]


def test_group_authorization_failed():
    err = "KafkaError{code=GROUP_AUTHORIZATION_FAILED,...}"
    out = _classify_kafka_error(err, topic="x", action="consume")
    assert out["is_fatal_infra"] is True
    assert "consumer group" in out["friendly"]


def test_sasl_authentication_failed():
    err = "KafkaError{code=SASL_AUTHENTICATION_FAILED,...}"
    out = _classify_kafka_error(err, topic="x", action="publish")
    assert out["is_fatal_infra"] is True
    assert "credentials" in out["friendly"]
    assert "KAFKA_SASL_USERNAME" in out["recommendation"]


def test_unknown_topic():
    err = "KafkaError{code=UNKNOWN_TOPIC_OR_PART,...}"
    out = _classify_kafka_error(err, topic="missing-topic", action="publish")
    assert out["is_fatal_infra"] is True
    assert "missing-topic" in out["friendly"]
    assert "לא קיים" in out["friendly"]


def test_unknown_error_is_not_fatal():
    err = "Some random Kafka error"
    out = _classify_kafka_error(err, topic="x", action="publish")
    assert out["is_fatal_infra"] is False
    assert out["friendly"] == "Some random Kafka error"
