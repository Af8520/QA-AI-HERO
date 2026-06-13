"""טסטים לסיווג שגיאות Kafka — מסר ידידותי + early stop בפייפליין."""

from __future__ import annotations

import os

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "")

from agents.runner.dotnet_runner import (  # noqa: E402
    _classify_kafka_error,
    _parse_group_from_error,
    _resolve_consumer_group,
)


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


def test_rest_group_authz_403():
    """REST proxy: 403 + 'access group' → group branch (לא topic), עם שם ה-group."""
    err = 'HTTP 403 on records: {"error_code":40301,"message":"Not authorized to access group: worker.cb.catalog-99dabfd0"}'
    out = _classify_kafka_error(err, topic="patient_parameters-raw", action="consume")
    assert out["is_fatal_infra"] is True
    assert "consumer group" in out["friendly"]
    assert "worker.cb.catalog-99dabfd0" in out["friendly"]
    # ההמלצה מציעה KAFKA_CONSUMER_GROUP (לא topic ACL בלבד)
    assert "KAFKA_CONSUMER_GROUP" in out["recommendation"]


def test_rest_topic_authz_403_no_group():
    """REST proxy: 403 בלי 'group' → topic branch."""
    err = 'HTTP 403 on records: {"error_code":40301,"message":"Not authorized to access topic"}'
    out = _classify_kafka_error(err, topic="some-topic", action="consume")
    assert out["is_fatal_infra"] is True
    assert "topic" in out["friendly"]
    assert "some-topic" in out["friendly"]


def test_parse_group_from_error():
    err = '{"error_code":40301,"message":"Not authorized to access group: worker.cb.catalog-99dabfd0"}'
    assert _parse_group_from_error(err) == "worker.cb.catalog-99dabfd0"
    assert _parse_group_from_error("no group here") is None


def test_resolve_consumer_group_exact(monkeypatch):
    from config.settings import settings
    monkeypatch.setattr(settings, "KAFKA_CONSUMER_GROUP", "exact-group", raising=False)
    assert _resolve_consumer_group() == "exact-group"


def test_resolve_consumer_group_prefix(monkeypatch):
    from config.settings import settings
    monkeypatch.setattr(settings, "KAFKA_CONSUMER_GROUP", None, raising=False)
    monkeypatch.setattr(settings, "KAFKA_CONSUMER_GROUP_PREFIX", "qa-ai-hero", raising=False)
    g = _resolve_consumer_group()
    assert g.startswith("qa-ai-hero-")
    assert len(g) > len("qa-ai-hero-")
