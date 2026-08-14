import threading

import pytest
from redis.exceptions import ConnectionError
from sqlalchemy.exc import SQLAlchemyError

import health
from config import REQUIRED_ENVIRONMENT_VARIABLES, validate_environment
from rabbitmq_publisher import RabbitMQPublisher


class HealthyDatabase:
    def execute(self, statement):
        return None


class UnhealthyDatabase:
    def execute(self, statement):
        raise SQLAlchemyError("database unavailable")


def test_postgres_health_reports_connected():
    result = health.check_postgres(HealthyDatabase())

    assert result.healthy is True
    assert result.detail == "connected"


def test_postgres_health_reports_sqlalchemy_error():
    result = health.check_postgres(UnhealthyDatabase())

    assert result.healthy is False
    assert result.detail == "SQLAlchemyError"


def test_redis_health_reports_connected(monkeypatch):
    monkeypatch.setattr(
        health.redis_client,
        "ping",
        lambda: True,
    )

    result = health.check_redis()

    assert result.healthy is True
    assert result.detail == "connected"


def test_redis_health_reports_redis_error(monkeypatch):
    def fail_ping():
        raise ConnectionError("redis unavailable")

    monkeypatch.setattr(
        health.redis_client,
        "ping",
        fail_ping,
    )

    result = health.check_redis()

    assert result.healthy is False
    assert result.detail == "ConnectionError"


def test_environment_validation_reports_all_missing_variables(monkeypatch):
    for variable in REQUIRED_ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(variable, raising=False)

    with pytest.raises(
        RuntimeError,
        match=(
            "Missing required environment variables: "
            "DATABASE_URL, REDIS_URL, RABBITMQ_URL"
        ),
    ):
        validate_environment()


def test_publisher_exposes_lifecycle_state():
    publisher = RabbitMQPublisher(max_pending_requests=1)

    assert publisher.is_running is False
    assert publisher.is_connected is False
    assert publisher.pending_count == 0

    publisher.start()

    assert publisher.is_running is True
    assert isinstance(publisher._thread, threading.Thread)

    publisher.stop()

    assert publisher.is_running is False
    assert publisher.pending_count == 0
