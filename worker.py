import json
import os
import logging
import signal
import threading
import time
from datetime import datetime, timezone

import pika
from dotenv import load_dotenv
from prometheus_client import start_http_server
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from providers.factory import get_provider
from logging_config import configure_logging
from metrics import (
    DEAD_LETTERED,
    DELIVERY_ATTEMPTS,
    DELIVERY_FAILURES,
    DELIVERY_SUCCESS,
    END_TO_END_DELIVERY_DURATION,
    PROVIDER_DURATION,
    RETRIES_SCHEDULED,
    WORKER_HEALTH,
    WORKER_JOBS_IN_PROGRESS,
    WORKER_PROCESSING_DURATION,
    WORKER_RABBITMQ_CONNECTED,
)

load_dotenv()

configure_logging(
    service_name="notification-worker",
    log_level=os.getenv("LOG_LEVEL", "INFO"),
)
logger = logging.getLogger("notification-worker")

MAIN_EXCHANGE = "notification.exchange"
MAIN_QUEUE = "notification.delivery"
MAIN_ROUTING_KEY = "notification.delivery"

RETRY_EXCHANGE = "notification.retry.exchange"
RETRY_QUEUE = "notification.delivery.retry"
RETRY_ROUTING_KEY = "notification.delivery.retry"

DLX_EXCHANGE = "notification.dlx"
DLQ_QUEUE = "notification.delivery.dlq"
DLQ_ROUTING_KEY = "notification.delivery.dead"



RABBITMQ_URL = os.getenv("RABBITMQ_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

MAX_ATTEMPTS = 4

PRIORITY_MAP = {
    "LOW": 1,
    "NORMAL": 5,
    "HIGH": 10,
}

shutdown_requested = threading.Event()


class NotificationWorker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.shutting_down = False

    def stop(self):
        self.shutting_down = True
        WORKER_HEALTH.set(0)

        if (
            self.connection is not None
            and self.connection.is_open
            and self.channel is not None
        ):
            self.connection.add_callback_threadsafe(
                self.channel.stop_consuming
            )


worker = NotificationWorker()


def request_shutdown(signum, frame):
    logger.warning(
        "worker_shutdown_requested",
        extra={
            "signal": signum,
        },
    )

    shutdown_requested.set()
    worker.stop()

def declare_topology(channel) -> None:
    # Main exchange and queue
    channel.exchange_declare(
        exchange=MAIN_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    channel.queue_declare(
        queue=MAIN_QUEUE,
        durable=True,
        arguments={
            "x-max-priority": 10,
        },
    )

    # This line:

    # "x-max-priority": 10

    # tells RabbitMQ:

    # This queue supports priorities from 0 through 10.

    channel.queue_bind(
        queue=MAIN_QUEUE,
        exchange=MAIN_EXCHANGE,
        routing_key=MAIN_ROUTING_KEY,
    )

    # Retry exchange
    channel.exchange_declare(
        exchange=RETRY_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    # Messages that expire in this queue are sent back to the main exchange.
    channel.queue_declare(
        queue=RETRY_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": MAIN_EXCHANGE,# When a message expires in the retry queue, send it to the main exchange.
            "x-dead-letter-routing-key": MAIN_ROUTING_KEY,
        },
    )

    channel.queue_bind(
        queue=RETRY_QUEUE,
        exchange=RETRY_EXCHANGE,
        routing_key=RETRY_ROUTING_KEY,
    )

    # Dead-letter exchange and final DLQ
    channel.exchange_declare(
        exchange=DLX_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    channel.queue_declare(
        queue=DLQ_QUEUE,
        durable=True,
    )

    channel.queue_bind(
        queue=DLQ_QUEUE,
        exchange=DLX_EXCHANGE,
        routing_key=DLQ_ROUTING_KEY,
    )

def publish_retry(
    channel,
    notification_id: str,
    next_attempt_no: int,
    delay_ms: int,
    priority: str,
    request_id: str | None,
) -> None:
    rabbitmq_priority = PRIORITY_MAP.get(priority, 5)
    message = {
        "notification_id": notification_id,
        "attempt_no": next_attempt_no,
        "priority": priority,
        "request_id": request_id,
    }

    channel.basic_publish(
        exchange=RETRY_EXCHANGE,
        routing_key=RETRY_ROUTING_KEY,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            priority=rabbitmq_priority,

            # Pika/RabbitMQ expects expiration as a string in milliseconds.
            expiration=str(delay_ms),
        ),
    )
# Pika's BasicProperties supports an expiration field for per-message TTL.
# means how much time to wait in retry queue before it sends it to worker

def publish_to_dlq(
    channel,
    notification_id: str,
    attempt_no: int,
    error_message: str,
    priority: str,
    request_id: str | None,
) -> None:
    message = {
        "notification_id": notification_id,
        "attempt_no": attempt_no,
        "error": error_message,
        "priority": priority,
        "request_id": request_id,
    }

    channel.basic_publish(
        exchange=DLX_EXCHANGE,
        routing_key=DLQ_ROUTING_KEY,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
    )
    
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)
# Worker receives message
# ↓
# Fetch notification
# ↓
# If SENT, ack and skip
# ↓
# If FAILED, ack and skip
# ↓
# Check previous attempts
# ↓
# If max attempts already reached, mark FAILED and ack
# ↓
# Create new attempt with PROCESSING
# ↓
# Mark notification PROCESSING
# ↓
# Resolve the provider for the stored channel and send
# ↓
# If success:
#     attempt = SUCCESS
#     notification = SENT
#     commit
#     ack
# ↓
# If failure and attempts left:
#     attempt = FAILED
#     notification = PENDING
#     commit
#     sleep with backoff
#     nack with requeue=True
# ↓
# If failure and no attempts left:
#     attempt = FAILED
#     notification = FAILED
#     commit
#     ack
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


def process_message(ch, method, properties, body) -> None:
    """Process one RabbitMQ notification job."""

    # ----------------------------------
    # 1. Validate RabbitMQ message
    # ----------------------------------

    notification_id = None
    request_id = None
    job_metric_started = False
    processing_start = time.perf_counter()
    processing_result = "unknown"
    processing_channel = "unknown"

    try:
        data = json.loads(body)

        notification_id = data["notification_id"]
        request_id = data.get("request_id")
        message_attempt_no = data.get("attempt_no", 1)
        priority = data.get("priority", "NORMAL")

        if not isinstance(notification_id, str):
            raise TypeError("notification_id must be a string")

        if not isinstance(message_attempt_no, int) or message_attempt_no < 1:
            raise TypeError("attempt_no must be a positive integer")



    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        KeyError,
        TypeError,
    ) as exc:
        logger.warning(
            "malformed_notification_job",
            extra={
                "service": "notification-worker",
                "request_id": request_id,
                "notification_id": notification_id,
                "action": "ack_and_discard",
                "error_type": type(exc).__name__,
            },
        )

        # Retrying an unchanged malformed message will never help.
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    WORKER_JOBS_IN_PROGRESS.inc()
    job_metric_started = True

    logger.info(
        "notification_job_received",
        extra={
            "service": "notification-worker",
            "request_id": request_id,
            "notification_id": notification_id,
            "message_attempt_no": message_attempt_no,
            "priority": priority,
            "redelivered": method.redelivered,
        },
    )

    db = SessionLocal()

    try:
        # ----------------------------------
        # 2. Fetch notification
        # ----------------------------------

        notification = db.execute(
            text(
                """
                SELECT
                    id,
                    channel,
                    recipient,
                    subject,
                    body,
                    priority,
                    status,
                    created_at
                FROM notification.notifications
                WHERE id = :notification_id
                """
            ),
            {"notification_id": notification_id},
        ).mappings().one_or_none()

        if notification is None:
            logger.warning(
                "notification_not_found",
                extra={
                    "service": "notification-worker",
                    "request_id": request_id,
                    "notification_id": notification_id,
                    "action": "ack_and_discard",
                },
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        processing_channel = notification["channel"]

        # ----------------------------------
        # 3. Basic worker idempotency checks
        # ----------------------------------

        if notification["status"] == "SENT":
            logger.warning(
                "notification_already_sent",
                extra={
                    "service": "notification-worker",
                    "request_id": request_id,
                    "notification_id": notification_id,
                    "action": "ack_and_skip",
                },
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if notification["status"] in {"FAILED", "DEAD_LETTERED"}:
            logger.warning(
                "notification_already_final",
                extra={
                    "service": "notification-worker",
                    "request_id": request_id,
                    "notification_id": notification_id,
                    "status": notification["status"],
                    "action": "ack_and_skip",
                },
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # ----------------------------------
        # 4. Determine next attempt number
        # ----------------------------------

        last_attempt_no = db.execute(
            text(
                """
                SELECT COALESCE(MAX(attempt_no), 0)
                FROM notification.notification_attempts
                WHERE notification_id = :notification_id
                """
            ),
            {"notification_id": notification_id},
        ).scalar_one()

        if last_attempt_no >= MAX_ATTEMPTS:
            db.execute(
                text(
                    """
                    UPDATE notification.notifications
                    SET
                        status = 'FAILED',
                        updated_at = NOW()
                    WHERE id = :notification_id
                    """
                ),
                {"notification_id": notification_id},
            )

            db.commit()

            logger.warning(
                "notification_max_attempts_reached",
                extra={
                    "service": "notification-worker",
                    "request_id": request_id,
                    "notification_id": notification_id,
                    "attempt_no": last_attempt_no,
                    "status": "FAILED",
                },
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        next_attempt_no = last_attempt_no + 1

        if message_attempt_no != next_attempt_no:
            logger.warning(
                "notification_attempt_mismatch",
                extra={
                    "service": "notification-worker",
                    "request_id": request_id,
                    "notification_id": notification_id,
                    "message_attempt_no": message_attempt_no,
                    "database_attempt_no": next_attempt_no,
                },
            )

        attempt_no = next_attempt_no

        # ----------------------------------
        # 5. Create a new attempt row
        # ----------------------------------

        attempt_id = db.execute(
            text(
                """
                INSERT INTO notification.notification_attempts (
                    notification_id,
                    attempt_no,
                    status,
                    error_message
                )
                VALUES (
                    :notification_id,
                    :attempt_no,
                    'PROCESSING',
                    NULL
                )
                RETURNING id
                """
            ),
            {
                "notification_id": notification_id,
                "attempt_no": attempt_no,
            },
        ).scalar_one()

        db.execute(
            text(
                """
                UPDATE notification.notifications
                SET
                    status = 'PROCESSING',
                    updated_at = NOW()
                WHERE id = :notification_id
                """
            ),
            {"notification_id": notification_id},
        )

        db.commit()

        logger.info(
            "notification_attempt_started",
            extra={
                "service": "notification-worker",
                "request_id": request_id,
                "notification_id": notification_id,
                "attempt_no": next_attempt_no,
                "channel": notification["channel"],
                "priority": notification["priority"],
            },
        )

        # ----------------------------------
        # 6. Deliver through the provider registered for this channel
        # ----------------------------------

        provider = None
        provider_name = "unknown"
        provider_start = time.perf_counter()

        try:
            provider = get_provider(notification["channel"])
            provider_name = provider.__class__.__name__
            DELIVERY_ATTEMPTS.labels(
                channel=notification["channel"],
                provider=provider_name,
                priority=notification["priority"],
            ).inc()

            try:
                provider.send(
                    recipient=notification["recipient"],
                    subject=notification["subject"],
                    body=notification["body"],
                )
            finally:
                provider_duration_seconds = time.perf_counter() - provider_start
                PROVIDER_DURATION.labels(
                    channel=notification["channel"],
                    provider=provider_name,
                ).observe(provider_duration_seconds)

            provider_duration_ms = round(
                provider_duration_seconds * 1000,
                2,
            )
            logger.info(
                "notification_delivery_succeeded",
                extra={
                    "service": "notification-worker",
                    "request_id": request_id,
                    "notification_id": notification_id,
                    "attempt_no": next_attempt_no,
                    "channel": notification["channel"],
                    "provider": provider.__class__.__name__,
                    "duration_ms": provider_duration_ms,
                },
            )

        except Exception as exc:
            error_message = str(exc)
            provider_duration_ms = round(
                (time.perf_counter() - provider_start) * 1000,
                2,
            )
            DELIVERY_FAILURES.labels(
                channel=notification["channel"],
                provider=provider_name,
                priority=notification["priority"],
                error_type=type(exc).__name__,
            ).inc()
            logger.exception(
                "notification_delivery_failed",
                extra={
                    "service": "notification-worker",
                    "request_id": request_id,
                    "notification_id": notification_id,
                    "attempt_no": next_attempt_no,
                    "channel": notification["channel"],
                    "provider": provider_name,
                    "duration_ms": provider_duration_ms,
                    "error_type": type(exc).__name__,
                },
            )

            # Record this attempt as failed.
            db.execute(
                text(
                    """
                    UPDATE notification.notification_attempts
                    SET
                        status = 'FAILED',
                        error_message = :error_message
                    WHERE id = :attempt_id
                    """
                ),
                {
                    "attempt_id": attempt_id,
                    "error_message": error_message,
                },
            )

            # ----------------------------------
            # 7A. Permanent failure
            # ----------------------------------

            if next_attempt_no >= MAX_ATTEMPTS:
                publish_to_dlq(
                    channel=ch,
                    notification_id=notification_id,
                    attempt_no=next_attempt_no,
                    error_message=error_message,
                    priority=notification["priority"],
                    request_id=request_id,
                )

                db.execute(
                    text(
                        """
                        UPDATE notification.notifications
                        SET
                            status = 'DEAD_LETTERED',
                            updated_at = NOW()
                        WHERE id = :notification_id
                        """
                    ),
                    {"notification_id": notification_id},
                )

                db.commit()

                DEAD_LETTERED.labels(
                    channel=notification["channel"],
                    priority=notification["priority"],
                ).inc()
                processing_result = "dead_lettered"

                ch.basic_ack(delivery_tag=method.delivery_tag)

                logger.error(
                    "notification_dead_lettered",
                    extra={
                        "service": "notification-worker",
                        "request_id": request_id,
                        "notification_id": notification_id,
                        "attempt_no": next_attempt_no,
                        "channel": notification["channel"],
                        "priority": notification["priority"],
                        "error_type": type(exc).__name__,
                    },
                )
                return

            # ----------------------------------
            # 7B. Temporary failure — retry
            # ----------------------------------

            db.execute(
                text(
                    """
                    UPDATE notification.notifications
                    SET
                        status = 'PENDING',
                        updated_at = NOW()
                    WHERE id = :notification_id
                    """
                ),
                {"notification_id": notification_id},
            )

            db.commit()

            delay_seconds = 2 ** next_attempt_no

            publish_retry(
                channel=ch,
                notification_id=notification_id,
                next_attempt_no=next_attempt_no + 1,
                delay_ms=delay_seconds * 1000,
                priority=notification["priority"],
                request_id=request_id,
            )

            RETRIES_SCHEDULED.labels(
                channel=notification["channel"],
                priority=notification["priority"],
            ).inc()
            processing_result = "retry_scheduled"

            ch.basic_ack(delivery_tag=method.delivery_tag)

            logger.warning(
                "notification_retry_scheduled",
                extra={
                    "service": "notification-worker",
                    "request_id": request_id,
                    "notification_id": notification_id,
                    "attempt_no": next_attempt_no,
                    "next_attempt_no": next_attempt_no + 1,
                    "delay_seconds": delay_seconds,
                    "channel": notification["channel"],
                    "error_type": type(exc).__name__,
                },
            )
            return

        # ----------------------------------
        # 8. Successful delivery
        # ----------------------------------

        db.execute(
            text(
                """
                UPDATE notification.notification_attempts
                SET
                    status = 'SUCCESS',
                    error_message = NULL
                WHERE id = :attempt_id
                """
            ),
            {"attempt_id": attempt_id},
        )

        db.execute(
            text(
                """
                UPDATE notification.notifications
                SET
                    status = 'SENT',
                    updated_at = NOW()
                WHERE id = :notification_id
                """
            ),
            {"notification_id": notification_id},
        )

        db.commit()

        created_at = notification["created_at"]
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        duration_seconds = (
            datetime.now(timezone.utc) - created_at
        ).total_seconds()

        END_TO_END_DELIVERY_DURATION.labels(
            channel=notification["channel"],
            priority=notification["priority"],
        ).observe(duration_seconds)

        DELIVERY_SUCCESS.labels(
            channel=notification["channel"],
            provider=provider_name,
            priority=notification["priority"],
        ).inc()
        processing_result = "success"

        logger.info(
            "notification_status_updated",
            extra={
                "service": "notification-worker",
                "request_id": request_id,
                "notification_id": notification_id,
                "attempt_no": next_attempt_no,
                "status": "SENT",
            },
        )

        # Only acknowledge after DB updates are committed.
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except SQLAlchemyError as exc:
        db.rollback()
        processing_result = "database_error"

        logger.exception(
            "database_operation_failed",
            extra={
                "service": "notification-worker",
                "request_id": request_id,
                "notification_id": notification_id,
                "operation": "process_notification",
            },
        )

        # A database outage may be temporary.
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True,
        )

    except Exception as exc:
        db.rollback()

        logger.exception(
            "notification_processing_failed",
            extra={
                "service": "notification-worker",
                "request_id": request_id,
                "notification_id": notification_id,
                "error_type": type(exc).__name__,
            },
        )

        # For now requeue unexpected errors.
        # Later the DLQ will prevent infinite poison-message loops.
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=True,
        )

    finally:
        if job_metric_started:
            WORKER_JOBS_IN_PROGRESS.dec()
            processing_duration = time.perf_counter() - processing_start
            WORKER_PROCESSING_DURATION.labels(
                channel=processing_channel,
                result=processing_result,
            ).observe(processing_duration)
        db.close()


def main() -> None:
    if not RABBITMQ_URL:
        raise RuntimeError("RABBITMQ_URL is missing")

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing")

    worker_metrics_port = int(os.getenv("WORKER_METRICS_PORT", "8001"))
    start_http_server(worker_metrics_port)
    WORKER_HEALTH.set(0)
    WORKER_RABBITMQ_CONNECTED.set(0)

    logger.info(
        "worker_metrics_server_started",
        extra={
            "service": "notification-worker",
            "port": worker_metrics_port,
        },
    )

    signal.signal(
        signal.SIGINT,
        request_shutdown,
    )
    signal.signal(
        signal.SIGTERM,
        request_shutdown,
    )

    connection = None

    try:
        params = pika.URLParameters(RABBITMQ_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        worker.connection = connection
        worker.channel = channel
        worker.shutting_down = False
        shutdown_requested.clear()

        WORKER_RABBITMQ_CONNECTED.set(1)

        declare_topology(channel)

        # Give this worker only one unacknowledged job at a time.
        channel.basic_qos(prefetch_count=1)
        # It means RabbitMQ should give this worker only one unacknowledged message at a time.
        # Without this, RabbitMQ may dump many messages onto one worker.


        # With this:

        # Worker gets 1 message
        # ↓
        # Processes it
        # ↓
        # Acks it
        # ↓
        # Gets next message

        channel.basic_consume(
            queue=MAIN_QUEUE,
            on_message_callback=process_message,
        )

        WORKER_HEALTH.set(1)

        logger.info(
            "worker_started",
            extra={"service": "notification-worker", "queue": MAIN_QUEUE},
        )

        channel.start_consuming()

    except KeyboardInterrupt:
        request_shutdown(signal.SIGINT, None)

    except Exception:
        WORKER_RABBITMQ_CONNECTED.set(0)
        logger.exception(
            "worker_rabbitmq_connection_failed",
            extra={
                "service": "notification-worker",
            },
        )
        raise

    finally:
        WORKER_HEALTH.set(0)
        WORKER_RABBITMQ_CONNECTED.set(0)

        if connection is not None and connection.is_open:
            connection.close()

        worker.connection = None
        worker.channel = None

        logger.info(
            "worker_stopped",
            extra={
                "service": "notification-worker",
            },
        )


if __name__ == "__main__":
    main()
