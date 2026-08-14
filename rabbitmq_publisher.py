from __future__ import annotations

import json
import logging
import os
import queue
import threading
from dataclasses import dataclass, field

import pika
from dotenv import load_dotenv
from pika.exceptions import AMQPError, NackError, UnroutableError

from metrics import RABBITMQ_PUBLISH_FAILURES


load_dotenv()

logger = logging.getLogger("rabbitmq-publisher")

RABBITMQ_URL = os.environ["RABBITMQ_URL"]

MAIN_EXCHANGE = "notification.exchange"
MAIN_QUEUE = "notification.delivery"
MAIN_ROUTING_KEY = "notification.delivery"

PRIORITY_MAP = {
    "LOW": 1,
    "NORMAL": 5,
    "HIGH": 10,
}


def declare_topology(channel) -> None:
    channel.exchange_declare(
        exchange=MAIN_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    # This creates the queue if it does not already exist.
    channel.queue_declare(
        queue=MAIN_QUEUE,
        durable=True,
        arguments={
            "x-max-priority": 10,
            # tells RabbitMQ:

            # This queue supports priorities from 0 through 10.
        },
    )

    channel.queue_bind(
        queue=MAIN_QUEUE,
        exchange=MAIN_EXCHANGE,
        routing_key=MAIN_ROUTING_KEY, 
        # Because we are using the default exchange, this sends the message to: notification.delivery
    )


class PublisherStoppedError(RuntimeError):
    pass


@dataclass
class PublishRequest:
    notification_id: str
    priority: str
    request_id: str | None

    completed: threading.Event = field(
        default_factory=threading.Event
    )

    error: Exception | None = None
    published: bool = False


class RabbitMQPublisher:
    def __init__(
        self,
        *,
        max_pending_requests: int = 1000,
    ) -> None:
        self._requests: queue.Queue[PublishRequest | None] = (
            queue.Queue(maxsize=max_pending_requests)
        )

        self._connection: pika.BlockingConnection | None = None
        self._channel = None

        self._started = False
        self._stopping = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        return (
            self._started
            and not self._stopping.is_set()
            and self._thread is not None
            and self._thread.is_alive()
        )

    @property
    def pending_count(self) -> int:
        return self._requests.qsize()

    @property
    def is_connected(self) -> bool:
        return (
            self._connection is not None
            and self._connection.is_open
            and self._channel is not None
            and self._channel.is_open
        )

    def start(self) -> None:
        if self._started:
            return

        self._started = True
        self._stopping.clear()

        self._thread = threading.Thread(
            target=self._run,
            name="rabbitmq-publisher",
            daemon=True,
        )
        self._thread.start()

        logger.info("rabbitmq_publisher_started")

    def _connect(self) -> None:
        parameters = pika.URLParameters(RABBITMQ_URL)

        # Detect broken or blocked connections rather than waiting forever.
        parameters.heartbeat = 30
        parameters.blocked_connection_timeout = 30
        parameters.socket_timeout = 10
        parameters.stack_timeout = 15
        parameters.connection_attempts = 3
        parameters.retry_delay = 2

        connection = pika.BlockingConnection(parameters)
        # BlockingConnection means this code waits until RabbitMQ accepts the connection.


        channel = connection.channel()

        declare_topology(channel)

        # basic_publish waits for ACK/NACK from RabbitMQ.
        channel.confirm_delivery()

        self._connection = connection
        self._channel = channel

        logger.info("rabbitmq_publisher_connected")

    def _close_connection(self) -> None:
        connection = self._connection

        self._connection = None
        self._channel = None

        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:
                logger.exception(
                    "rabbitmq_connection_close_failed"
                )

    def _ensure_connection(self) -> None:
        if (
            self._connection is None
            or self._connection.is_closed
            or self._channel is None
            or self._channel.is_closed
        ):
            self._close_connection()
            self._connect()

    def _publish_once(
        self,
        request: PublishRequest,
    ) -> None:
        self._ensure_connection()

        body = json.dumps(
            {
                "notification_id": request.notification_id,
                "attempt_no": 1,
                "priority": request.priority,
                "request_id": request.request_id,
            }
        )

        published = self._channel.basic_publish(
            exchange=MAIN_EXCHANGE,
            routing_key=MAIN_ROUTING_KEY,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                priority=PRIORITY_MAP.get(
                    request.priority,
                    5,
                ),
                message_id=request.notification_id,
                correlation_id=request.request_id,
            ),
            mandatory=True,
        )

        if published is False:
            raise NackError(
                "RabbitMQ negatively acknowledged publication"
            )

    def _publish_with_one_retry(
        self,
        request: PublishRequest,
    ) -> None:
        try:
            self._publish_once(request)
            return

        except (
            AMQPError,
            OSError,
            TimeoutError,
        ):
            logger.warning(
                "rabbitmq_publish_reconnecting",
                extra={
                    "notification_id": request.notification_id,
                    "request_id": request.request_id,
                },
                exc_info=True,
            )

            self._close_connection()

        # Retry the same request once on a fresh connection.
        self._publish_once(request)

    def _run(self) -> None:
        while True:
            request = self._requests.get()

            if request is None:
                self._requests.task_done()
                break

            try:
                self._publish_with_one_retry(request)
                request.published = True

                logger.info(
                    "rabbitmq_publish_succeeded",
                    extra={
                        "notification_id":
                            request.notification_id,
                        "request_id": request.request_id,
                    },
                )

            except (
                AMQPError,
                OSError,
                TimeoutError,
                UnroutableError,
                NackError,
            ) as exc:
                request.error = exc
                RABBITMQ_PUBLISH_FAILURES.inc()

                logger.exception(
                    "rabbitmq_publish_failed",
                    extra={
                        "notification_id":
                            request.notification_id,
                        "request_id": request.request_id,
                        "error_type": type(exc).__name__,
                    },
                )

                self._close_connection()

            except Exception as exc:
                request.error = exc
                RABBITMQ_PUBLISH_FAILURES.inc()

                logger.exception(
                    "rabbitmq_publish_unexpected_failure",
                    extra={
                        "notification_id":
                            request.notification_id,
                        "request_id": request.request_id,
                        "error_type": type(exc).__name__,
                    },
                )

                self._close_connection()

            finally:
                request.completed.set()
                self._requests.task_done()

        self._close_connection()
        logger.info("rabbitmq_publisher_stopped")

    def publish(
        self,
        *,
        notification_id: str,
        priority: str,
        request_id: str | None,
        queue_timeout_seconds: float = 2,
    ) -> None:
        if not self._started or self._stopping.is_set():
            raise PublisherStoppedError(
                "RabbitMQ publisher is not running"
            )

        request = PublishRequest(
            notification_id=notification_id,
            priority=priority,
            request_id=request_id,
        )

        try:
            self._requests.put(
                request,
                block=True,
                timeout=queue_timeout_seconds,
            )
        except queue.Full as exc:
            RABBITMQ_PUBLISH_FAILURES.inc()

            raise RuntimeError(
                "RabbitMQ publisher queue is full"
            ) from exc

        # Once accepted into our internal queue, wait for the real
        # RabbitMQ publication result. Do not mark it failed while
        # it can still be published later.
        request.completed.wait()

        if request.error is not None:
            raise request.error

        if not request.published:
            raise RuntimeError(
                "RabbitMQ publication did not complete"
            )

    def stop(self) -> None:
        if not self._started:
            return

        self._stopping.set()

        self._requests.put(None)

        if self._thread is not None:
            self._thread.join(timeout=30)

        self._close_connection()
        self._started = False
