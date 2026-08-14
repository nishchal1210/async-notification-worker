import json

import rabbitmq_publisher
import worker


class RecordingChannel:
    def __init__(self):
        self.queue_declarations = []
        self.published = []

    def exchange_declare(self, **kwargs):
        pass

    def queue_declare(self, **kwargs):
        self.queue_declarations.append(kwargs)

    def queue_bind(self, **kwargs):
        pass

    def basic_publish(self, **kwargs):
        self.published.append(kwargs)


def test_publisher_declares_new_priority_delivery_queue():
    channel = RecordingChannel()

    rabbitmq_publisher.declare_topology(channel)

    assert channel.queue_declarations == [
        {
            "queue": "notification.delivery",
            "durable": True,
            "arguments": {"x-max-priority": 10},
        }
    ]


def test_worker_retry_preserves_high_priority():
    channel = RecordingChannel()

    worker.publish_retry(
        channel=channel,
        notification_id="notification-id",
        next_attempt_no=2,
        delay_ms=2000,
        priority="HIGH",
        request_id="request-id",
    )

    published = channel.published[0]
    assert json.loads(published["body"])["priority"] == "HIGH"
    assert json.loads(published["body"])["request_id"] == "request-id"
    assert published["properties"].priority == 10
    assert published["properties"].expiration == "2000"


def test_worker_dlq_preserves_priority_metadata():
    channel = RecordingChannel()

    worker.publish_to_dlq(
        channel=channel,
        notification_id="notification-id",
        attempt_no=4,
        error_message="delivery failed",
        priority="LOW",
        request_id="request-id",
    )

    message = json.loads(channel.published[0]["body"])
    assert message["priority"] == "LOW"
    assert message["request_id"] == "request-id"
