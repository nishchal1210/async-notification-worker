import json
import logging

from log_utils import mask_device_token, mask_email, mask_phone, mask_recipient
from logging_config import JsonFormatter


def test_json_formatter_includes_structured_fields():
    record = logging.LogRecord(
        name="notification-worker",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="notification_job_received",
        args=(),
        exc_info=None,
    )
    record.service = "notification-worker"
    record.request_id = "request-id"
    record.notification_id = "notification-id"

    output = json.loads(JsonFormatter().format(record))

    assert output["level"] == "INFO"
    assert output["message"] == "notification_job_received"
    assert output["service"] == "notification-worker"
    assert output["request_id"] == "request-id"
    assert output["notification_id"] == "notification-id"


def test_recipient_masking():
    assert mask_email("keshav@gmail.com") == "ke***@gmail.com"
    assert mask_phone("+919876543210") == "***3210"
    assert mask_device_token("device-token-1234") == "devi...1234"
    assert mask_recipient("unknown", "other") == "***"
