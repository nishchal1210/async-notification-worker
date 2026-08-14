import pytest
from pydantic import ValidationError

from notification_schemas import NotificationRequest
from providers.email_provider import EmailProvider
from providers.factory import get_provider
from providers.push_provider import PushProvider
from providers.sms_provider import SMSProvider
from providers.load_test_provider import LoadTestProvider


def test_notification_priority_defaults_to_normal():
    request = NotificationRequest(
        user_id=1,
        channel="sms",
        recipient="+919876543210",
        body="Hello",
    )

    assert request.priority == "NORMAL"


@pytest.mark.parametrize("priority", ["HIGH", "NORMAL", "LOW"])
def test_notification_accepts_supported_priorities(priority):
    request = NotificationRequest(
        user_id=1,
        channel="sms",
        recipient="+919876543210",
        body="Hello",
        priority=priority,
    )

    assert request.priority == priority


def test_notification_rejects_unknown_priority():
    with pytest.raises(ValidationError):
        NotificationRequest(
            user_id=1,
            channel="sms",
            recipient="+919876543210",
            body="Hello",
            priority="URGENT",
        )


@pytest.mark.parametrize(
    ("channel", "provider_type"),
    [
        ("email", EmailProvider),
        ("sms", SMSProvider),
        ("push", PushProvider),
    ],
)
def test_factory_returns_registered_provider(
    channel,
    provider_type,
    monkeypatch,
):
    monkeypatch.setenv(
        "LOAD_TEST_MODE",
        "false",
    )

    assert isinstance(
        get_provider(channel),
        provider_type,
    )


def test_factory_rejects_unknown_channel(
    monkeypatch,
):
    monkeypatch.setenv(
        "LOAD_TEST_MODE",
        "false",
    )

    with pytest.raises(
        ValueError,
        match="Unsupported notification channel",
    ):
        get_provider("carrier-pigeon")

@pytest.mark.parametrize(
    "channel",
    ["email", "sms", "push"],
)
def test_factory_uses_load_test_provider(
    channel,
    monkeypatch,
):
    monkeypatch.setenv(
        "LOAD_TEST_MODE",
        "true",
    )

    assert isinstance(
        get_provider(channel),
        LoadTestProvider,
    )

@pytest.mark.parametrize(
    "payload",
    [
        {
            "user_id": 1,
            "channel": "email",
            "recipient": "test@example.com",
            "subject": "Welcome",
            "body": "Hello",
        },
        {
            "user_id": 1,
            "channel": "sms",
            "recipient": "+919876543210",
            "subject": None,
            "body": "Your OTP is 123456",
        },
        {
            "user_id": 1,
            "channel": "push",
            "recipient": "device-token-123",
            "subject": "Order update",
            "body": "Out for delivery",
        },
    ],
)
def test_valid_channel_payloads(payload):
    assert NotificationRequest(**payload).channel == payload["channel"]


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"recipient": "not-an-email"}, "value is not a valid email address"),
        (
            {"subject": None},
            "subject is required for direct email notifications",
        ),
        (
            {"channel": "sms", "recipient": "919876543210", "subject": None},
            "SMS recipient must be in international format",
        ),
        (
            {"channel": "push", "recipient": "   "},
            "Push notification requires a device token",
        ),
    ],
)
def test_channel_specific_validation(changes, message):
    payload = {
        "user_id": 1,
        "channel": "email",
        "recipient": "test@example.com",
        "subject": "Welcome",
        "body": "Hello",
    }
    payload.update(changes)

    with pytest.raises(ValidationError, match=message):
        NotificationRequest(**payload)


def test_template_request_defaults_template_data():
    request = NotificationRequest(
        user_id=1,
        channel="email",
        recipient="test@example.com",
        template_name="otp",
        priority="HIGH",
    )

    assert request.body is None
    assert request.template_data == {}


def test_request_rejects_template_and_direct_body_together():
    with pytest.raises(ValidationError, match="either template_name or direct body"):
        NotificationRequest(
            user_id=1,
            channel="sms",
            recipient="+919876543210",
            body="Direct body",
            template_name="otp",
        )


def test_request_requires_content_source():
    with pytest.raises(ValidationError, match="Either template_name or body"):
        NotificationRequest(
            user_id=1,
            channel="push",
            recipient="device-token",
        )


def test_fake_sms_provider_masks_recipient(caplog):
    caplog.set_level("INFO")
    SMSProvider().send("+919876543210", None, "OTP 123456")

    record = next(record for record in caplog.records if record.msg == "fake_sms_sent")
    assert record.recipient_masked == "***3210"
    assert "123456" not in record.getMessage()


def test_push_provider_sends_through_firebase(monkeypatch):
    from providers import push_provider

    sent = {}
    firebase_app = object()

    monkeypatch.setattr(push_provider, "_get_firebase_app", lambda: firebase_app)
    monkeypatch.setattr(
        push_provider.messaging,
        "Notification",
        lambda **values: values,
    )
    monkeypatch.setattr(
        push_provider.messaging,
        "Message",
        lambda **values: values,
    )
    monkeypatch.setattr(
        push_provider.messaging,
        "send",
        lambda message, app: sent.update(message=message, app=app),
    )

    PushProvider().send("device-token", "Update", "Delivered")

    assert sent == {
        "message": {
            "notification": {"title": "Update", "body": "Delivered"},
            "token": "device-token",
        },
        "app": firebase_app,
    }


def test_firebase_credentials_are_built_from_environment(monkeypatch):
    from providers.push_provider import _service_account_info

    values = {
        "FIREBASE_PROJECT_ID": "project-id",
        "FIREBASE_PRIVATE_KEY_ID": "key-id",
        "FIREBASE_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
        "FIREBASE_CLIENT_EMAIL": "firebase@example.test",
        "FIREBASE_CLIENT_ID": "client-id",
        "FIREBASE_CLIENT_X509_CERT_URL": "https://example.test/cert",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    info = _service_account_info()

    assert info["project_id"] == "project-id"
    assert "\\n" not in info["private_key"]
    assert "\n" in info["private_key"]
