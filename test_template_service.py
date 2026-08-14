import pytest

from template_service import render_notification_template


class MappingResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one_or_none(self):
        return self.row


class TemplateSession:
    def __init__(self, row):
        self.row = row
        self.parameters = None

    def execute(self, statement, parameters):
        self.parameters = parameters
        return MappingResult(self.row)


def test_renders_latest_enabled_template_content():
    db = TemplateSession(
        {
            "id": 12,
            "subject_template": "Your OTP",
            "body_template": "Hello {{ name }}, your OTP is {{ otp }}.",
            "version": 2,
        }
    )

    rendered = render_notification_template(
        db=db,
        template_name="otp",
        channel="email",
        template_data={"name": "Keshav", "otp": "123456"},
    )

    assert rendered.subject == "Your OTP"
    assert rendered.body == "Hello Keshav, your OTP is 123456."
    assert rendered.template_id == 12
    assert rendered.template_version == 2
    assert db.parameters == {"template_name": "otp", "channel": "email"}


def test_missing_template_variable_fails_strictly():
    db = TemplateSession(
        {
            "id": 1,
            "subject_template": "Your OTP",
            "body_template": "Hello {{ name }}, OTP {{ otp }}",
            "version": 1,
        }
    )

    with pytest.raises(ValueError, match="Template rendering failed: 'otp' is undefined"):
        render_notification_template(db, "otp", "email", {"name": "Keshav"})


def test_missing_channel_specific_template_is_rejected():
    db = TemplateSession(None)

    with pytest.raises(
        ValueError,
        match="No enabled template 'otp' exists for channel 'push'",
    ):
        render_notification_template(db, "otp", "push", {"otp": "123456"})
