from typing import Any, Literal

from pydantic import BaseModel, EmailStr, Field, TypeAdapter, model_validator


class NotificationRequest(BaseModel):
    user_id: int
    channel: Literal["email", "sms", "push"]
    recipient: str = Field(min_length=1)
    subject: str | None = Field(default=None, max_length=200)
    body: str | None = Field(default=None, min_length=1)
    template_name: str | None = None
    template_data: dict[str, Any] | None = None
    priority: Literal[
        "HIGH",
        "NORMAL",
        "LOW",
    ] = "NORMAL"

    @model_validator(mode="after")
    def validate_channel_fields(self) -> "NotificationRequest":
        if self.channel == "email":
            TypeAdapter(EmailStr).validate_python(self.recipient)
        elif self.channel == "sms" and not self.recipient.startswith("+"):
            raise ValueError("SMS recipient must be in international format")
        elif self.channel == "push" and not self.recipient.strip():
            raise ValueError("Push notification requires a device token")

        using_template = self.template_name is not None
        using_direct_content = self.body is not None

        if using_template and using_direct_content:
            raise ValueError("Provide either template_name or direct body, not both")
        if not using_template and not using_direct_content:
            raise ValueError("Either template_name or body is required")
        if using_template and self.template_data is None:
            self.template_data = {}
        if not using_template and self.channel == "email" and not self.subject:
            raise ValueError(
                "subject is required for direct email notifications"
            )

        return self


class TemplateCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    channel: Literal["email", "sms", "push"]
    subject_template: str | None = Field(default=None, max_length=200)
    body_template: str = Field(min_length=1)
