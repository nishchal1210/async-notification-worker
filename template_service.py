from dataclasses import dataclass
from typing import Any

from jinja2 import Environment, StrictUndefined, TemplateError
from sqlalchemy import text
from sqlalchemy.orm import Session


jinja_environment = Environment(
    undefined=StrictUndefined,
    autoescape=False,
)


@dataclass
class RenderedNotification:
    subject: str | None
    body: str
    template_id: int
    template_version: int


def render_notification_template(
    db: Session,
    template_name: str,
    channel: str,
    template_data: dict[str, Any],
) -> RenderedNotification:
    template_row = db.execute(
        text(
            """
            SELECT
                id,
                subject_template,
                body_template,
                version
            FROM notification.notification_templates
            WHERE name = :template_name
              AND channel = :channel
              AND enabled = TRUE
            ORDER BY version DESC
            LIMIT 1
            """
        ),
        {
            "template_name": template_name,
            "channel": channel,
        },
    ).mappings().one_or_none()

    if template_row is None:
        raise ValueError(
            f"No enabled template '{template_name}' "
            f"exists for channel '{channel}'"
        )

    try:
        subject = None
        if template_row["subject_template"] is not None:
            subject = jinja_environment.from_string(
                template_row["subject_template"]
            ).render(**template_data)

        body = jinja_environment.from_string(
            template_row["body_template"]
        ).render(**template_data)
    except TemplateError as exc:
        raise ValueError(f"Template rendering failed: {exc}") from exc

    return RenderedNotification(
        subject=subject,
        body=body,
        template_id=template_row["id"],
        template_version=template_row["version"],
    )
