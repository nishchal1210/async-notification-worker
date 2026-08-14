import os
import smtplib
from email.message import EmailMessage

from providers.base import NotificationProvider


class EmailProvider(NotificationProvider):
    """Deliver notifications through the configured SMTP server."""

    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
    ) -> None:
        smtp_host = os.getenv("SMTP_HOST")
        smtp_port = os.getenv("SMTP_PORT")
        smtp_username = os.getenv("SMTP_USERNAME")
        smtp_password = os.getenv("SMTP_PASSWORD")
        from_email = os.getenv("FROM_EMAIL")

        if not all(
            [smtp_host, smtp_port, smtp_username, smtp_password, from_email]
        ):
            raise RuntimeError("SMTP configuration is incomplete")

        message = EmailMessage()
        message["From"] = from_email
        message["To"] = recipient
        message["Subject"] = subject or ""
        message.set_content(body)

        with smtplib.SMTP(smtp_host, int(smtp_port), timeout=15) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(message)
