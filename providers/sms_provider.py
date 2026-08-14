import logging

from log_utils import mask_recipient
from providers.base import NotificationProvider


logger = logging.getLogger("sms-provider")


class SMSProvider(NotificationProvider):
    """Development SMS provider; replace its internals with a real gateway later."""

    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
    ) -> None:
        logger.info(
            "fake_sms_sent",
            extra={
                "service": "notification-worker",
                "channel": "sms",
                "recipient_masked": mask_recipient(recipient, "sms"),
            },
        )
