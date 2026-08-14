from collections.abc import Callable

import os
import logging

from providers.base import NotificationProvider
from providers.email_provider import EmailProvider
from providers.load_test_provider import LoadTestProvider
from providers.push_provider import PushProvider
from providers.sms_provider import SMSProvider

logger = logging.getLogger("provider-factory")

PROVIDERS: dict[str, Callable[[], NotificationProvider]] = {
    "email": EmailProvider,
    "sms": SMSProvider,
    "push": PushProvider,
}
def is_load_test_mode() -> bool:
    return (
        os.getenv("LOAD_TEST_MODE", "false")
        .strip()
        .lower()
        == "true"
    )


def get_provider(channel: str):
    if channel not in {"email", "sms", "push"}:
        raise ValueError(
            f"Unsupported notification channel: {channel}"
        )

    if is_load_test_mode():
        logger.info("load_test_provider_selected")
        return LoadTestProvider()

    if channel == "email":
        return EmailProvider()

    if channel == "sms":
        return SMSProvider()

    if channel == "push":
        return PushProvider()

# Now:

# LOAD_TEST_MODE=true

# means every channel uses the simulated provider.
