import os
import random
import time

from providers.base import NotificationProvider


class LoadTestProvider(NotificationProvider):
    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
    ) -> None:
        minimum_ms = int(
            os.getenv("LOAD_TEST_PROVIDER_MIN_MS", "20")
        )
        maximum_ms = int(
            os.getenv("LOAD_TEST_PROVIDER_MAX_MS", "80")
        )
        failure_rate = float(
            os.getenv("LOAD_TEST_PROVIDER_FAILURE_RATE", "0")
        )

        # Simulate external provider/network latency.
        time.sleep(
            random.uniform(
                minimum_ms / 1000,
                maximum_ms / 1000,
            )
        )

        # Optionally simulate provider failures.
        if random.random() < failure_rate:
            raise RuntimeError(
                "Simulated provider failure"
            )