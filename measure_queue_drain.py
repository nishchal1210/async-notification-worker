import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv


load_dotenv()

RABBITMQ_MANAGEMENT_URL = os.getenv(
    "RABBITMQ_MANAGEMENT_URL",
    "http://localhost:15672",
)
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
RABBITMQ_QUEUE = os.getenv(
    "RABBITMQ_QUEUE",
    "notification.delivery",
)
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")


def encoded_vhost(vhost: str) -> str:
    return "%2F" if vhost == "/" else requests.utils.quote(vhost, safe="")


def get_queue_counts() -> tuple[int, int]:
    url = (
        f"{RABBITMQ_MANAGEMENT_URL}/api/queues/"
        f"{encoded_vhost(RABBITMQ_VHOST)}/"
        f"{requests.utils.quote(RABBITMQ_QUEUE, safe='')}"
    )

    response = requests.get(
        url,
        auth=(RABBITMQ_USERNAME, RABBITMQ_PASSWORD),
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()

    ready = int(data.get("messages_ready", 0))
    unacked = int(data.get("messages_unacknowledged", 0))

    return ready, unacked


def wait_for_queue_to_drain() -> None:
    start_time = time.perf_counter()
    started_at = datetime.now(timezone.utc)

    print(f"Started monitoring at {started_at.isoformat()}")

    while True:
        elapsed = time.perf_counter() - start_time

        try:
            ready, unacked = get_queue_counts()
        except requests.RequestException as exc:
            print(
                f"elapsed={elapsed:.1f}s "
                "RabbitMQ management API unavailable; "
                f"retrying in 2 seconds ({exc})"
            )
            time.sleep(2)
            continue

        print(
            f"elapsed={elapsed:.1f}s "
            f"ready={ready} "
            f"unacked={unacked}"
        )

        if ready == 0 and unacked == 0:
            break

        time.sleep(2)

    finished_at = datetime.now(timezone.utc)
    drain_seconds = time.perf_counter() - start_time

    print(f"Queue drained at {finished_at.isoformat()}")
    print(f"Queue drain time: {drain_seconds:.2f} seconds")


if __name__ == "__main__":
    wait_for_queue_to_drain()
