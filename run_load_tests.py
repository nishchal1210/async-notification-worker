import subprocess
import time

from measure_queue_drain import wait_for_queue_to_drain


def main() -> None:
    command = [
        "locust",
        "-f",
        "load_tests/locustfile.py",
        "--host",
        "http://127.0.0.1:8000",
        "--headless",
        "--users",
        "100",
        "--spawn-rate",
        "10",
        "--run-time",
        "5m",
        "--csv",
        "load_tests/results/rate_limit_test",
    ]

    print("Starting Locust test...")

    result = subprocess.run(command, check=False)

    test_finished_at = time.time()

    print(
        f"Locust finished with exit code {result.returncode}. "
        "Starting queue drain measurement..."
    )

    wait_for_queue_to_drain()

    print(f"Locust finish timestamp: {test_finished_at}")


if __name__ == "__main__":
    main()