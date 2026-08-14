from pathlib import Path

from prometheus_client import generate_latest

import metrics


def test_all_milestone_metrics_are_exposed():
    output = generate_latest().decode("utf-8")
    metric_names = (
        "notification_requests_total",
        "notification_requests_rejected_total",
        "notifications_created_total",
        "rabbitmq_publish_failures_total",
        "notification_api_request_duration_seconds",
        "notification_delivery_attempts_total",
        "notification_delivery_success_total",
        "notification_delivery_failures_total",
        "notification_retry_scheduled_total",
        "notification_dead_lettered_total",
        "notification_provider_duration_seconds",
        "notification_worker_processing_duration_seconds",
        "notification_worker_jobs_in_progress",
    )

    for metric_name in metric_names:
        assert metric_name in output


def test_prometheus_config_scrapes_api_and_workers():
    config = Path("prometheus.yml").read_text(encoding="utf-8")

    assert "api:8000" in config
    assert "worker-1:8001" in config
    assert "worker-2:8001" in config
    assert "worker-3:8001" in config
