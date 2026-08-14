from prometheus_client import Counter, Gauge, Histogram


# API metrics
NOTIFICATION_REQUESTS = Counter(
    "notification_requests_total",
    "Total notification API requests received",
    ["channel", "priority"],
)

NOTIFICATION_REQUESTS_REJECTED = Counter(
    "notification_requests_rejected_total",
    "Total notification requests rejected",
    ["reason"],
)

NOTIFICATIONS_CREATED = Counter(
    "notifications_created_total",
    "Total notifications successfully created",
    ["channel", "priority"],
)

RABBITMQ_PUBLISH_FAILURES = Counter(
    "rabbitmq_publish_failures_total",
    "Total failures while publishing jobs to RabbitMQ",
)

API_REQUEST_DURATION = Histogram(
    "notification_api_request_duration_seconds",
    "Time spent handling notification API requests",
    ["method", "path", "status_code"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)

DEPENDENCY_HEALTH = Gauge(
    "notification_dependency_health",
    "Health status of application dependencies",
    ["service", "dependency"],
)


# Worker metrics
DELIVERY_ATTEMPTS = Counter(
    "notification_delivery_attempts_total",
    "Total notification delivery attempts",
    ["channel", "provider", "priority"],
)

DELIVERY_SUCCESS = Counter(
    "notification_delivery_success_total",
    "Total successful notification deliveries",
    ["channel", "provider", "priority"],
)

DELIVERY_FAILURES = Counter(
    "notification_delivery_failures_total",
    "Total failed notification delivery attempts",
    ["channel", "provider", "priority", "error_type"],
)

RETRIES_SCHEDULED = Counter(
    "notification_retry_scheduled_total",
    "Total notification retries scheduled",
    ["channel", "priority"],
)

DEAD_LETTERED = Counter(
    "notification_dead_lettered_total",
    "Total notifications moved to the dead-letter queue",
    ["channel", "priority"],
)

PROVIDER_DURATION = Histogram(
    "notification_provider_duration_seconds",
    "Duration of provider send operations",
    ["channel", "provider"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20),
)

WORKER_PROCESSING_DURATION = Histogram(
    "notification_worker_processing_duration_seconds",
    "Total time spent processing one RabbitMQ job",
    ["channel", "result"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)

WORKER_JOBS_IN_PROGRESS = Gauge(
    "notification_worker_jobs_in_progress",
    "Number of notification jobs currently being processed",
)

WORKER_HEALTH = Gauge(
    "notification_worker_health",
    "Whether the notification worker is healthy",
)

WORKER_RABBITMQ_CONNECTED = Gauge(
    "notification_worker_rabbitmq_connected",
    "Whether the worker is connected to RabbitMQ",
)

END_TO_END_DELIVERY_DURATION = Histogram(
    "notification_end_to_end_delivery_duration_seconds",
    "Time from notification creation to successful delivery",
    ["channel", "priority"],
    buckets=(
        0.1,
        0.25,
        0.5,
        1,
        2,
        5,
        10,
        30,
        60,
        120,
        300,
        600,
    ),
)
