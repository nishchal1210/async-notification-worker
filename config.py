import os


REQUIRED_ENVIRONMENT_VARIABLES = (
    "DATABASE_URL",
    "REDIS_URL",
    "RABBITMQ_URL",
)


def validate_environment() -> None:
    missing = [
        variable
        for variable in REQUIRED_ENVIRONMENT_VARIABLES
        if not os.getenv(variable)
    ]

    if missing:
        missing_names = ", ".join(missing)

        raise RuntimeError(
            f"Missing required environment variables: "
            f"{missing_names}"
        )
