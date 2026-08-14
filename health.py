from dataclasses import dataclass

from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from redis_client import redis_client


@dataclass
class DependencyHealth:
    healthy: bool
    detail: str


def check_postgres(db: Session) -> DependencyHealth:
    try:
        db.execute(text("SELECT 1"))
        return DependencyHealth(
            healthy=True,
            detail="connected",
        )

    except SQLAlchemyError as exc:
        return DependencyHealth(
            healthy=False,
            detail=type(exc).__name__,
        )


def check_redis() -> DependencyHealth:
    try:
        redis_client.ping()

        return DependencyHealth(
            healthy=True,
            detail="connected",
        )

    except RedisError as exc:
        return DependencyHealth(
            healthy=False,
            detail=type(exc).__name__,
        )
