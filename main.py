import os
import time
import uuid
import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, status, Header, Request
from fastapi.responses import JSONResponse, Response as FastAPIResponse
from config import validate_environment
from health import check_postgres, check_redis
from logging_config import configure_logging
from metrics import (
    API_REQUEST_DURATION,
    DEPENDENCY_HEALTH,
    NOTIFICATION_REQUESTS,
    NOTIFICATION_REQUESTS_REJECTED,
    NOTIFICATIONS_CREATED,
)
from notification_schemas import NotificationRequest, TemplateCreateRequest
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from rabbitmq_publisher import RabbitMQPublisher
from template_service import render_notification_template

from sqlalchemy import (
    create_engine,
    Column,
    BigInteger,
    String,
    Text,
    DateTime,
    ForeignKey,
    Boolean,
    Integer,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship

from fastapi import (
    FastAPI,
    Depends,
    HTTPException,
    Response,
    status,
)

from rate_limiter import check_rate_limit
load_dotenv()

configure_logging(
    service_name="notification-api",
    log_level=os.getenv("LOG_LEVEL", "INFO"),
)
logger = logging.getLogger("notification-api")

request_id_context: ContextVar[str | None] = ContextVar(
    "request_id",
    default=None,
)

publisher = RabbitMQPublisher(
    max_pending_requests=1000,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "notification_api_starting",
        extra={
            "service": "notification-api",
        },
    )

    app.state.shutting_down = False
    validate_environment()
    publisher.start()

    logger.info(
        "notification_api_started",
        extra={
            "service": "notification-api",
        },
    )

    try:
        yield
    finally:
        app.state.shutting_down = True

        logger.info(
            "notification_api_stopping",
            extra={
                "service": "notification-api",
                "pending_publish_requests": publisher.pending_count,
            },
        )

        publisher.stop()

        logger.info(
            "notification_api_stopped",
            extra={
                "service": "notification-api",
            },
        )


app = FastAPI(
    title="Notification API",
    lifespan=lifespan,
)


@app.middleware("http")
async def reject_during_shutdown(
    request: Request,
    call_next,
):
    allowed_during_shutdown = {
        "/health/live",
        "/metrics",
    }

    if (
        getattr(
            request.app.state,
            "shutting_down",
            False,
        )
        and request.url.path not in allowed_during_shutdown
    ):
        return FastAPIResponse(
            status_code=503,
            content="Service is shutting down",
        )

    return await call_next(request)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    incoming_request_id = request.headers.get("X-Request-ID")
    request_id = incoming_request_id or str(uuid.uuid4())
    token = request_id_context.set(request_id)
    start_time = time.perf_counter()

    logger.info(
        "http_request_started",
        extra={
            "service": "notification-api",
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
        },
    )

    try:
        response = await call_next(request)
        duration_seconds = time.perf_counter() - start_time
        duration_ms = round(duration_seconds * 1000, 2)
        API_REQUEST_DURATION.labels(
            method=request.method,
            path=request.url.path,
            status_code=str(response.status_code),
        ).observe(duration_seconds)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "http_request_completed",
            extra={
                "service": "notification-api",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )
        return response
    except Exception:
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        logger.exception(
            "http_request_failed",
            extra={
                "service": "notification-api",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "duration_ms": duration_ms,
            },
        )
        raise
    finally:
        request_id_context.reset(token)


# -------------------------
# Database setup
# -------------------------

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

Base = declarative_base()



# -------------------------
# Database models
# -------------------------

class User(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "notification"}

    id = Column(BigInteger, primary_key=True, index=True)
    email = Column(String(255), unique=True, nullable=False)
    phone = Column(String(20), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    notifications = relationship("Notification", back_populates="user")


class NotificationTemplate(Base):
    __tablename__ = "notification_templates"
    __table_args__ = (
        UniqueConstraint(
            "name",
            "channel",
            "version",
            name="uq_notification_template_name_channel_version",
        ),
        {"schema": "notification"},
    )

    id = Column(BigInteger, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    channel = Column(String(20), nullable=False)
    subject_template = Column(String(200), nullable=True)
    body_template = Column(Text, nullable=False)
    version = Column(Integer, nullable=False, default=1)
    enabled = Column(Boolean, nullable=False, default=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = {"schema": "notification"}
    idempotency_key = Column(String(255), unique=True, nullable=True)
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    request_id = Column(String(100), nullable=True, index=True)
    user_id = Column(BigInteger, ForeignKey("notification.users.id"), nullable=False)
    channel = Column(String(20), nullable=False)
    recipient = Column(String(255), nullable=False)
    priority = Column(
        String(20),
        nullable=False,
        default="NORMAL",
    )
    subject = Column(String(200), nullable=True)
    body = Column(Text, nullable=False)
    template_id = Column(
        BigInteger,
        ForeignKey("notification.notification_templates.id"),
        nullable=True,
    )
    template_version = Column(Integer, nullable=True)
    status = Column(String(30), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    user = relationship("User", back_populates="notifications")
    attempts = relationship("NotificationAttempt", back_populates="notification")


class NotificationAttempt(Base):
    __tablename__ = "notification_attempts"
    __table_args__ = {"schema": "notification"}

    id = Column(BigInteger, primary_key=True, index=True)
    notification_id = Column(
        UUID(as_uuid=True),
        ForeignKey("notification.notifications.id"),
        nullable=False,
    )
    attempt_no = Column(BigInteger, nullable=False)
    status = Column(String(30), nullable=False)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    notification = relationship("Notification", back_populates="attempts")


# This creates tables if they do not exist.
# Later we will replace this with Alembic migrations.

from sqlalchemy import text

with engine.begin() as conn:
    conn.execute(
        text("CREATE SCHEMA IF NOT EXISTS notification")
    )

Base.metadata.create_all(bind=engine)



# -------------------------
# Database session dependency
# -------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get(
    "/health/live",
    include_in_schema=False,
)
def liveness():
    return {
        "status": "alive",
        "service": "notification-api",
    }


@app.get(
    "/health/ready",
    include_in_schema=False,
)
def readiness(
    db: Session = Depends(get_db),
):
    postgres_health = check_postgres(db)
    redis_health = check_redis()

    publisher_healthy = publisher.is_running

    dependencies = {
        "postgres": {
            "healthy": postgres_health.healthy,
            "detail": postgres_health.detail,
        },
        "redis": {
            "healthy": redis_health.healthy,
            "detail": redis_health.detail,
        },
        "rabbitmq_publisher": {
            "healthy": publisher_healthy,
            "detail": (
                "running"
                if publisher_healthy
                else "not_running"
            ),
        },
    }

    DEPENDENCY_HEALTH.labels(
        service="notification-api",
        dependency="postgres",
    ).set(1 if postgres_health.healthy else 0)

    DEPENDENCY_HEALTH.labels(
        service="notification-api",
        dependency="redis",
    ).set(1 if redis_health.healthy else 0)

    DEPENDENCY_HEALTH.labels(
        service="notification-api",
        dependency="rabbitmq_publisher",
    ).set(1 if publisher_healthy else 0)

    ready = all(
        dependency["healthy"]
        for dependency in dependencies.values()
    )

    response_body = {
        "status": "ready" if ready else "not_ready",
        "service": "notification-api",
        "dependencies": dependencies,
    }

    return JSONResponse(
        status_code=200 if ready else 503,
        content=response_body,
    )


# -------------------------
# Pydantic request model
# -------------------------

# -------------------------
# API endpoint
# -------------------------

@app.get("/metrics", include_in_schema=False)
def metrics_endpoint():
    return FastAPIResponse(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )

@app.post("/templates", status_code=status.HTTP_201_CREATED)
def create_template(
    payload: TemplateCreateRequest,
    db: Session = Depends(get_db),
):
    latest_version = db.execute(
        text(
            """
            SELECT COALESCE(MAX(version), 0)
            FROM notification.notification_templates
            WHERE name = :name
              AND channel = :channel
            """
        ),
        {"name": payload.name, "channel": payload.channel},
    ).scalar_one()

    new_version = latest_version + 1
    template_id = db.execute(
        text(
            """
            INSERT INTO notification.notification_templates (
                name,
                channel,
                subject_template,
                body_template,
                version,
                enabled
            )
            VALUES (
                :name,
                :channel,
                :subject_template,
                :body_template,
                :version,
                TRUE
            )
            RETURNING id
            """
        ),
        {
            "name": payload.name,
            "channel": payload.channel,
            "subject_template": payload.subject_template,
            "body_template": payload.body_template,
            "version": new_version,
        },
    ).scalar_one()
    db.commit()

    return {
        "template_id": template_id,
        "name": payload.name,
        "channel": payload.channel,
        "version": new_version,
    }

@app.post("/notifications", status_code=status.HTTP_202_ACCEPTED) # I accepted your request and queued it for processing.
def create_notification(
    payload: NotificationRequest,
    response: Response,
    db: Session = Depends(get_db),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    start = time.time()
    request_id = request_id_context.get()

    NOTIFICATION_REQUESTS.labels(
        channel=payload.channel,
        priority=payload.priority,
    ).inc()

    logger.info(
        "notification_creation_started",
        extra={
            "service": "notification-api",
            "request_id": request_id,
            "user_id": payload.user_id,
            "channel": payload.channel,
            "priority": payload.priority,
        },
    )
    
   
    # 1. Check if user exists
    user = db.query(User).filter(User.id == payload.user_id).first()

    if user is None:
        NOTIFICATION_REQUESTS_REJECTED.labels(reason="user_not_found").inc()
        raise HTTPException(
            status_code=404,
            detail=f"User with id {payload.user_id} not found",
        )

    if not idempotency_key:
        NOTIFICATION_REQUESTS_REJECTED.labels(reason="validation_error").inc()
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key header is required",
        )

    existing_notification = (
        db.query(Notification)
        .filter(Notification.idempotency_key == idempotency_key)
        .first()
    )

    if existing_notification:
        return {
            "notification_id": str(existing_notification.id),
            "status": existing_notification.status,
            "message": "Duplicate request; returning existing notification",
        }

    try:
        rate_limit = check_rate_limit(payload.user_id)
    except RuntimeError as exc:
        NOTIFICATION_REQUESTS_REJECTED.labels(
            reason="service_unavailable"
        ).inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Rate-limit service is temporarily unavailable",
        ) from exc

    response.headers["X-RateLimit-Limit"] = str(rate_limit.limit)
    response.headers["X-RateLimit-Remaining"] = str(
        rate_limit.remaining
    )

    if not rate_limit.allowed:
        NOTIFICATION_REQUESTS_REJECTED.labels(reason="rate_limited").inc()
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                "Notification request limit exceeded. "
                f"Try again in {rate_limit.retry_after} seconds."
            ),
            headers={
                "Retry-After": str(rate_limit.retry_after),
                "X-RateLimit-Limit": str(rate_limit.limit),
                "X-RateLimit-Remaining": "0",
            },
        )
        
    # So if the same key comes again, your API does not create a second notification.
    
    rendered_subject = payload.subject
    rendered_body = payload.body
    template_id = None
    template_version = None

    if payload.template_name is not None:
        try:
            rendered = render_notification_template(
                db=db,
                template_name=payload.template_name,
                channel=payload.channel,
                template_data=payload.template_data or {},
            )
        except ValueError as exc:
            NOTIFICATION_REQUESTS_REJECTED.labels(reason="template_error").inc()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(exc),
            ) from exc

        rendered_subject = rendered.subject
        rendered_body = rendered.body
        template_id = rendered.template_id
        template_version = rendered.template_version

    # 2. Create notification with the final rendered content.
    notification = Notification(
        user_id=payload.user_id,
        channel=payload.channel,
        recipient=payload.recipient,
        subject=rendered_subject,
        body=rendered_body,
        template_id=template_id,
        template_version=template_version,
        status="PENDING",
        priority=payload.priority,
        idempotency_key=idempotency_key,
        request_id=request_id,
    )

    try:
        db.add(notification)
        db.commit()
        db.refresh(notification)

    except IntegrityError:
        db.rollback()

        existing_notification = (
            db.query(Notification)
            .filter(
                Notification.idempotency_key
                == idempotency_key
            )
            .first()
        )

        if existing_notification is None:
            raise

        return {
            "notification_id": str(existing_notification.id),
            "status": existing_notification.status,
            "message": (
                "Duplicate request; returning existing notification"
            ),
        }

    NOTIFICATIONS_CREATED.labels(
        channel=notification.channel,
        priority=notification.priority,
    ).inc()

    logger.info(
        "notification_created",
        extra={
            "service": "notification-api",
            "request_id": request_id,
            "notification_id": str(notification.id),
            "user_id": notification.user_id,
            "channel": notification.channel,
            "priority": notification.priority,
            "status": notification.status,
        },
    )

    try:
        publisher.publish(
            notification_id=str(notification.id),
            priority=notification.priority,
            request_id=request_id,
        )

    except Exception as exc:
        notification.status = "PUBLISH_FAILED"

        try:
            db.commit()
        except Exception:
            db.rollback()
            logger.exception(
                "publish_failure_status_update_failed",
                extra={
                    "notification_id": str(notification.id),
                    "request_id": request_id,
                },
            )

        logger.exception(
            "notification_queueing_failed",
            extra={
                "notification_id": str(notification.id),
                "request_id": request_id,
                "error_type": type(exc).__name__,
            },
        )

        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "notification_id": str(notification.id),
                "status": "PUBLISH_FAILED",
                "message": "Notification could not be queued",
            },
        ) from exc

    return {
        "notification_id": str(notification.id),
        "status": "PENDING",
        "message": "Notification accepted for processing",
    }


# 1. FastAPI validates request using Pydantic.
# 2. API checks whether user_id exists in users table.
# 3. API creates a notification row with status PENDING.
# 4. API creates an attempt row with status PROCESSING.
# 5. API publishes the notification ID to RabbitMQ and returns PENDING.
# 6. Worker loads the notification and sends the email.
# 7. Worker updates both final statuses, commits, and acknowledges the message.
