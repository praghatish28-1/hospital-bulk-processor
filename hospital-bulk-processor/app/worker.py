import structlog
from celery import Celery

from app.core.config import settings


def configure_structlog() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),
        cache_logger_on_first_use=True,
    )


configure_structlog()

celery_app = Celery(
    "hospital_bulk_processor",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        "app.tasks.orchestrator",
        "app.tasks.create_hospital",
        "app.tasks.activate_batch",
        "app.tasks.resume",
    ],
)
celery_app.config_from_object("app.celeryconfig")

