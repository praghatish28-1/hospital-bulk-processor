from app.core.config import settings

broker_url = settings.REDIS_URL
result_backend = settings.REDIS_URL

worker_concurrency = 10
worker_prefetch_multiplier = 1
task_acks_late = True
task_reject_on_worker_lost = True
task_routes = {
    "tasks.orchestrator": {"queue": "orchestration"},
    "tasks.create_hospital": {"queue": "hospital_creation"},
    "tasks.activate_batch": {"queue": "activation"},
    "tasks.resume_batch": {"queue": "orchestration"},
}
task_soft_time_limit = 30
task_time_limit = 60
task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
timezone = "UTC"
enable_utc = True
broker_connection_retry_on_startup = True

