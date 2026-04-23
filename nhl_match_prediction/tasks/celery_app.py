from celery import Celery

# import nhl_match_prediction.tasks.train_tasks

celery_app = Celery(
    "nhl_tasks",
    broker="redis://redis:6379/0",  # redis из docker-compose
    backend="redis://redis:6379/0",
)

celery_app.conf.update(
    task_track_started=True,
    result_expires=3600,
    include=[
        "nhl_match_prediction.tasks.train_tasks",
        "nhl_match_prediction.notifications.scheduler",
    ],
)

celery_app.conf.beat_schedule = {
    "check-upcoming-games-every-minute": {
        "task": "nhl_match_prediction.notifications.scheduler.schedule_notifications",
        "schedule": 60.0,
    },
}
