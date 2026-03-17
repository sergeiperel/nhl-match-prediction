from celery import Celery

# import nhl_match_prediction.tasks.train_tasks

celery_app = Celery(
    "nhl_tasks",
    broker="redis://redis:6379/0",  # redis из docker-compose
    backend="redis://redis:6379/0",
)

celery_app.conf.update(
    task_track_started=True,
    include=["nhl_match_prediction.tasks.train_tasks"],
)
