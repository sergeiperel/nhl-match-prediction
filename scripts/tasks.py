import sqlite3
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter
from pydantic import BaseModel

from nhl_match_prediction.tasks.train_tasks import train_model_task

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"

router = APIRouter()


class TrainModelRequest(BaseModel):
    model_type: str  # 'logistic' или 'random_forest'


def create_task_record(task_type: str) -> str:
    """Создаёт запись в таблице tasks и возвращает task_id"""
    task_id = str(uuid4())
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO tasks (id, task_type, status, created_at) VALUES (?, ?, ?, datetime('now'))",
        (task_id, task_type, "pending"),
    )
    conn.commit()
    conn.close()
    return task_id


@router.post("/tasks/train_model")
def train_model(request: TrainModelRequest):
    """Эндпойнт для запуска обучения модели"""
    task_id = create_task_record("train_model")
    train_model_task.delay(task_id, model_type=request.model_type)
    return {"task_id": task_id, "status": "pending"}


@router.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
    row = cursor.fetchone()
    conn.close()
    if not row:
        return {"error": "Task not found"}
    keys = ["id", "task_type", "status", "created_at", "started_at", "finished_at", "result"]
    return dict(zip(keys, row[: len(keys)], strict=True))
