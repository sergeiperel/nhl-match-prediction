import sqlite3
import uuid
from datetime import UTC, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

task_id = str(uuid.uuid4())

cursor.execute(
    """
INSERT INTO tasks (id, task_type, status, created_at)
VALUES (?, ?, ?, ?)
""",
    (task_id, "train_model", "pending", datetime.now(UTC).isoformat()),
)

conn.commit()
conn.close()

print("✅ Задача добавлена:", task_id)
