import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"
# print(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
    DROP TABLE IF EXISTS tasks;
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,          -- UUID задачи
        task_type TEXT NOT NULL,      -- тип задачи (например, train_model)
        status TEXT NOT NULL,         -- pending / running / done / failed
        created_at TEXT NOT NULL,     -- время создания
        started_at TEXT,              -- время начала выполнения
        finished_at TEXT,             -- время завершения
        result TEXT,                  -- результат (путь к модели, метрики)
        error TEXT                    -- текст ошибки
    )
""")

conn.commit()
conn.close()

print("✅ Таблица tasks создана")
