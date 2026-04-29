import time

from sqlalchemy import text

from nhl_match_prediction.etl_pipeline.connection import get_engine
from nhl_match_prediction.etl_pipeline.init_db import load_all_csv_to_postgres


def wait_for_db(retries=30):
    engine = get_engine()

    for _ in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ DB is ready")
            return
        except Exception:
            print("⏳ Waiting for DB...")
            time.sleep(2)

    raise Exception("❌ DB not ready")


def run_all():
    wait_for_db()
    load_all_csv_to_postgres()


if __name__ == "__main__":
    run_all()
