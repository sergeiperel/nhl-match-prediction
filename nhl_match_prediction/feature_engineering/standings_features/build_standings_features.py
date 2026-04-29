import pandas as pd

from nhl_match_prediction.etl_pipeline.connection import get_engine

from .schedule_strength import add_schedule_strength


def build_standings_daily_features(engine=None, mode="full"):
    if engine is None:
        engine = get_engine()

    # --- load ---
    standings = pd.read_sql("SELECT * FROM standings_daily", engine)
    standings["date"] = pd.to_datetime(standings["date"])
    standings = standings.sort_values(["team_abbrev", "date"]).reset_index(drop=True)

    # --- features ---
    standings = add_schedule_strength(standings)

    # --- save (always full rebuild) ---
    standings.to_sql(
        "standings_daily_features",
        engine,
        if_exists="replace",
        index=False,
    )

    print(f"✅ standings_daily_features rebuilt: {len(standings)} rows")


if __name__ == "__main__":
    build_standings_daily_features()
