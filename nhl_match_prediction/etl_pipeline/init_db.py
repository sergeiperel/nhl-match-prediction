import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from nhl_match_prediction.etl_pipeline.connection import get_engine

# load_dotenv()

DB_URL = os.environ["DATABASE_URL"]
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = BASE_DIR / "data" / "processed"


TABLES = {
    "games": "games.csv",
    "team_game_stats": "team_game_stats.csv",
    "goalie_game_stats": "goalie_game_stats.csv",
    "standings_daily": "standings_daily.csv",
    "roster_snapshot": "roster_snapshot.csv",
    "play_by_play_stats": "play_by_play_stats.csv",
    "schedule_games": "schedule_games.csv",
    "player_stats": "player_stats.csv",
    "player_stats_features": "player_stats_features.csv",
    "arenas_data": "arenas_data.csv",
    "team_logo": "team_logo.csv",
}


# -------------------------
# PRIMARY KEYS
# -------------------------
def create_primary_keys(engine):
    queries = [
        "ALTER TABLE games ADD PRIMARY KEY (game_id)",
        "ALTER TABLE play_by_play_stats ADD PRIMARY KEY (game_id)",
        "ALTER TABLE schedule_games ADD PRIMARY KEY (game_id)",
        "ALTER TABLE team_game_stats ADD PRIMARY KEY (game_id, team_id)",
        "ALTER TABLE goalie_game_stats ADD PRIMARY KEY (game_id, goalie_id)",
        "ALTER TABLE player_stats ADD PRIMARY KEY (game_id, player_id, team_id)",
        "ALTER TABLE player_stats_features ADD PRIMARY KEY (game_id, team_id)",
        "ALTER TABLE standings_daily ADD PRIMARY KEY (team_abbrev, date)",
        "ALTER TABLE roster_snapshot ADD PRIMARY KEY (season, team_abbrev, player_id)",
    ]

    with engine.begin() as conn:
        for q in queries:
            try:
                conn.execute(text(q))
            except Exception as e:
                print(f"⚠️ PK skip: {e}")


# -------------------------
# INDEXES
# -------------------------
def create_indexes(engine):
    queries = [
        # базовые
        "CREATE INDEX IF NOT EXISTS idx_games_game_id ON games(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_game_id ON play_by_play_stats(game_id)",
        # team stats
        "CREATE INDEX IF NOT EXISTS idx_team_stats_game_id ON team_game_stats(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_team_stats_team_id ON team_game_stats(team_id)",
        # goalie
        "CREATE INDEX IF NOT EXISTS idx_goalie_stats_game_id ON goalie_game_stats(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_goalie_stats_goalie_id ON goalie_game_stats(goalie_id)",
        # player
        "CREATE INDEX IF NOT EXISTS idx_player_stats_game_id ON player_stats(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_stats_player_id ON player_stats(player_id)",
        "CREATE INDEX IF NOT EXISTS idx_player_stats_team_id ON player_stats(team_id)",
        # schedule
        "CREATE INDEX IF NOT EXISTS idx_schedule_game_id ON schedule_games(game_id)",
        # standings
        "CREATE INDEX IF NOT EXISTS idx_standings_team_id ON standings_daily(team_abbrev, date)",
        # xG tables
        "CREATE INDEX IF NOT EXISTS idx_pbp_events_game_id ON pbp_events_features(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_events_event_type ON pbp_events_features(event_type)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_events_team_id ON pbp_events_features(team_id)",
        "CREATE INDEX IF NOT EXISTS idx_pbp_events_game_time ON pbp_events_features(game_id, game_time)",  # noqa: E501
        "CREATE INDEX IF NOT EXISTS idx_shots_game_id ON pbp_shots_features(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_shots_team_id ON pbp_shots_features(team_id)",
        "CREATE INDEX IF NOT EXISTS idx_shots_game_time ON pbp_shots_features(game_id, game_time)",
        "CREATE INDEX IF NOT EXISTS idx_games_xg_date ON game_xg_features(game_date)",
        "CREATE INDEX IF NOT EXISTS idx_games_xg_home_team ON game_xg_features(home_team_id)",
        "CREATE INDEX IF NOT EXISTS idx_games_xg_away_team ON game_xg_features(away_team_id)",
    ]

    with engine.begin() as conn:
        for q in queries:
            conn.execute(text(q))


# -------------------------
# XG TABLES
# -------------------------
def create_xg_tables(engine):
    queries = [
        # =========================
        # EVENTS FEATURES
        # =========================
        """
        CREATE TABLE IF NOT EXISTS pbp_events_features (
            -- =========================
            -- KEYS
            -- =========================
            game_id        BIGINT NOT NULL,
            event_idx      INT NOT NULL,

            -- =========================
            -- BASIC INFO
            -- =========================
            game_date      DATE,

            team_id        INT,
            is_home        BOOLEAN,

            period         INT,
            game_time      INT,

            -- =========================
            -- COORDINATES / GEOMETRY
            -- =========================
            x              DOUBLE PRECISION,
            y              DOUBLE PRECISION,
            distance       DOUBLE PRECISION,
            angle          DOUBLE PRECISION,

            -- =========================
            -- EVENT INFO
            -- =========================
            shot_type          TEXT,
            event_type         TEXT,
            situation          DOUBLE PRECISION,

            -- =========================
            -- DYNAMICS
            -- =========================
            delta_t        DOUBLE PRECISION,
            delta_d        DOUBLE PRECISION,
            speed          DOUBLE PRECISION,
            delta_angle    DOUBLE PRECISION,
            prev_event_type TEXT,

            -- =========================
            -- TARGET
            -- =========================
            goal           INT,

            -- =========================
            -- MANPOWER FEATURES
            -- =========================
            team_skaters   DOUBLE PRECISION,
            opp_skaters    DOUBLE PRECISION,
            man_diff       DOUBLE PRECISION,
            total_skaters  DOUBLE PRECISION,

            is_powerplay       INT,
            is_penalty_kill    INT,
            is_even            INT,
            is_empty_net       INT,

            -- =========================
            -- CONTEXT FEATURES
            -- =========================
            prev_shot_event    TEXT,
            prev_time          DOUBLE PRECISION,
            is_rebound         INT,
            situation_compact  TEXT,
            is_shot_event      INT,

            -- =========================
            -- META
            -- =========================
            created_at     TIMESTAMP DEFAULT NOW(),
            updated_at     TIMESTAMP DEFAULT NOW(),

            -- =========================
            -- PRIMARY KEY
            -- =========================
            PRIMARY KEY (game_id, event_idx)
        );
        """,
        # =========================
        # SHOTS FEATURES
        # =========================
        """
        CREATE TABLE IF NOT EXISTS pbp_shots_features (
            -- =========================
            -- KEYS (same as events)
            -- =========================
            game_id        BIGINT NOT NULL,
            event_idx      INT NOT NULL,

            -- =========================
            -- BASIC INFO
            -- =========================
            game_date      DATE,

            team_id        INT,
            is_home        BOOLEAN,

            period         INT,
            game_time      INT,

            -- =========================
            -- COORDINATES / GEOMETRY
            -- =========================
            x              DOUBLE PRECISION,
            y              DOUBLE PRECISION,
            distance       DOUBLE PRECISION,
            angle          DOUBLE PRECISION,

            -- =========================
            -- EVENT INFO
            -- =========================
            shot_type          TEXT,
            event_type         TEXT,
            situation          DOUBLE PRECISION,

            -- =========================
            -- DYNAMICS
            -- =========================
            delta_t        DOUBLE PRECISION,
            delta_d        DOUBLE PRECISION,
            speed          DOUBLE PRECISION,
            delta_angle    DOUBLE PRECISION,
            prev_event_type TEXT,

            -- =========================
            -- TARGET
            -- =========================
            goal           INT,

            -- =========================
            -- MANPOWER FEATURES
            -- =========================
            team_skaters   DOUBLE PRECISION,
            opp_skaters    DOUBLE PRECISION,
            man_diff       DOUBLE PRECISION,
            total_skaters  DOUBLE PRECISION,

            is_powerplay       INT,
            is_penalty_kill    INT,
            is_even            INT,
            is_empty_net       INT,

            -- =========================
            -- CONTEXT FEATURES
            -- =========================
            prev_shot_event    TEXT,
            prev_time          DOUBLE PRECISION,
            is_rebound         INT,
            situation_compact  TEXT,
            is_shot_event      INT,

            -- =========================
            -- META
            -- =========================
            created_at     TIMESTAMP DEFAULT NOW(),

            -- =========================
            -- PRIMARY KEY
            -- =========================
            PRIMARY KEY (game_id, event_idx)
        );
        """,
        # =========================
        # XG PER SHOT
        # =========================
        """
        CREATE TABLE IF NOT EXISTS pbp_shots_xg (
            game_id BIGINT NOT NULL,
            event_idx INT NOT NULL,

            xg FLOAT NOT NULL,
            model_version TEXT NOT NULL,

            created_at TIMESTAMP DEFAULT NOW(),

            PRIMARY KEY (game_id, event_idx)
        )
        """,
        # =========================
        # GAME LEVEL XG
        # =========================
        """
        CREATE TABLE IF NOT EXISTS game_xg_features (
            -- =========================
            -- PRIMARY KEY
            -- =========================
            game_id        BIGINT PRIMARY KEY,

            -- =========================
            -- BASIC INFO
            -- =========================
            game_date      DATE NOT NULL,

            home_team_id   INT NOT NULL,
            away_team_id   INT NOT NULL,

            -- =========================
            -- RAW AGGREGATES
            -- =========================
            home_xg        DOUBLE PRECISION NOT NULL,
            away_xg        DOUBLE PRECISION NOT NULL,

            home_shots     INT NOT NULL,
            away_shots     INT NOT NULL,

            home_goals     INT NOT NULL,
            away_goals     INT NOT NULL,

            -- =========================
            -- EFFICIENCY
            -- =========================
            home_xg_per_shot DOUBLE PRECISION,
            away_xg_per_shot DOUBLE PRECISION,

            -- =========================
            -- ROLLING FEATURES (nullable!)
            -- =========================
            home_xg_ewm       DOUBLE PRECISION,
            away_xg_ewm       DOUBLE PRECISION,

            home_xg_last5     DOUBLE PRECISION,
            away_xg_last5     DOUBLE PRECISION,

            home_shots_last5  DOUBLE PRECISION,
            away_shots_last5  DOUBLE PRECISION,

            xg_diff_last5     DOUBLE PRECISION,
            shots_diff_last5  DOUBLE PRECISION,

            -- =========================
            -- META
            -- =========================
            updated_at     TIMESTAMP DEFAULT NOW()
        );
        """,
        # =========================
        # INCREMENTAL CONTROL
        # =========================
        """
        CREATE TABLE IF NOT EXISTS xg_game_status (
            game_id BIGINT PRIMARY KEY,
            needs_recompute BOOLEAN DEFAULT TRUE,
            is_final BOOLEAN DEFAULT FALSE,
            last_updated TIMESTAMP DEFAULT NOW()
        )
        """,
    ]

    with engine.begin() as conn:
        for q in queries:
            conn.execute(text(q))


# -------------------------
# LOAD CSV
# -------------------------
def load_all_csv_to_postgres():
    engine = get_engine()

    for table, file_name in TABLES.items():
        file_path = DATA_PATH / file_name

        if not file_path.exists():
            print(f"❌ {file_name} not found")
            continue

        print(f"📥 Loading {file_name} → {table}")

        df = pd.read_csv(file_path, low_memory=False)

        df.to_sql(
            table,
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000,
        )

    create_xg_tables(engine)
    create_primary_keys(engine)
    create_indexes(engine)

    print("✅ All tables loaded + PK + indexed")


if __name__ == "__main__":
    load_all_csv_to_postgres()
