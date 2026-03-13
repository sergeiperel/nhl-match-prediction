import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"


def upcoming_match_features() -> None:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA cache_size=1000000;")

    # ------------------------------------------------------------------------------------------
    # UPCOMING GAMES
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS upcoming_games;")

    con.execute("""
        CREATE TABLE upcoming_games AS
        SELECT
            game_id,
            game_date,
            season,
            home_team_id,
            away_team_id
        FROM schedule_games
        WHERE game_state = "FUT";
    """)

    con.commit()
    print("✅ UPCOMING GAMES done!")

    # ------------------------------------------------------------------------------------------
    # TEAM LAST FEATURES
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS team_last_features;")

    con.execute("""
        CREATE TABLE team_last_features AS
        WITH last_games as (
        SELECT * FROM (
        SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY team_id
                    ORDER BY game_date DESC
                ) AS row_num
            FROM team_features_full
        )
        WHERE row_num = 1)

        SELECT * FROM last_games t1
        LEFT JOIN games t2
            ON t1.game_id = t2.game_id
        WHERE
            game_type in (1, 2, 3) and
            strftime("%Y", date()) <= SUBSTRING(t2.season, 5, 4)
        ORDER BY date DESC, game_id desc;
    """)

    con.commit()
    print("✅ TEAM LAST FEATURES done!")

    # ------------------------------------------------------------------------------------------
    # UPCOMING MATCH FEATURES
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS upcoming_match_features;")

    con.execute("""
        CREATE TABLE upcoming_match_features AS
        SELECT
            sg.game_id,
            sg.game_date as game_date,
            sg.season,

            NULL AS home_win,

            /* ================= IDs ================= */

            h.team_id   AS home_team_id,
            h.team_abbr AS home_team_abbr,

            a.team_id   AS away_team_id,
            a.team_abbr AS away_team_abbr,

            /* ================= HOME FEATURES ================= */

            h.pp_goals_last5_avg           AS home_pp_goals_last5_avg,
            h.pim_last5_avg                AS home_pim_last5_avg,
            h.turnover_diff_last5_avg      AS home_turnover_diff_last5_avg,
            h.goals_season_avg             AS home_goals_season_avg,
            h.shots_season_avg             AS home_shots_season_avg,
            h.point_pctg                   AS home_point_pctg,
            h.streak_count                 AS home_streak_count,
            h.rest_days                    AS home_rest_days,

            h.goals_last5_avg              AS home_goals_last5_avg,
            h.shots_last5_avg              AS home_shots_last5_avg,
            h.faceoff_last5_avg            AS home_faceoff_last5_avg,

            h.starter_save_pct_last5       AS home_starter_save_pct_last5,
            h.starter_ga_last5             AS home_starter_ga_last5,
            h.starter_save_pct_var_last5   AS home_starter_save_pct_var_last5,

            /* ================= AWAY FEATURES ================= */

            a.pp_goals_last5_avg           AS away_pp_goals_last5_avg,
            a.pim_last5_avg                AS away_pim_last5_avg,
            a.turnover_diff_last5_avg      AS away_turnover_diff_last5_avg,
            a.goals_season_avg             AS away_goals_season_avg,
            a.shots_season_avg             AS away_shots_season_avg,
            a.point_pctg                   AS away_point_pctg,
            a.streak_count                 AS away_streak_count,
            a.rest_days                    AS away_rest_days,

            a.goals_last5_avg              AS away_goals_last5_avg,
            a.shots_last5_avg              AS away_shots_last5_avg,
            a.faceoff_last5_avg            AS away_faceoff_last5_avg,

            a.starter_save_pct_last5       AS away_starter_save_pct_last5,
            a.starter_ga_last5             AS away_starter_ga_last5,
            a.starter_save_pct_var_last5   AS away_starter_save_pct_var_last5,

            /* ================= PBP HOME ================= */

            h.shot_attempts_last5          AS home_shot_attempts_last5,
            h.corsi_5v5_last5              AS home_corsi_last5,
            h.high_danger_ratio_last5      AS home_hd_ratio_last5,
            h.slot_ratio_last5             AS home_slot_ratio_last5,
            h.avg_shot_distance_last5      AS home_avg_shot_dist_last5,
            h.avg_xg_proxy_last5           AS home_avg_xg_proxy_last5,
            h.avg_xg_5v5_last5             AS home_avg_xg_5v5_last5,

            /* ================= PBP AWAY ================= */

            a.shot_attempts_last5          AS away_shot_attempts_last5,
            a.corsi_5v5_last5              AS away_corsi_last5,
            a.high_danger_ratio_last5      AS away_hd_ratio_last5,
            a.slot_ratio_last5             AS away_slot_ratio_last5,
            a.avg_shot_distance_last5      AS away_avg_shot_dist_last5,
            a.avg_xg_proxy_last5           AS away_avg_xg_proxy_last5,
            a.avg_xg_5v5_last5             AS away_avg_xg_5v5_last5,

            /* ================= DIFF PBP ================= */

            (h.shot_attempts_last5 - a.shot_attempts_last5)         AS diff_shot_attempts_last5,
            (h.corsi_5v5_last5 - a.corsi_5v5_last5)                 AS diff_corsi_last5,
            (h.high_danger_ratio_last5 - a.high_danger_ratio_last5) AS diff_hd_ratio_last5,
            (h.slot_ratio_last5 - a.slot_ratio_last5)               AS diff_slot_ratio_last5,
            (h.avg_shot_distance_last5 - a.avg_shot_distance_last5) AS diff_avg_shot_dist_last5,
            (h.avg_xg_proxy_last5 - a.avg_xg_proxy_last5)           AS diff_avg_xg_proxy_last5,
            (h.avg_xg_5v5_last5 - a.avg_xg_5v5_last5)               AS diff_avg_xg_5v5_last5,

            /* ================= DIFF TEAM ================= */

            (h.goals_last5_avg - a.goals_last5_avg)                 AS diff_goals_last5,
            (h.shots_last5_avg - a.shots_last5_avg)                 AS diff_shots_last5,
            (h.faceoff_last5_avg - a.faceoff_last5_avg)             AS diff_faceoff_last5,
            (h.starter_save_pct_last5 - a.starter_save_pct_last5)   AS diff_goalie_save_pct_last5,
            (h.point_pctg - a.point_pctg)                           AS diff_point_pctg,
            (h.rest_days - a.rest_days)                             AS diff_rest_days

        FROM schedule_games sg

        JOIN team_last_features h
            ON h.team_id = sg.home_team_id

        JOIN team_last_features a
            ON a.team_id = sg.away_team_id

        WHERE sg.game_state = "FUT"
        and strftime("%Y", date()) <= SUBSTRING(sg.season, 5, 4);
    """)

    con.commit()
    print("✅ UPCOMING MATCH FEATURES done!")

    con.close()


if __name__ == "__main__":
    upcoming_match_features()
