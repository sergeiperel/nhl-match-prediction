import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"


def build_match_features() -> None:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA cache_size=1000000;")

    # ------------------------------------------------------------------------------------------
    # INDEXES
    # ------------------------------------------------------------------------------------------

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_tgs_game_id
        ON team_game_stats(game_id);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_games_game_id
        ON games(game_id);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_tgs_team_season_date
        ON team_game_stats(team_id, game_id);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_games_date
        ON games(game_id, date, season);
    """)

    # ------------------------------------------------------------------------------------------
    # TEAM GAME STATS
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS team_games_with_date;")

    con.execute("""
        CREATE TABLE team_games_with_date AS
        SELECT
            tgs.*,
            g.date AS game_date,
            g.season
        FROM team_game_stats tgs
        JOIN games g
            ON tgs.game_id = g.game_id;
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_tgwd_team_season_date
        ON team_games_with_date(team_id, season, game_date);
    """)

    con.execute("DROP TABLE IF EXISTS team_game_stats_features;")

    con.execute("""
        CREATE TABLE team_game_stats_features AS

        WITH base AS (
            SELECT
                *,
                plus_minus AS goal_diff,
                (takeaways - giveaways) AS turnover_diff,

                LAG(game_date) OVER (
                    PARTITION BY team_id
                    ORDER BY game_date
                ) AS prev_game_date,

                LAG(season) OVER (
                    PARTITION BY team_id
                    ORDER BY game_date
                ) AS prev_season

            FROM team_games_with_date
        ),

        rest_calc AS (
            SELECT
                *,
                CASE
                    WHEN season = prev_season THEN
                        JULIANDAY(game_date) - JULIANDAY(prev_game_date)
                    ELSE NULL
                END AS raw_rest
            FROM base
        )

        SELECT
            *,

            /* ================= REST ================= */

            CAST(raw_rest AS INTEGER) AS rest_days,

            CASE
                WHEN raw_rest = 1 THEN 1
                ELSE 0
            END AS is_back_to_back,

            /* ================= WINDOWS ================= */

            -- 3 games
            AVG(goals)          OVER w3  AS goals_last3_avg,
            AVG(shots)          OVER w3  AS shots_last3_avg,
            AVG(goal_diff)      OVER w3  AS goal_diff_last3_avg,
            AVG(faceoff_pct)    OVER w3  AS faceoff_last3_avg,
            AVG(pp_goals)       OVER w3  AS pp_goals_last3_avg,
            AVG(pim)            OVER w3  AS pim_last3_avg,
            AVG(turnover_diff)  OVER w3  AS turnover_diff_last3_avg,

            -- 5 games
            AVG(goals)          OVER w5  AS goals_last5_avg,
            AVG(shots)          OVER w5  AS shots_last5_avg,
            AVG(goal_diff)      OVER w5  AS goal_diff_last5_avg,
            AVG(faceoff_pct)    OVER w5  AS faceoff_last5_avg,
            AVG(pp_goals)       OVER w5  AS pp_goals_last5_avg,
            AVG(pim)            OVER w5  AS pim_last5_avg,
            AVG(turnover_diff)  OVER w5  AS turnover_diff_last5_avg,

            -- 10 games
            AVG(goals)          OVER w10 AS goals_last10_avg,
            AVG(shots)          OVER w10 AS shots_last10_avg,
            AVG(goal_diff)      OVER w10 AS goal_diff_last10_avg,
            AVG(faceoff_pct)    OVER w10 AS faceoff_last10_avg,
            AVG(pp_goals)       OVER w10 AS pp_goals_last10_avg,
            AVG(pim)            OVER w10 AS pim_last10_avg,
            AVG(turnover_diff)  OVER w10 AS turnover_diff_last10_avg,

            /* ================= SEASON TO DATE ================= */

            AVG(goals)     OVER w_season AS goals_season_avg,
            AVG(shots)     OVER w_season AS shots_season_avg,
            AVG(goal_diff) OVER w_season AS goal_diff_season_avg

        FROM rest_calc

        WINDOW

        w3 AS (
            PARTITION BY team_id, season
            ORDER BY game_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ),

        w5 AS (
            PARTITION BY team_id, season
            ORDER BY game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),

        w10 AS (
            PARTITION BY team_id, season
            ORDER BY game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ),

        w_season AS (
            PARTITION BY team_id, season
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        );
    """)
    con.commit()
    print("✅ TEAM GAME STATS done!")

    # ------------------------------------------------------------------------------------------
    # STANDINGS DAILY
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS team_features_with_standings;")

    con.execute("""
        CREATE TABLE team_features_with_standings AS
        WITH joined AS (
            SELECT
                t.*,
                s.point_pctg,
                s.goal_diff         AS season_goal_diff,
                s.home_goal_diff,
                s.road_goal_diff,
                s.l10_goal_diff,
                s.streak_count,
                s.regulation_win_pctg,
                s.regulation_plus_ot_win_pctg,

                ROW_NUMBER() OVER (
                    PARTITION BY t.game_id, t.team_id
                    ORDER BY s.date DESC
                ) AS rn

            FROM team_game_stats_features t
            LEFT JOIN standings_daily s
                ON s.team_abbrev = t.team_abbr
               AND s.season_id = t.season
               AND s.date < t.game_date
        )

        SELECT *
        FROM joined
        WHERE rn = 1 OR rn IS NULL;
    """)
    con.commit()
    print("✅ STANDINGS DAILY done!")

    # ------------------------------------------------------------------------------------------
    # GOALIE FEATURES
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS goalie_games_with_date;")

    con.execute("""
        CREATE TABLE goalie_games_with_date AS
        SELECT
            ggs.*,
            g.date   AS game_date,
            g.season AS season
        FROM goalie_game_stats ggs
        JOIN games g ON ggs.game_id = g.game_id
        WHERE ggs.starter = 1;
    """)

    con.execute("DROP TABLE IF EXISTS goalie_features;")

    con.execute("""
        CREATE TABLE goalie_features AS
        SELECT
            game_id,
            team_id,

            AVG(save_pct) OVER w3  AS starter_save_pct_last3,
            AVG(save_pct) OVER w5  AS starter_save_pct_last5,
            AVG(save_pct) OVER w10 AS starter_save_pct_last10,

            AVG(goals_against) OVER w5 AS starter_ga_last5,

            AVG(save_pct * save_pct) OVER w5
            - (AVG(save_pct) OVER w5 * AVG(save_pct) OVER w5)
            AS starter_save_pct_var_last5

        FROM goalie_games_with_date

        WINDOW

        w3 AS (
            PARTITION BY goalie_id, season
            ORDER BY game_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ),

        w5 AS (
            PARTITION BY goalie_id, season
            ORDER BY game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),

        w10 AS (
            PARTITION BY goalie_id, season
            ORDER BY game_date
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        );
    """)
    con.commit()
    print("✅ GOALIE FEATURES done!")

    # ------------------------------------------------------------------------------------------
    # PLAY BY PLAY
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS team_play_by_play;")

    con.execute("""
        CREATE TABLE team_play_by_play AS

        SELECT
            p.game_id,
            g.season,
            g.home_team_id AS team_id,
            1 AS is_home,
            g.date as game_date,
            p.home_shots_on_goal AS shots_on_goal,
            p.home_missed_shots AS missed_shots,
            p.home_blocked_shots AS blocked_shots,
            p.home_hits AS hits,
            p.home_giveaways AS giveaways,
            p.home_takeaways AS takeaways,
            p.home_faceoffs AS faceoffs,
            p.home_penalty_minutes AS penalty_minutes,
            p.home_shot_attempts AS shot_attempts,
            p.home_corsi_5v5 AS corsi_5v5,
            p.home_high_danger_ratio AS high_danger_ratio,
            p.home_slot_ratio AS slot_ratio,
            p.home_avg_shot_distance AS avg_shot_distance,
            p.home_xg_proxy as xg_proxy,
            p.home_xg_5v5 as xg_5v5

        FROM play_by_play_stats p
        JOIN games g ON p.game_id = g.game_id

        UNION ALL

        SELECT
            p.game_id,
            g.season,
            g.away_team_id AS team_id,
            0 AS is_home,
            g.date as game_date,
            p.away_shots_on_goal,
            p.away_missed_shots,
            p.away_blocked_shots,
            p.away_hits,
            p.away_giveaways,
            p.away_takeaways,
            p.away_faceoffs,
            p.away_penalty_minutes,
            p.away_shot_attempts,
            p.away_corsi_5v5,
            p.away_high_danger_ratio,
            p.away_slot_ratio,
            p.away_avg_shot_distance,
            p.away_xg_proxy,
            p.away_xg_5v5

        FROM play_by_play_stats p
        JOIN games g ON p.game_id = g.game_id;
    """)

    con.execute("DROP TABLE IF EXISTS team_play_by_play_rolling;")

    con.execute("""
        CREATE TABLE team_play_by_play_rolling AS
        SELECT
            *,
            AVG(shot_attempts)      OVER w5 AS shot_attempts_last5,
            AVG(corsi_5v5)          OVER w5 AS corsi_5v5_last5,
            AVG(high_danger_ratio)  OVER w5 AS high_danger_ratio_last5,
            AVG(slot_ratio)         OVER w5 AS slot_ratio_last5,
            AVG(avg_shot_distance)  OVER w5 AS avg_shot_distance_last5,
            AVG(penalty_minutes)    OVER w5 AS avg_penalty_minutes_last5,
            AVG(xg_proxy)           OVER w5 AS avg_xg_proxy_last5,
            AVG(xg_5v5)             OVER w5 AS avg_xg_5v5_last5

        FROM team_play_by_play

        WINDOW w5 AS (
            PARTITION BY team_id, season
            ORDER BY game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        );
    """)
    con.commit()
    print("✅ PLAY BY PLAY done!")

    # ------------------------------------------------------------------------------------------
    # TEAM FEATURES FULL
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS team_features_full;")

    con.execute("""
        CREATE TABLE team_features_full AS
        SELECT
            t.*,
            gtf.starter_save_pct_last3,
            gtf.starter_save_pct_last5,
            gtf.starter_save_pct_last10,
            gtf.starter_ga_last5,
            gtf.starter_save_pct_var_last5,
            pb.shot_attempts_last5,
            pb.corsi_5v5_last5,
            pb.high_danger_ratio_last5,
            pb.slot_ratio_last5,
            pb.avg_shot_distance_last5,
            pb.avg_penalty_minutes_last5,
            pb.avg_xg_proxy_last5,
            pb.avg_xg_5v5_last5

        FROM team_features_with_standings t
        LEFT JOIN goalie_features gtf
            ON t.game_id = gtf.game_id
           AND t.team_id = gtf.team_id
        LEFT JOIN team_play_by_play_rolling pb
            ON t.game_id = pb.game_id
           AND t.team_id = pb.team_id;
    """)
    con.commit()
    print("✅ TEAM FEATURES FULL done!")

    # ------------------------------------------------------------------------------------------
    # MATCH FEATURES
    # ------------------------------------------------------------------------------------------

    con.execute("DROP TABLE IF EXISTS match_features;")

    con.execute("""
        CREATE TABLE match_features AS
        SELECT
            h.game_id,
            h.game_date,
            h.season,
            g.home_win,

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

        FROM team_features_full h
        JOIN team_features_full a
            ON h.game_id = a.game_id
        JOIN games g
            ON h.game_id = g.game_id
        WHERE h.is_home = 1
        AND a.is_home = 0;
    """)

    print("✅ MATCH FEATURES done!")

    con.commit()
    con.close()


if __name__ == "__main__":
    build_match_features()
