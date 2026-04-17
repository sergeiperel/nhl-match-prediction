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
            g.season,
            ROW_NUMBER() OVER (
                PARTITION BY tgs.team_id, g.season
                ORDER BY g.date
            ) AS game_number
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
                    PARTITION BY team_id, season
                    ORDER BY game_date
                ) AS prev_game_date

            FROM team_games_with_date
        ),

        rest_calc AS (
            SELECT
                *,

                JULIANDAY(game_date) - JULIANDAY(prev_game_date) AS raw_rest

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

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_team_features_lookup
        ON team_game_stats_features(team_abbr, season, game_date);
    """)

    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_standings_lookup
        ON standings_daily(team_abbrev, season_id, date);
    """)

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

                s.team_name,
                s.conference,
                s.division,
                s.conference_rank,
                s.division_rank,
                s.wildcard_rank,
                s.league_rank,
                s.home_win_pctg,
                s.road_win_pctg,
                s.goals_for_per_game,
                s.goals_against_per_game,
                s.l10_win_pctg,
                s.point_pct_last3,
                s.point_pct_last5,
                s.point_pct_last10,
                s.goal_diff_last3,
                s.goal_diff_last5,
                s.goal_diff_last10,
                s.is_wildcard_race,


                ROW_NUMBER() OVER (
                    PARTITION BY t.game_id, t.team_id
                    ORDER BY s.date DESC
                ) AS rn

            FROM team_game_stats_features t
            LEFT JOIN standings_daily s
                ON s.team_abbrev = t.team_abbr
                AND s.season_id = t.season
                AND s.date = (
                    SELECT MAX(s2.date)
                    FROM standings_daily s2
                    WHERE s2.team_abbrev = t.team_abbr
                    AND s2.season_id = t.season
                    AND s2.date < t.game_date
                )
        )

        SELECT 	*,
                CASE
                WHEN game_number <= 20 THEN 0
                WHEN game_number <= 60 THEN 1
                ELSE 2
                END AS season_phase
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

            /* SAVE % */

            AVG(save_pct) OVER w3  AS starter_save_pct_last3,
            AVG(save_pct) OVER w5  AS starter_save_pct_last5,
            AVG(save_pct) OVER w10 AS starter_save_pct_last10,

            /* GOALS AGAINST */

            AVG(goals_against) OVER w5 AS starter_ga_last5,

            /* VARIANCE */

            AVG(save_pct * save_pct) OVER w5
            - (AVG(save_pct) OVER w5 * AVG(save_pct) OVER w5)
            AS starter_save_pct_var_last5,

            AVG(CAST(backup_goalie_flag AS INTEGER)) OVER w5 	AS backup_goalie_last5,
            AVG(CAST(quality_start AS INTEGER)) OVER w5 		AS quality_start_last5,
            AVG(CAST(elite_goalie_game AS INTEGER)) OVER w5 	AS elite_goalie_last5,
            AVG(CAST(bad_goalie_game AS INTEGER)) OVER w5 		AS bad_goalie_last5,
            SUM(CAST(played_full_game AS INTEGER)) OVER w10 	AS played_full_games_last10



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

        -- =========================
        -- HOME TEAM
        -- =========================
        SELECT
            p.game_id,
            g.season,
            g.date AS game_date,
            g.home_team_id AS team_id,
            1 AS is_home,

            /* ================= BASIC ================= */

            p.home_goals               AS goals,
            p.home_shots_on_goal       AS shots_on_goal,
            p.home_missed_shots        AS missed_shots,
            p.home_blocked_shots       AS blocked_shots,
            p.home_hits                AS hits,
            p.home_giveaways           AS giveaways,
            p.home_takeaways           AS takeaways,
            p.home_faceoffs            AS faceoffs,
            p.home_penalties           AS penalties,
            p.home_penalty_minutes     AS penalty_minutes,

            /* ================= SPECIAL TEAMS ================= */

            p.home_pp_goals,
            p.home_pp_opportunities,
            p.home_pk_goals_against,
            p.home_pk_opportunities,

            p.home_pp_goals / NULLIF(p.home_pp_opportunities, 0) AS pp_eff,
            (p.home_pk_opportunities - p.home_pk_goals_against)
            / NULLIF(p.home_pk_opportunities, 0)                 AS pk_eff,

            /* ================= SHOOTING ================= */

            p.home_shot_attempts,
            p.home_shots_on_goal * 1.0 / NULLIF(p.home_shot_attempts, 0) AS shot_accuracy,

            p.home_high_danger_shots * 1.0 / NULLIF(p.home_shots_on_goal, 0) AS high_danger_pct,
            p.home_slot_shots * 1.0 / NULLIF(p.home_shots_on_goal, 0) AS slot_pct,

            /* ================= POSSESSION ================= */

            p.home_corsi_5v5 AS corsi,
            p.home_fenwick_5v5 AS fenwick,

            /* ================= XG ================= */

            p.home_xg_sum AS xg,
            p.home_xg_sum * 1.0 / NULLIF(p.home_shot_attempts, 0) AS xg_per_shot,

            /* ================= GOALS BY TYPE ================= */

            p.home_even_goals,
            p.home_goals_PP,
            p.home_goals_SH,

            /* ================= SHOT QUALITY ================= */

            p.home_avg_shot_distance,
            p.home_avg_shot_angle,

            /* ================= GAME FLOW ================= */

            p.home_events_leading,
            p.away_events_leading,
            p.events_tied,
            p.stoppages_total,

            /* ================= DIFFERENCES ================= */

            p.home_shots_on_goal - p.away_shots_on_goal AS diff_shots,
            p.home_hits - p.away_hits AS diff_hits,
            p.home_xg_sum - p.away_xg_sum AS diff_xg,
            p.home_corsi_5v5 - p.away_corsi_5v5 AS diff_corsi,
            p.home_fenwick_5v5 - p.away_fenwick_5v5 AS diff_fenwick,

            p.home_pp_goals / NULLIF(p.home_pp_opportunities, 0)
                - p.away_pp_goals / NULLIF(p.away_pp_opportunities, 0) AS diff_pp

        --     p.home_pk_eff - p.away_pk_eff AS diff_pk

        FROM play_by_play_stats p
        JOIN games g ON p.game_id = g.game_id


        UNION ALL


        -- =========================
        -- AWAY TEAM
        -- =========================
        SELECT
            p.game_id,
            g.season,
            g.date AS game_date,
            g.away_team_id AS team_id,
            0 AS is_home,

            /* ================= BASIC ================= */

            p.away_goals,
            p.away_shots_on_goal,
            p.away_missed_shots,
            p.away_blocked_shots,
            p.away_hits,
            p.away_giveaways,
            p.away_takeaways,
            p.away_faceoffs,
            p.away_penalties,
            p.away_penalty_minutes,

            /* ================= SPECIAL TEAMS ================= */

            p.away_pp_goals,
            p.away_pp_opportunities,
            p.away_pk_goals_against,
            p.away_pk_opportunities,

            p.away_pp_goals / NULLIF(p.away_pp_opportunities, 0) AS pp_eff,
            (p.away_pk_opportunities - p.away_pk_goals_against)
            / NULLIF(p.away_pk_opportunities, 0)                 AS pk_eff,

            /* ================= SHOOTING ================= */

            p.away_shot_attempts,
            p.away_shots_on_goal * 1.0 / NULLIF(p.away_shot_attempts, 0) AS shot_accuracy,

            p.away_high_danger_shots * 1.0 / NULLIF(p.away_shots_on_goal, 0) AS high_danger_pct,
            p.away_slot_shots * 1.0 / NULLIF(p.away_shots_on_goal, 0) AS slot_pct,

            /* ================= POSSESSION ================= */

            p.away_corsi_5v5 AS corsi,
            p.away_fenwick_5v5 AS fenwick,

            /* ================= XG ================= */

            p.away_xg_sum AS xg,
            p.away_xg_sum * 1.0 / NULLIF(p.away_shot_attempts, 0) AS xg_per_shot,

            /* ================= GOALS BY TYPE ================= */

            p.away_even_goals,
            p.away_goals_PP,
            p.away_goals_SH,

            /* ================= SHOT QUALITY ================= */

            p.away_avg_shot_distance,
            p.away_avg_shot_angle,

            /* ================= GAME FLOW ================= */

            p.away_events_leading,
            p.home_events_leading,
            p.events_tied,
            p.stoppages_total,

            /* ================= DIFFERENCES ================= */

            p.away_shots_on_goal - p.home_shots_on_goal AS diff_shots,
            p.away_hits - p.home_hits AS diff_hits,
            p.away_xg_sum - p.home_xg_sum AS diff_xg,
            p.away_corsi_5v5 - p.home_corsi_5v5 AS diff_corsi,
            p.away_fenwick_5v5 - p.home_fenwick_5v5 AS diff_fenwick,

            p.away_pp_goals / NULLIF(p.away_pp_opportunities, 0)
                - p.home_pp_goals / NULLIF(p.home_pp_opportunities, 0) AS diff_pp

        --     p.away_pk_eff - p.home_pk_eff AS diff_pk

        FROM play_by_play_stats p
        JOIN games g ON p.game_id = g.game_id;
    """)

    con.execute("DROP TABLE IF EXISTS team_play_by_play_rolling;")

    con.execute("""
        CREATE TABLE team_play_by_play_rolling AS

        WITH game_level AS (
            SELECT
                team_id,
                season,
                game_id,
                game_date,

                /* ===== BASE FEATURES ===== */
                goals,
                shots_on_goal,
                missed_shots,
                blocked_shots,
                hits,
                giveaways,
                takeaways,
                faceoffs,
                penalties,
                penalty_minutes,

                /* ===== SPECIAL TEAMS ===== */
                home_pp_goals,
                home_pp_opportunities,
                home_pk_goals_against,
                home_pk_opportunities,

                /* ===== SHOOTING ===== */
                high_danger_pct,
                slot_pct,
                shot_accuracy,

                /* ===== POSSESSION ===== */
                corsi,
                fenwick,

                /* ===== XG ===== */
                xg,
                xg_per_shot,

                /* ===== DERIVED (обязательно) ===== */
                (shots_on_goal + missed_shots + blocked_shots) AS shot_attempts

            FROM team_play_by_play
        )

        SELECT
            *,

            /* ================= CUMULATIVE ================= */

            CASE
                WHEN SUM(home_pp_opportunities) OVER w_cum > 0
                THEN SUM(home_pp_goals) OVER w_cum * 1.0
                    / SUM(home_pp_opportunities) OVER w_cum
                ELSE NULL
            END AS pp_eff_season,

            CASE
                WHEN SUM(home_pk_opportunities) OVER w_cum > 0
                THEN 1 - (
                    SUM(home_pk_goals_against) OVER w_cum * 1.0
                    / SUM(home_pk_opportunities) OVER w_cum
                )
                ELSE NULL
            END AS pk_eff_season,

            /* ================= LAST 5 ================= */

            AVG(goals) OVER w5            AS goals_last5,
            AVG(shots_on_goal) OVER w5    AS shots_last5,
            AVG(shot_attempts) OVER w5     AS shot_attempts_last5,
            AVG(hits) OVER w5             AS hits_last5,
            AVG(giveaways) OVER w5        AS giveaways_last5,
            AVG(takeaways) OVER w5        AS takeaways_last5,

            AVG(corsi) OVER w5            AS corsi_last5,
            AVG(fenwick) OVER w5          AS fenwick_last5,

            AVG(high_danger_pct) OVER w5  AS high_danger_last5,
            AVG(slot_pct) OVER w5         AS slot_last5,

            AVG(xg) OVER w5               AS xg_last5,
            AVG(xg_per_shot) OVER w5      AS xg_per_shot_last5,

            /* ================= LAST 3 ================= */

            AVG(goals) OVER w3            AS goals_last3,
            AVG(shots_on_goal) OVER w3    AS shots_last3,
            AVG(shot_attempts) OVER w3     AS shot_attempts_last3,
            AVG(hits) OVER w3             AS hits_last3,
            AVG(giveaways) OVER w3        AS giveaways_last3,
            AVG(takeaways) OVER w3        AS takeaways_last3,

            AVG(corsi) OVER w3            AS corsi_last3,
            AVG(fenwick) OVER w3          AS fenwick_last3,

            AVG(high_danger_pct) OVER w3  AS high_danger_last3,
            AVG(slot_pct) OVER w3         AS slot_last3,

            AVG(xg) OVER w3               AS xg_last3,
            AVG(xg_per_shot) OVER w3      AS xg_per_shot_last3,

            /* ================= SEASON AVG ================= */

            AVG(goals) OVER w_cum         AS goals_season_avg,
            AVG(shots_on_goal) OVER w_cum AS shots_season_avg,
            AVG(xg) OVER w_cum            AS xg_season_avg

        FROM game_level

        WINDOW
        w5 AS (
            PARTITION BY team_id, season
            ORDER BY game_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ),

        w3 AS (
            PARTITION BY team_id, season
            ORDER BY game_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ),

        w_cum AS (
            PARTITION BY team_id, season
            ORDER BY game_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        );
    """)
    con.commit()
    print("✅ PLAY BY PLAY done!")

    # ------------------------------------------------------------------------------------------
    # TEAM FEATURES FULL
    # ------------------------------------------------------------------------------------------
    con.execute("DROP TABLE IF EXISTS player_stats_features_safe;")

    con.execute("""
        CREATE TABLE player_stats_features_safe AS

        WITH base AS (
            SELECT
                *,
                g.date as game_date,
                ROW_NUMBER() OVER (
                    PARTITION BY p.team_id, p.season
                    ORDER BY g.date
                ) AS rn
            FROM player_stats_features p
            LEFT JOIN games g
                ON p.game_id = g.game_id
        ),

        rolling AS (
            SELECT
                game_id,
                team_id,
                season,
                game_date,

                -- === Командные очки и суммарные фичи ===
                AVG(team_points_total) OVER w5 AS team_points_last5,
                AVG(forward_points_sum) OVER w5 AS forward_points_last5,
                AVG(pp_goals_team_sum) OVER w5 AS pp_goals_team_last5,
                AVG(team_last5_points_sum) OVER w5 AS team_last5_points_last5,

                -- === Топ-3 игроков ===
                AVG(top3_points_sum) OVER w5 AS top3_points_last5,
                AVG(top3_goals_sum) OVER w5 AS top3_goals_last5,
                AVG(top3_assists_sum) OVER w5 AS top3_assists_last5,
                AVG(top3_toi_sum) OVER w5 AS top3_toi_last5,
                AVG(top3_sog_sum) OVER w5 AS top3_sog_last5,
                AVG(top3_last5_points_sum) OVER w5 AS top3_last5_points_last5,
                AVG(top3_points_ratio) OVER w5 AS top3_points_ratio_last5,

                -- === Топ-2 защитники ===
                AVG(top2_defense_blocked_sum) OVER w5 AS top2_blocked_last5,
                AVG(top2_defense_hits_sum) OVER w5 AS top2_hits_last5,
                AVG(top2_defense_toi_sum) OVER w5 AS top2_toi_last5,
                AVG(top2_defense_points_sum) OVER w5 AS top2_points_last5,

                -- === Голкипер ===
                AVG(goalie_save_pct) OVER w5 AS goalie_save_pct_last5,
                AVG(goalie_goals_against) OVER w5 AS goalie_goals_against_last5,
                AVG(goalie_shots_against) OVER w5 AS goalie_shots_against_last5,
                SUM(goalie_played_full_game) OVER w5 AS goalie_played_full_last5,

                AVG(goalie_save_pct) OVER w10 AS goalie_save_pct_last10

            FROM base
            WINDOW
                w5 AS (
                    PARTITION BY team_id, season
                    ORDER BY game_id
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
                ),
                w10 AS (
                    PARTITION BY team_id, season
                    ORDER BY game_id
                    ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
                )
        )

        SELECT * FROM rolling;
    """)

    con.execute("DROP TABLE IF EXISTS team_features_full;")

    con.execute("""
        CREATE TABLE team_features_full AS
        SELECT
            t.*,

            -- ===== GOALIE FEATURES =====
            gtf.starter_save_pct_last3,
            gtf.starter_save_pct_last5,
            gtf.starter_save_pct_last10,
            gtf.starter_ga_last5,
            gtf.starter_save_pct_var_last5,
            gtf.backup_goalie_last5,
            gtf.played_full_games_last10,
            gtf.quality_start_last5,
            gtf.elite_goalie_last5,
            gtf.bad_goalie_last5,

            -- ===== PLAY-BY-PLAY (LAST GAMES) =====
            pb.shot_attempts_last5,
            pb.shot_attempts_last3,

            pb.corsi_last5,
            pb.corsi_last3,

            pb.fenwick_last5,
            pb.fenwick_last3,

            pb.high_danger_last5,
            pb.high_danger_last3,

            pb.slot_last5,
            pb.slot_last3,

            pb.xg_last5,
            pb.xg_last3,

            pb.xg_per_shot_last5,
            pb.xg_per_shot_last3,

            -- ===== SEASON LEVEL (ИСПРАВЛЕНО) =====
            pb.goals_season_avg,
            pb.shots_season_avg,
            pb.xg_season_avg,

            -- ===== SPECIAL TEAMS (ИСПРАВЛЕНО) =====
            -- PP%
            pb.pp_eff_season AS pp_pct,

            -- PK% (исправленный расчёт)
            pb.pk_eff_season AS pk_pct,

            -- ===== PLAYER FEATURES =====
            psf.team_points_last5,
            psf.forward_points_last5,
            psf.pp_goals_team_last5,
            psf.team_last5_points_last5,

            psf.top3_points_last5,
            psf.top3_goals_last5,
            psf.top3_assists_last5,
            psf.top3_toi_last5,
            psf.top3_sog_last5,
            psf.top3_last5_points_last5,
            psf.top3_points_ratio_last5,

            psf.top2_blocked_last5,
            psf.top2_hits_last5,
            psf.top2_toi_last5,
            psf.top2_points_last5,

            psf.goalie_save_pct_last5,
            psf.goalie_goals_against_last5,
            psf.goalie_shots_against_last5,
            psf.goalie_played_full_last5,
            psf.goalie_save_pct_last10

        FROM team_features_with_standings t

        LEFT JOIN games g
            ON t.game_id = g.game_id

        LEFT JOIN goalie_features gtf
            ON t.game_id = gtf.game_id
        AND t.team_id = gtf.team_id

        LEFT JOIN team_play_by_play_rolling pb
            ON t.game_id = pb.game_id
        AND t.team_id = pb.team_id

        LEFT JOIN player_stats_features_safe psf
            ON t.game_id = psf.game_id
        AND t.team_id = psf.team_id;
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

            /* ================= METADATA ================= */

            h.game_id,
            h.game_date,
            h.season,
            g.game_type,
            g.home_win,
            g.neutral_site,

            /* ================= IDs ================= */

            h.team_id   AS home_team_id,
            h.team_abbr AS home_team_abbr,

            a.team_id   AS away_team_id,
            a.team_abbr AS away_team_abbr,

            /* ================= ELO (САМЫЙ СИЛЬНЫЙ СИНГЛЬНЫЙ СИГНАЛ) ================= */

            g.elo_diff,
            (g.home_elo_trend_last5 - g.away_elo_trend_last5) AS elo_trend_diff,

            /* ================= TEAM QUALITY / FORM ================= */

            (h.point_pctg - a.point_pctg) AS diff_point_pctg,

            (h.goal_diff_last5_avg - a.goal_diff_last5_avg) AS diff_goal_diff_last5,
            (h.goal_diff_last10_avg - a.goal_diff_last10_avg) AS diff_goal_diff_last10,

            (h.point_pct_last5 - a.point_pct_last5) AS diff_point_pct_last5,

            /* ================= OFFENSE / DEFENSE ================= */

            (h.goals_for_per_game - a.goals_for_per_game) AS diff_goals_for_pg,
            (h.goals_against_per_game - a.goals_against_per_game) AS diff_goals_against_pg,

            /* ================= SHOT QUALITY / PBP ================= */

            (h.xg_last5 - a.xg_last5) AS diff_xg_last5,
            (h.xg_per_shot_last5 - a.xg_per_shot_last5) AS diff_xg_per_shot_last5,

            (h.corsi_last5 - a.corsi_last5) AS diff_corsi_last5,
            (h.fenwick_last5 - a.fenwick_last5) AS diff_fenwick_last5,

            (h.high_danger_last5 - a.high_danger_last5) AS diff_high_danger_last5,
            (h.slot_last5 - a.slot_last5) AS diff_slot_last5,

            /* ================= SPECIAL TEAMS ================= */

            (h.pp_pct - a.pp_pct) AS diff_pp_pct,
            (h.pk_pct - a.pk_pct) AS diff_pk_pct,

            /* ================= GOALIE ================= */

            (h.starter_save_pct_last5 - a.starter_save_pct_last5) AS diff_save_pct_last5,
            (h.starter_save_pct_last10 - a.starter_save_pct_last10) AS diff_save_pct_last10,

            (h.starter_ga_last5 - a.starter_ga_last5) AS diff_goalie_ga_last5,

            /* ================= REST / FATIGUE ================= */

            (h.rest_days - a.rest_days) AS diff_rest_days,
            (h.raw_rest - a.raw_rest) AS diff_rest_hours,

            /* ================= SEASON CONTEXT ================= */

            CASE
                WHEN h.game_number <= 20 THEN 0
                WHEN h.game_number <= 60 THEN 1
                ELSE 2
            END AS season_phase,

            (h.game_number - a.game_number) AS diff_game_number,

            /* ================= TRAVEL ================= */

            g.travel_distance_away_team,

            /* ================= TEAM IDENTITY / RANK ================= */

            (h.league_rank - a.league_rank) AS diff_league_rank,

            (h.conference_rank - a.conference_rank) AS diff_conference_rank

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
