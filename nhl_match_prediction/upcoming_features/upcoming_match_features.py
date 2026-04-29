from nhl_match_prediction.etl_pipeline.connection import get_engine


def upcoming_match_features() -> None:
    engine = get_engine()

    with engine.begin() as con:
        con.execute("DROP TABLE IF EXISTS last_features;")

        con.execute("""
            CREATE TABLE last_features AS
            SELECT tf.*
            FROM team_features_full tf
            JOIN (
                SELECT team_id, MAX(game_date) AS max_date
                FROM team_features_full
                GROUP BY team_id
            ) t
            ON tf.team_id = t.team_id
            AND tf.game_date = t.max_date;
        """)

        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_tf_team_date
            ON team_features_full(team_id, game_date DESC);
        """)

        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_schedule_state
            ON schedule_games(game_state);
        """)

        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_schedule_home
            ON schedule_games(home_team_id);
        """)

        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_schedule_away
            ON schedule_games(away_team_id);
        """)

        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_future_game_id
            ON future_games_features(game_id);
        """)

        # ------------------------------------------------------------------------------------------
        # UPCOMING MATCH FEATURES
        # ------------------------------------------------------------------------------------------

        con.execute("DROP TABLE IF EXISTS upcoming_match_features;")

        con.execute("""
            CREATE TABLE upcoming_match_features AS

            SELECT

                /* ================= METADATA ================= */

                sg.game_id,
                sg.game_date,
                sg.season,
            -- 	g.game_type,
                NULL AS home_win,
                sg.neutral_site,

                /* ================= IDs ================= */

                h.team_id   AS home_team_id,
                h.team_abbr AS home_team_abbr,

                a.team_id   AS away_team_id,
                a.team_abbr AS away_team_abbr,

                /* ================= ELO ================= */

                g.elo_diff,
                (g.home_elo_trend_last5 - g.away_elo_trend_last5) AS elo_trend_diff,

                /* ================= TEAM QUALITY ================= */

                (h.point_pctg - a.point_pctg) AS diff_point_pctg,

                (h.goal_diff_last5_avg - a.goal_diff_last5_avg) AS diff_goal_diff_last5,
                (h.goal_diff_last10_avg - a.goal_diff_last10_avg) AS diff_goal_diff_last10,

                (h.point_pct_last5 - a.point_pct_last5) AS diff_point_pct_last5,

                /* ================= OFFENSE / DEFENSE ================= */

                (h.goals_for_per_game - a.goals_for_per_game) AS diff_goals_for_pg,
                (h.goals_against_per_game - a.goals_against_per_game) AS diff_goals_against_pg,

                /* ================= SHOT QUALITY ================= */

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

                COALESCE(h.starter_save_pct_last5, h.goalie_save_pct_last5, 0)
                -
                COALESCE(a.starter_save_pct_last5, a.goalie_save_pct_last5, 0)
                AS diff_save_pct_last5,
                COALESCE(h.starter_save_pct_last10, h.goalie_save_pct_last10, 0)
                -
                COALESCE(a.starter_save_pct_last10, a.goalie_save_pct_last10, 0)
                AS diff_save_pct_last10,

                COALESCE(h.starter_ga_last5, h.goalie_goals_against_last5, 0)
                -
                COALESCE(a.starter_ga_last5, a.goalie_goals_against_last5, 0)
                AS diff_goalie_ga_last5,

                /* ================= REST ================= */

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

                /* ================= RANK ================= */

                (h.league_rank - a.league_rank) AS diff_league_rank,
                (h.conference_rank - a.conference_rank) AS diff_conference_rank

            FROM schedule_games sg

            JOIN last_features h
                ON h.team_id = sg.home_team_id

            JOIN last_features a
                ON a.team_id = sg.away_team_id

            LEFT JOIN future_games_features g
                ON sg.game_id = g.game_id

            WHERE sg.game_state = 'FUT'
            AND sg.season >= '20252026'
        """)

        print("✅ UPCOMING MATCH FEATURES done!")


if __name__ == "__main__":
    upcoming_match_features()
