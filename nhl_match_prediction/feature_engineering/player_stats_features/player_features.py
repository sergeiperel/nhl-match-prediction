import numpy as np
import pandas as pd

FULL_GAME_MINUTES = 59


def process_teams(g, row):
    # =========================
    # TEAM TOTALS
    # =========================

    team_points_total = g["total_points"].sum(skipna=True)

    row["team_points_total"] = team_points_total
    row["team_points_std"] = g["total_points"].std(skipna=True)

    row["hits_sum"] = g["hits"].sum(skipna=True)
    row["pim_sum"] = g["pim"].sum(skipna=True)
    row["takeaways_sum"] = g["takeaways"].sum(skipna=True)
    row["giveaways_sum"] = g["giveaways"].sum(skipna=True)
    row["faceoff_avg"] = g["faceoffWinningPctg"].mean(skipna=True)

    row["pp_goals_team_sum"] = g["powerPlayGoals"].sum(skipna=True)
    # row["team_last5_points_sum"] = g["last_n_games_points"].sum(skipna=True)

    return team_points_total


def process_skaters(g, row, team_points_total):
    # =========================
    # SKATERS
    # =========================

    skaters = g[g["position"] != "G"]

    if not skaters.empty:
        top3 = skaters.sort_values("total_points", ascending=False).head(3)

        row["top3_points_sum"] = top3["total_points"].sum()
        row["top3_goals_sum"] = top3["total_goals"].sum()
        row["top3_assists_sum"] = top3["total_assists"].sum()
        row["top3_toi_sum"] = top3["toi_minutes"].sum()
        row["top3_sog_sum"] = top3["sog"].sum()
        row["top3_faceoff_avg"] = top3["faceoffWinningPctg"].mean()

        # row["top3_last5_points_sum"] = top3["last_n_games_points"].sum()
        row["top3_pp_goals_sum"] = top3["powerPlayGoals"].sum()

        row["top3_points_ratio"] = (
            row["top3_points_sum"] / team_points_total if team_points_total > 0 else 0
        )


def process_defence(g, row):
    # =========================
    # DEFENSE
    # =========================

    defense = g[g["position"] == "D"]

    if not defense.empty:
        top2_def = defense.sort_values("toi_minutes", ascending=False).head(2)

        row["top2_defense_blocked_sum"] = top2_def["blockedShots"].sum()
        row["top2_defense_hits_sum"] = top2_def["hits"].sum()
        row["top2_defense_toi_sum"] = top2_def["toi_minutes"].sum()
        row["top2_defense_points_sum"] = top2_def["total_points"].sum()
        row["top2_defense_pp_goals"] = top2_def["powerPlayGoals"].sum()

        row["defense_hits_sum"] = defense["hits"].sum()


def process_forwards(g, row):
    # =========================
    # FORWARD AGGREGATES
    # =========================

    forwards = g[g["position"].isin(["C", "L", "R"])]

    if not forwards.empty:
        row["forward_points_sum"] = forwards["total_points"].sum()
        row["avg_forward_faceoff"] = forwards["faceoffWinningPctg"].mean()


def process_goalie(g, row):
    # =========================
    # GOALIE
    # =========================

    goalies = g[g["position"] == "G"]

    if not goalies.empty:
        starter = goalies[goalies["starter"]]

        goalie = (
            starter.iloc[0]
            if not starter.empty
            else goalies.sort_values("toi_minutes", ascending=False).iloc[0]
        )

        shots = goalie.get("shotsAgainst", np.nan)
        saves = goalie.get("saves", np.nan)

        save_pct = saves / shots if pd.notna(shots) and shots > 0 else np.nan

        row["goalie_save_pct"] = save_pct
        row["goalie_goals_against"] = goalie.get("goalsAgainst", np.nan)
        row["goalie_shots_against"] = shots
        row["goalie_saves"] = saves
        row["goalie_toi"] = goalie.get("toi_minutes", np.nan)
        row["goalie_played_full_game"] = (
            1 if goalie.get("toi_minutes", 0) >= FULL_GAME_MINUTES else 0
        )


def add_player_features(df: pd.DataFrame) -> pd.DataFrame:
    features = []

    grouped = df.groupby(["game_id", "team_id"], sort=False)

    for (game_id, team_id), g in grouped:
        row = {
            "game_id": game_id,
            "team_id": team_id,
            "season": g["season"].iloc[0],  # сохраняем сезон
        }

        team_points_total = process_teams(g, row)
        process_skaters(g, row, team_points_total)
        process_defence(g, row)
        process_forwards(g, row)
        process_goalie(g, row)

        features.append(row)

    return pd.DataFrame(features)
