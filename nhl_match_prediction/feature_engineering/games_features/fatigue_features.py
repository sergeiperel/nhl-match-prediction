import numpy as np
import pandas as pd

from .geo_location_data import travel_distance


def add_fatigue_features(games: pd.DataFrame) -> pd.DataFrame:
    games["date"] = pd.to_datetime(games["date"], errors="coerce")
    games = games.sort_values(["date"]).reset_index(drop=True)

    # --- Back-to-back & last_game_overtime ---
    for team_type, team_col in [("home", "home_team_abbr"), ("away", "away_team_abbr")]:
        team_grp = games.groupby(team_col, sort=False)

        # Back-to-back: разница между играми == 1 день
        games[f"{team_type}_back_to_back"] = team_grp["date"].diff().dt.days.eq(1).astype(int)

        # Last game overtime (OT или SO)
        overtime_cols = ["is_overtime", "is_shootout"]
        games[f"{team_type}_last_game_overtime"] = (
            team_grp[overtime_cols].shift().fillna(False).any(axis=1).astype(int)
        )

        # Games in last 3 / 7 days
        for n_days in [3, 7]:
            col_name = f"{team_type}_games_last_{n_days}_days"
            counts = []
            for _team, df_team in team_grp:
                dates = df_team["date"]

                diffs = dates.values[:, None] - dates.values[None, :]
                diffs_days = diffs.astype("timedelta64[D]")
                mask = (diffs_days > 0) & (diffs_days <= n_days)
                counts.extend(mask.sum(axis=1))
            games[col_name] = counts

    # --- Away trip length & travel from previous city distance ---
    unique_teams = pd.unique(games[["home_team_abbr", "away_team_abbr"]].values.ravel())
    travel_cache = {(h, a): travel_distance(h, a) for h in unique_teams for a in unique_teams}

    away_trip_length = np.zeros(len(games), dtype=int)
    away_travel_dist = np.zeros(len(games), dtype=float)
    last_game_location = {}  # last location (home arena) каждой команды

    for idx, row in games.iterrows():
        team = row["away_team_abbr"]
        prev_loc = last_game_location.get(team)

        if prev_loc is None:
            away_trip_length[idx] = 0
            away_travel_dist[idx] = np.nan
        else:
            away_trip_length[idx] = (
                row["away_back_to_back"] + 1 if prev_loc != row["home_team_abbr"] else 1
            )
            away_travel_dist[idx] = travel_cache.get((prev_loc, row["home_team_abbr"]), np.nan)

        last_game_location[team] = row["home_team_abbr"]

    games["away_away_trip_length"] = away_trip_length
    games["away_travel_from_previous_city"] = away_travel_dist

    return games
