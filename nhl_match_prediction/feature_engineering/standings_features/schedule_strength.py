import pandas as pd


def add_schedule_strength(standings: pd.DataFrame) -> pd.DataFrame:
    standings = standings.sort_values(["team_abbrev", "date"]).reset_index(drop=True)

    windows = [3, 5, 10]
    grouped = standings.groupby("team_abbrev", sort=False)

    for n in windows:
        standings[f"point_pct_last{n}"] = grouped["point_pctg"].transform(
            lambda s, window=n: s.shift(1).rolling(window, min_periods=1).mean()
        )

        standings[f"goal_diff_last{n}"] = grouped["goal_diff"].transform(
            lambda s, window=n: s.shift(1).rolling(window, min_periods=1).mean()
        )

    return standings
