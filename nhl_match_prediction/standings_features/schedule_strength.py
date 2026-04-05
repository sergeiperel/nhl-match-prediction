import pandas as pd


def add_schedule_strength(standings: pd.DataFrame) -> pd.DataFrame:
    standings = standings.sort_values(["team_abbrev", "date"]).reset_index(drop=True)

    windows = [3, 5, 10]

    for last_n in windows:
        standings[f"point_pct_last{last_n}"] = standings.groupby("team_abbrev")[
            "point_pctg"
        ].transform(lambda s, n=last_n: s.shift(1).rolling(n, min_periods=1).mean())

        standings[f"goal_diff_last{last_n}"] = standings.groupby("team_abbrev")[
            "goal_diff"
        ].transform(lambda s, n=last_n: s.shift(1).rolling(n, min_periods=1).mean())

    return standings
