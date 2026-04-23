import pandas as pd


def add_performance_features(games: pd.DataFrame) -> pd.DataFrame:
    """
    Добавляет фичи:
    - home_team_home_win_pct_season
    - away_team_away_win_pct_season
    - home_team_home_goal_diff
    """
    games = games.sort_values("date").reset_index(drop=True)

    games["home_team_home_win_pct_season"] = games.groupby(["season", "home_team_id"])[
        "home_win"
    ].transform(lambda x: x.shift().expanding().mean())

    games["away_team_away_win_pct_season"] = (
        games.assign(away_win=1 - games["home_win"])
        .groupby(["season", "away_team_id"])["away_win"]
        .transform(lambda x: x.shift().expanding().mean())
    )

    games["home_team_home_goal_diff"] = (
        (games["home_score"] - games["away_score"])
        .groupby(games["season"].astype(str) + "_" + games["home_team_id"].astype(str))
        .transform(lambda x: x.shift().expanding().mean())
    )

    return games
