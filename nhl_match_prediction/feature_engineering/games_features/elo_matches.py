import numpy as np
import pandas as pd


def add_elo_features(games):
    K = 20  # шаг обновления рейтинга
    HOME_ADV = 80  # бонус домашней команды
    TREND_ALPHA = 0.6  # тренд Elo

    games = games.sort_values("date").reset_index(drop=True)

    teams = pd.concat([games["home_team_id"], games["away_team_id"]]).unique()
    elo = {team: 1000 for team in teams}
    elo_history = {team: [] for team in teams}
    elo_trend = {team: 0 for team in teams}

    home_elo, away_elo = [], []
    elo_diff = []
    home_trend, away_trend = [], []

    for _, row in games.iterrows():
        h = row["home_team_id"]
        a = row["away_team_id"]

        h_elo = elo[h]
        a_elo = elo[a]

        home_elo.append(h_elo)
        away_elo.append(a_elo)
        elo_diff.append(h_elo - a_elo)

        home_trend.append(elo_trend[h])
        away_trend.append(elo_trend[a])

        expected_home = 1 / (1 + 10 ** ((a_elo - (h_elo + HOME_ADV)) / 400))

        result_home = row["home_win"]

        if not pd.isna(result_home):
            result_away = 1 - result_home

            goal_diff = row.get("goal_diff", 1)
            if pd.isna(goal_diff) or goal_diff < 0:
                goal_diff = 1

            multiplier = np.log(goal_diff + 1)

            elo[h] += K * multiplier * (result_home - expected_home)
            elo[a] += K * multiplier * (result_away - (1 - expected_home))

            elo_history[h].append(elo[h])
            elo_history[a].append(elo[a])

            elo_trend[h] = TREND_ALPHA * (elo[h] - h_elo) + (1 - TREND_ALPHA) * elo_trend[h]
            elo_trend[a] = TREND_ALPHA * (elo[a] - a_elo) + (1 - TREND_ALPHA) * elo_trend[a]

    games["home_elo"] = home_elo
    games["away_elo"] = away_elo
    games["elo_diff"] = elo_diff
    games["home_elo_trend_last5"] = home_trend
    games["away_elo_trend_last5"] = away_trend

    return games
