import csv
import json
from pathlib import Path

RAW_DIR = Path("data")
OUT_DIR = Path("processed")
OUT_DIR.mkdir(exist_ok=True)

# -----------------------
# helpers
# -----------------------


def read_json(path: Path):
    with Path.open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -----------------------
# 1. games.csv
# -----------------------

games_rows = []

for path in (RAW_DIR / "games").glob("*.json"):
    data = read_json(path)

    game_id = data["id"]
    date = data["gameDate"]
    season = data["season"]
    venue = data.get("venue", {}).get("default")

    home = data["homeTeam"]
    away = data["awayTeam"]

    games_rows.append(
        {
            "game_id": game_id,
            "date": date,
            "season": season,
            "home_team_id": home["id"],
            "away_team_id": away["id"],
            "home_score": home.get("score"),
            "away_score": away.get("score"),
            "venue": venue,
            "game_type": data.get("gameType"),
        }
    )


with Path.open(OUT_DIR / "games.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=games_rows[0].keys())
    writer.writeheader()
    writer.writerows(games_rows)

print("games.csv written")


# -----------------------
# 2. team_game_stats.csv
# -----------------------

team_rows = []

for path in (RAW_DIR / "boxscore").glob("*.json"):
    data = read_json(path)
    game_id = data["id"]

    for side in ["homeTeam", "awayTeam"]:
        team_meta = data[side]
        team_stats = data["playerByGameStats"][side]

        skaters = team_stats.get("forwards", []) + team_stats.get("defense", [])

        team_rows.append(
            {
                "game_id": game_id,
                "team_id": team_meta["id"],
                "is_home": side == "homeTeam",
                "goals": team_meta.get("score"),
                "shots": team_meta.get("sog"),
                "hits": sum(p.get("hits", 0) for p in skaters),
                "blocked_shots": sum(p.get("blockedShots", 0) for p in skaters),
                "pim": sum(p.get("pim", 0) for p in skaters),
            }
        )

with Path.open(OUT_DIR / "team_game_stats.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=team_rows[0].keys())
    writer.writeheader()
    writer.writerows(team_rows)

print("team_game_stats.csv written")


# -----------------------
# 3. goalie_game_stats.csv
# -----------------------

goalie_rows = []

for path in (RAW_DIR / "boxscore").glob("*.json"):
    data = read_json(path)
    game_id = data["id"]

    for side in ["homeTeam", "awayTeam"]:
        team = data[side]
        team_id = team["id"]

        goalies = data["playerByGameStats"][side].get("goalies", [])

        for goalie in goalies:
            shots = goalie.get("shotsAgainst", 0)
            saves = goalie.get("saves", 0)

            goalie_rows.append(
                {
                    "game_id": game_id,
                    "team_id": team_id,
                    "goalie_id": goalie["playerId"],
                    "starter": goalie.get("starter"),
                    "shots_against": shots,
                    "saves": saves,
                    "save_pct": saves / shots if shots else None,
                    "toi": goalie.get("toi"),
                }
            )

with Path.open(OUT_DIR / "goalie_game_stats.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=goalie_rows[0].keys())
    writer.writeheader()
    writer.writerows(goalie_rows)

print("goalie_game_stats.csv written")


# -----------------------
# 4. standings_daily.csv
# -----------------------

standings_rows = []

for path in (RAW_DIR / "standings").glob("*.json"):
    date = path.stem
    data = read_json(path)

    for team in data["standings"]:
        standings_rows.append(
            {
                "date": team.get("date", date),
                "season_id": team.get("seasonId"),
                # КЛЮЧ КОМАНДЫ
                "team_abbrev": team["teamAbbrev"]["default"],
                "team_name": team["teamName"]["default"],
                # conference / division
                "conference": team.get("conferenceAbbrev"),
                "division": team.get("divisionAbbrev"),
                # standings
                "games_played": team.get("gamesPlayed"),
                "points": team.get("points"),
                "wins": team.get("wins"),
                "losses": team.get("losses"),
                "ot_losses": team.get("otLosses"),
                "ties": team.get("ties"),
                # scoring
                "goals_for": team.get("goalFor"),
                "goals_against": team.get("goalAgainst"),
                "goal_diff": team.get("goalDifferential"),
                # percentages
                "point_pctg": team.get("pointPctg"),
                "win_pctg": team.get("winPctg"),
                # ranks
                "league_rank": team.get("leagueSequence"),
                "conference_rank": team.get("conferenceSequence"),
                "division_rank": team.get("divisionSequence"),
                # streak
                "streak_code": team.get("streakCode"),
                "streak_count": team.get("streakCount"),
            }
        )


with Path.open(OUT_DIR / "standings_daily.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=standings_rows[0].keys())
    writer.writeheader()
    writer.writerows(standings_rows)

print("standings_daily.csv written")
