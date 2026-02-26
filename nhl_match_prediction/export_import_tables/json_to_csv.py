import csv
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
OUT_DIR = BASE_DIR / "data" / "processed"

OUT_DIR.mkdir(parents=True, exist_ok=True)


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

    home = data["homeTeam"]
    away = data["awayTeam"]

    home_score = home.get("score")
    away_score = away.get("score")

    # пропускаем несыгранные матчи
    if home_score is None or away_score is None:
        continue

    home_sog = home.get("sog")
    away_sog = away.get("sog")

    venue = data.get("venue", {}).get("default")
    venue_location = data.get("venueLocation", {}).get("default")

    summary = data.get("summary", {})
    period_type = data.get("periodDescriptor", {}).get("periodType")

    # -----------------------
    # penalties из summary
    # -----------------------

    home_penalties = 0
    away_penalties = 0
    home_pim = 0
    away_pim = 0

    for period in summary.get("penalties", []):
        for pen in period.get("penalties", []):
            team = pen.get("teamAbbrev", {}).get("default")
            duration = pen.get("duration", 0)

            if team == home["abbrev"]:
                home_penalties += 1
                home_pim += duration
            elif team == away["abbrev"]:
                away_penalties += 1
                away_pim += duration

    # -----------------------
    # вычисляем производные фичи
    # -----------------------

    goal_diff = home_score - away_score
    sog_diff = home_sog - away_sog if home_sog is not None and away_sog is not None else None

    total_goals = home_score + away_score

    games_rows.append(
        {
            "game_id": game_id,
            "date": date,
            "season": season,
            "game_type": data.get("gameType"),
            "venue": venue,
            "venue_location": venue_location,
            "neutral_site": 1 if data.get("neutralSite") else 0,
            "home_team_id": home["id"],
            "home_team_abbr": home["abbrev"],
            "away_team_id": away["id"],
            "away_team_abbr": away["abbrev"],
            "home_score": home_score,
            "away_score": away_score,
            "total_goals": total_goals,
            "home_sog": home_sog,
            "away_sog": away_sog,
            "goal_diff": goal_diff,
            "sog_diff": sog_diff,
            "home_win": 1 if home_score > away_score else 0,
            "one_goal_game": 1 if abs(goal_diff) == 1 else 0,
            "home_penalties": home_penalties,
            "away_penalties": away_penalties,
            "home_pim_summary": home_pim,
            "away_pim_summary": away_pim,
            "penalty_diff": home_penalties - away_penalties,
            "pim_diff": home_pim - away_pim,
            "is_overtime": 1 if period_type == "OT" else 0,
            "is_shootout": 1 if period_type == "SO" else 0,
        }
    )


# -----------------------
# запись в CSV
# -----------------------

if games_rows:
    with Path.open(OUT_DIR / "games.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=games_rows[0].keys())
        writer.writeheader()
        writer.writerows(games_rows)

    print("games.csv written")
else:
    print("No finished games found.")


# -----------------------
# 2. team_game_stats.csv
# -----------------------


def toi_to_minutes(toi_str):
    if not toi_str:
        return 0
    minutes, seconds = map(int, toi_str.split(":"))
    return minutes + seconds / 60


team_rows = []

for path in (RAW_DIR / "boxscore").glob("*.json"):
    data = read_json(path)
    game_id = data.get("id")

    if "playerByGameStats" not in data:
        print(f"boxscore {game_id}: no playerByGameStats, skipping")
        continue

    for side in ["homeTeam", "awayTeam"]:
        if side not in data["playerByGameStats"]:
            continue

        team_meta = data[side]
        team_stats = data["playerByGameStats"][side]

        skaters = team_stats.get("forwards", []) + team_stats.get("defense", [])

        faceoff_values = [
            p.get("faceoffWinningPctg") for p in skaters if p.get("faceoffWinningPctg") is not None
        ]

        team_rows.append(
            {
                "game_id": game_id,
                "team_id": team_meta["id"],
                "team_abbr": team_meta["abbrev"],
                "is_home": side == "homeTeam",
                "goals": team_meta.get("score"),
                "shots": team_meta.get("sog"),
                "hits": sum(p.get("hits", 0) for p in skaters),
                "blocked_shots": sum(p.get("blockedShots", 0) for p in skaters),
                "pim": sum(p.get("pim", 0) for p in skaters),
                "pp_goals": sum(p.get("powerPlayGoals", 0) for p in skaters),
                "shots_from_players": sum(p.get("sog", 0) for p in skaters),
                "faceoff_pct": sum(faceoff_values) / len(faceoff_values)
                if faceoff_values
                else None,
                "giveaways": sum(p.get("giveaways", 0) for p in skaters),
                "takeaways": sum(p.get("takeaways", 0) for p in skaters),
                "plus_minus": sum(p.get("plusMinus", 0) for p in skaters),
                "total_toi": sum(toi_to_minutes(p.get("toi")) for p in skaters),
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


def parse_sa(s):
    if not s:
        return 0, 0
    saves, shots = map(int, s.split("/"))
    return saves, shots


goalie_rows = []

for path in (RAW_DIR / "boxscore").glob("*.json"):
    data = read_json(path)
    game_id = data["id"]

    if "playerByGameStats" not in data:
        print(f"boxscore {game_id}: no playerByGameStats, skipping")
        continue

    for side in ["homeTeam", "awayTeam"]:
        team = data[side]
        team_id = team["id"]

        goalies = data["playerByGameStats"][side].get("goalies", [])

        for goalie in goalies:
            shots = goalie.get("shotsAgainst", 0)
            saves = goalie.get("saves", 0)

            ev_saves, ev_shots = parse_sa(goalie.get("evenStrengthShotsAgainst"))
            pp_saves, pp_shots = parse_sa(goalie.get("powerPlayShotsAgainst"))

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
                    "goals_against": goalie.get("goalsAgainst"),
                    "decision": goalie.get("decision"),
                    "ev_ga": goalie.get("evenStrengthGoalsAgainst"),
                    "pp_ga": goalie.get("powerPlayGoalsAgainst"),
                    "sh_ga": goalie.get("shorthandedGoalsAgainst"),
                    "ev_shots_against": ev_shots,
                    "pp_shots_against": pp_shots,
                    "toi_minutes": toi_to_minutes(goalie.get("toi")),
                    "played_full_game": 1 if goalie.get("toi") == "60:00" else 0,
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

    for team in data.get("standings", []):
        standings_rows.append(
            {
                # date & season
                "date": team.get("date", date),
                "season_id": team.get("seasonId"),
                "team_abbrev": team.get("teamAbbrev", {}).get("default"),
                "team_name": team.get("teamName", {}).get("default"),
                "team_logo": team.get("teamLogo"),
                "place_name": team.get("placeName", {}).get("default"),
                # conference / division
                "conference": team.get("conferenceAbbrev"),
                "division": team.get("divisionAbbrev"),
                # total season stats
                "games_played": team.get("gamesPlayed"),
                "wins": team.get("wins"),
                "losses": team.get("losses"),
                "ot_losses": team.get("otLosses"),
                "ties": team.get("ties"),
                "points": team.get("points"),
                "point_pctg": team.get("pointPctg"),
                "win_pctg": team.get("winPctg"),
                "goal_diff": team.get("goalDifferential"),
                "goals_for": team.get("goalFor"),
                "goals_against": team.get("goalAgainst"),
                # home stats
                "home_games_played": team.get("homeGamesPlayed"),
                "home_wins": team.get("homeWins"),
                "home_losses": team.get("homeLosses"),
                "home_ot_losses": team.get("homeOtLosses"),
                "home_points": team.get("homePoints"),
                "home_goals_for": team.get("homeGoalsFor"),
                "home_goals_against": team.get("homeGoalsAgainst"),
                "home_goal_diff": team.get("homeGoalDifferential"),
                # road stats
                "road_games_played": team.get("roadGamesPlayed"),
                "road_wins": team.get("roadWins"),
                "road_losses": team.get("roadLosses"),
                "road_ot_losses": team.get("roadOtLosses"),
                "road_points": team.get("roadPoints"),
                "road_goals_for": team.get("roadGoalsFor"),
                "road_goals_against": team.get("roadGoalsAgainst"),
                "road_goal_diff": team.get("roadGoalDifferential"),
                # last 10 games
                "l10_games_played": team.get("l10GamesPlayed"),
                "l10_wins": team.get("l10Wins"),
                "l10_losses": team.get("l10Losses"),
                "l10_ot_losses": team.get("l10OtLosses"),
                "l10_points": team.get("l10Points"),
                "l10_goals_for": team.get("l10GoalsFor"),
                "l10_goals_against": team.get("l10GoalsAgainst"),
                "l10_goal_diff": team.get("l10GoalDifferential"),
                # streak
                "streak_code": team.get("streakCode"),
                "streak_count": team.get("streakCount"),
                # regulation / OT / SO
                "regulation_wins": team.get("regulationWins"),
                "regulation_plus_ot_wins": team.get("regulationPlusOtWins"),
                "regulation_win_pctg": team.get("regulationWinPctg"),
                "regulation_plus_ot_win_pctg": team.get("regulationPlusOtWinPctg"),
                "shootout_wins": team.get("shootoutWins"),
                "shootout_losses": team.get("shootoutLosses"),
            }
        )


with Path.open(OUT_DIR / "standings_daily.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=standings_rows[0].keys())
    writer.writeheader()
    writer.writerows(standings_rows)

print("standings_daily.csv written")


# -----------------------
# 7. roster_snapshot.csv
# -----------------------

roster_rows = []

for path in (RAW_DIR / "rosters").glob("*.json"):
    data = read_json(path)

    filename = path.stem
    team, season = filename.split("_")

    players = data.get("forwards", []) + data.get("defensemen", []) + data.get("goalies", [])

    for player in players:
        height_cm = player.get("heightInCentimeters")
        weight_kg = player.get("weightInKilograms")

        bmi = weight_kg / ((height_cm / 100) ** 2) if height_cm and weight_kg else None

        position = player.get("positionCode")

        if position in ["L", "R", "C"]:
            position_group = "F"
        elif position == "D":
            position_group = "D"
        else:
            position_group = "G"

        roster_rows.append(
            {
                "team_abbrev": team,
                "season": season,
                "player_id": player.get("id"),
                "position": position,
                "position_group": position_group,
                "shoots_catches": player.get("shootsCatches"),
                "height_cm": height_cm,
                "weight_kg": weight_kg,
                "bmi": bmi,
                "birth_date": player.get("birthDate"),
                "birth_country": player.get("birthCountry"),
            }
        )


if roster_rows:
    with Path.open(OUT_DIR / "roster_snapshot.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=roster_rows[0].keys())
        writer.writeheader()
        writer.writerows(roster_rows)

    print("roster_snapshot.csv written")
