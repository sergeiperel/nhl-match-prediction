import csv
import json
import logging
from pathlib import Path

import numpy as np

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw"
OUT_DIR = BASE_DIR / "data" / "processed"

OUT_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)


def read_json(path: Path):
    with Path.open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def toi_to_minutes(toi_str):
    if not toi_str:
        return 0
    minutes, seconds = map(int, toi_str.split(":"))
    return minutes + seconds / 60


def build_team_timezone():
    team_timezone = {}

    for path in (RAW_DIR / "games").glob("*.json"):
        data = read_json(path)

        home = data["homeTeam"]
        tz = data.get("venueUTCOffset")

        if tz and not data.get("neutralSite"):
            team_timezone[home["id"]] = tz

    return team_timezone


def parse_offset(offset_str):
    if not offset_str:
        return 0
    sign = -1 if offset_str.startswith("-") else 1
    hours = int(offset_str[1:3])
    return sign * hours


def extract_games():
    # -----------------------
    # 1. games.csv
    # -----------------------

    games_rows = []

    team_timezone = build_team_timezone()

    for path in (RAW_DIR / "games").glob("*.json"):
        data = read_json(path)

        game_id = data["id"]
        date = data["gameDate"]

        start_time = data["startTimeUTC"]

        season = data["season"]

        home = data["homeTeam"]
        away = data["awayTeam"]

        home_tz_str = data.get("venueUTCOffset")
        away_tz_str = team_timezone.get(away["id"], home_tz_str)

        home_tz = parse_offset(home_tz_str)
        away_tz = parse_offset(away_tz_str)

        timezone_change = abs(home_tz - away_tz)

        timezone_shift = home_tz - away_tz

        eastward_travel = 1 if timezone_shift > 0 else 0
        westward_travel = 1 if timezone_shift < 0 else 0

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
                "start_time": start_time,
                "timezone_change": timezone_change,
                "eastward_travel": eastward_travel,
                "westward_travel": westward_travel,
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
    return games_rows


def extract_team_stats():
    # -----------------------
    # 2. team_game_stats.csv
    # -----------------------

    team_rows = []

    for path in (RAW_DIR / "boxscore").glob("*.json"):
        data = read_json(path)
        game_id = data.get("id")

        if "playerByGameStats" not in data:
            logger.info(f"boxscore {game_id}: no playerByGameStats, skipping")
            continue

        for side in ["homeTeam", "awayTeam"]:
            if side not in data["playerByGameStats"]:
                continue

            team_meta = data[side]
            team_stats = data["playerByGameStats"][side]

            skaters = team_stats.get("forwards", []) + team_stats.get("defense", [])

            faceoff_values = [
                p.get("faceoffWinningPctg")
                for p in skaters
                if p.get("faceoffWinningPctg") is not None
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
    return team_rows


def extract_goalies():
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
            logger.info(f"boxscore {game_id}: no playerByGameStats, skipping")
            continue

        for side in ["homeTeam", "awayTeam"]:
            team = data[side]
            team_id = team["id"]

            goalies = data["playerByGameStats"][side].get("goalies", [])

            for goalie in goalies:
                shots = goalie.get("shotsAgainst", 0)
                saves = goalie.get("saves", 0)

                _ev_saves, ev_shots = parse_sa(goalie.get("evenStrengthShotsAgainst"))
                _pp_saves, pp_shots = parse_sa(goalie.get("powerPlayShotsAgainst"))
                _sh_saves, sh_shots = parse_sa(goalie.get("shorthandedShotsAgainst"))

                goalie_rows.append(
                    {
                        "game_id": game_id,
                        "team_id": team_id,
                        "goalie_id": goalie["playerId"],
                        "goalie_name": goalie.get("name", {}).get("default"),
                        "starter": goalie.get("starter"),
                        "shots_against": shots,
                        "saves": saves,
                        "save_pct": goalie.get("savePctg"),
                        "toi": goalie.get("toi"),
                        "goals_against": goalie.get("goalsAgainst"),
                        "decision": goalie.get("decision"),
                        "ev_ga": goalie.get("evenStrengthGoalsAgainst"),
                        "pp_ga": goalie.get("powerPlayGoalsAgainst"),
                        "sh_ga": goalie.get("shorthandedGoalsAgainst"),
                        "ev_shots_against": ev_shots,
                        "pp_shots_against": pp_shots,
                        "sh_shots_against": sh_shots,
                        "toi_minutes": toi_to_minutes(goalie.get("toi")),
                        "played_full_game": 1 if goalie.get("toi") == "60:00" else 0,
                    }
                )
    return goalie_rows


def extract_standings():
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
                    # ranking positions
                    "league_rank": team.get("leagueSequence"),
                    "conference_rank": team.get("conferenceSequence"),
                    "division_rank": team.get("divisionSequence"),
                    "wildcard_rank": team.get("wildcardSequence"),
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
                    # Home / Road win pct
                    "home_win_pctg": team.get("homeWins") / max(team.get("homeGamesPlayed", 1), 1),
                    "road_win_pctg": team.get("roadWins") / max(team.get("roadGamesPlayed", 1), 1),
                    # Goal rates
                    "goals_for_per_game": team.get("goalFor") / max(team.get("gamesPlayed", 1), 1),
                    "goals_against_per_game": team.get("goalAgainst")
                    / max(team.get("gamesPlayed", 1), 1),
                    # L10 win pct
                    "l10_win_pctg": team.get("l10Wins") / max(team.get("l10GamesPlayed", 1), 1),
                    # L10 goal rates
                    "l10_goals_for_per_game": team.get("l10GoalsFor")
                    / max(team.get("l10GamesPlayed", 1), 1),
                    "l10_goals_against_per_game": team.get("l10GoalsAgainst")
                    / max(team.get("l10GamesPlayed", 1), 1),
                    # wildCardIndicator
                    "is_wildcard_race": data.get("wildCardIndicator"),
                }
            )
    return standings_rows


def extract_rosters():
    # -----------------------
    # 5. roster_snapshot.csv
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

            first_name = player.get("firstName", {}).get("default")
            last_name = player.get("lastName", {}).get("default")
            birth_city = player.get("birthCity", {}).get("default")

            roster_rows.append(
                {
                    "team_abbrev": team,
                    "season": season,
                    "player_id": player.get("id"),
                    "headshot": player.get("headshot"),
                    "first_name": first_name,
                    "last_name": last_name,
                    "sweater_number": player.get("sweaterNumber"),
                    "position": position,
                    "position_group": position_group,
                    "shoots_catches": player.get("shootsCatches"),
                    "height_cm": height_cm,
                    "weight_kg": weight_kg,
                    "bmi": bmi,
                    "birth_date": player.get("birthDate"),
                    "birth_city": birth_city,
                    "birth_country": player.get("birthCountry"),
                }
            )
    return roster_rows


def extract_schedule():
    rows = []

    for path in (RAW_DIR / "schedule").glob("*.json"):
        data = read_json(path)

        for day in data.get("gameWeek", []):
            for game in day.get("games", []):
                rows.append(
                    {
                        "game_id": game["id"],
                        "season": game["season"],
                        "game_date": game["startTimeUTC"],
                        "game_state": game.get("gameState"),
                        "game_schedule_state": game.get("gameScheduleState"),
                        "home_team_id": game["homeTeam"]["id"],
                        "home_team_abbr": game["homeTeam"]["abbrev"],
                        "away_team_id": game["awayTeam"]["id"],
                        "away_team_abbr": game["awayTeam"]["abbrev"],
                        "home_score": game["homeTeam"].get("score"),
                        "away_score": game["awayTeam"].get("score"),
                        "venue": game.get("venue", {}).get("default"),
                        "neutral_site": game.get("neutralSite"),
                        "period_type": game.get("periodDescriptor", {}).get("periodType"),
                    }
                )

    return rows


def extract_player_stats():
    rows = []

    for path in (RAW_DIR / "boxscore").glob("*.json"):
        boxscore_json = read_json(path)

        if "playerByGameStats" not in boxscore_json:
            logger.info(f"{path.stem}: no playerByGameStats, skipping")
            continue

        for team_side in ["homeTeam", "awayTeam"]:
            if team_side not in boxscore_json["playerByGameStats"]:
                logger.info(f"{path.stem}: no {team_side} stats, skipping")
                continue

            team = boxscore_json[team_side]
            team_id = team["id"]
            season = boxscore_json.get("season")
            game_id = boxscore_json.get("id")
            game_state = boxscore_json.get("gameState")

            # --- Forwards ---
            for p in boxscore_json["playerByGameStats"][team_side]["forwards"]:
                rows.append(
                    {
                        "player_id": p["playerId"],
                        "name": p["name"]["default"],
                        "position": p["position"],
                        "team_id": team_id,
                        "season": season,
                        "game_id": game_id,
                        "gameState": game_state,
                        "total_points": p.get("points", np.nan),
                        "total_goals": p.get("goals", np.nan),
                        "total_assists": p.get("assists", np.nan),
                        "toi_minutes": toi_to_minutes(p.get("toi")),
                        "pim": p.get("pim", np.nan),
                        "hits": p.get("hits", np.nan),
                        "powerPlayGoals": p.get("powerPlayGoals", np.nan),
                        "sog": p.get("sog", np.nan),
                        "faceoffWinningPctg": p.get("faceoffWinningPctg", np.nan),
                        "blockedShots": p.get("blockedShots", np.nan),
                        "shifts": p.get("shifts", np.nan),
                        "giveaways": p.get("giveaways", np.nan),
                        "takeaways": p.get("takeaways", np.nan),
                        # Goalie fields
                        "starter": False,
                        "evenStrengthShotsAgainst": np.nan,
                        "powerPlayShotsAgainst": np.nan,
                        "shorthandedShotsAgainst": np.nan,
                        "saveShotsAgainst": np.nan,
                        "evenStrengthGoalsAgainst": np.nan,
                        "powerPlayGoalsAgainst": np.nan,
                        "shorthandedGoalsAgainst": np.nan,
                        "goalsAgainst": np.nan,
                        "shotsAgainst": np.nan,
                        "saves": np.nan,
                        "last_n_games_points": np.nan,  # заполнить позже на основе истории
                        "rank_in_team": np.nan,  # заполнить позже на основе истории
                    }
                )

            # --- Defense ---
            for p in boxscore_json["playerByGameStats"][team_side]["defense"]:
                rows.append(
                    {
                        "player_id": p["playerId"],
                        "name": p["name"]["default"],
                        "position": p["position"],
                        "team_id": team_id,
                        "season": season,
                        "game_id": game_id,
                        "gameState": game_state,
                        "total_points": p.get("points", np.nan),
                        "total_goals": p.get("goals", np.nan),
                        "total_assists": p.get("assists", np.nan),
                        "toi_minutes": toi_to_minutes(p.get("toi")),
                        "pim": p.get("pim", np.nan),
                        "hits": p.get("hits", np.nan),
                        "powerPlayGoals": p.get("powerPlayGoals", np.nan),
                        "sog": p.get("sog", np.nan),
                        "faceoffWinningPctg": p.get("faceoffWinningPctg", np.nan),
                        "blockedShots": p.get("blockedShots", np.nan),
                        "shifts": p.get("shifts", np.nan),
                        "giveaways": p.get("giveaways", np.nan),
                        "takeaways": p.get("takeaways", np.nan),
                        # Goalie fields
                        "starter": False,
                        "evenStrengthShotsAgainst": np.nan,
                        "powerPlayShotsAgainst": np.nan,
                        "shorthandedShotsAgainst": np.nan,
                        "saveShotsAgainst": np.nan,
                        "evenStrengthGoalsAgainst": np.nan,
                        "powerPlayGoalsAgainst": np.nan,
                        "shorthandedGoalsAgainst": np.nan,
                        "goalsAgainst": np.nan,
                        "shotsAgainst": np.nan,
                        "saves": np.nan,
                        "last_n_games_points": np.nan,  # заполнить позже на основе истории
                        "rank_in_team": np.nan,  # заполнить позже на основе истории
                    }
                )

            # --- Goalies ---
            for p in boxscore_json["playerByGameStats"][team_side]["goalies"]:
                rows.append(
                    {
                        "player_id": p["playerId"],
                        "name": p["name"]["default"],
                        "position": "G",
                        "team_id": team_id,
                        "season": season,
                        "game_id": game_id,
                        "gameState": game_state,
                        # Player fields
                        "total_points": np.nan,
                        "total_goals": np.nan,
                        "total_assists": np.nan,
                        "toi_minutes": toi_to_minutes(p.get("toi")),
                        "pim": p.get("pim", np.nan),
                        "hits": np.nan,
                        "powerPlayGoals": np.nan,
                        "sog": np.nan,
                        "faceoffWinningPctg": np.nan,
                        "blockedShots": np.nan,
                        "shifts": np.nan,
                        "giveaways": np.nan,
                        "takeaways": np.nan,
                        # Goalie fields
                        "starter": p.get("starter", False),
                        "evenStrengthShotsAgainst": p.get("evenStrengthShotsAgainst", np.nan),
                        "powerPlayShotsAgainst": p.get("powerPlayShotsAgainst", np.nan),
                        "shorthandedShotsAgainst": p.get("shorthandedShotsAgainst", np.nan),
                        "saveShotsAgainst": p.get("saveShotsAgainst", np.nan),
                        "evenStrengthGoalsAgainst": p.get("evenStrengthGoalsAgainst", np.nan),
                        "powerPlayGoalsAgainst": p.get("powerPlayGoalsAgainst", np.nan),
                        "shorthandedGoalsAgainst": p.get("shorthandedGoalsAgainst", np.nan),
                        "goalsAgainst": p.get("goalsAgainst", np.nan),
                        "shotsAgainst": p.get("shotsAgainst", np.nan),
                        "saves": p.get("saves", np.nan),
                        "last_n_games_points": np.nan,  # заполнить позже на основе истории
                        "rank_in_team": np.nan,  # заполнить позже на основе истории
                    }
                )

    return rows


def write_csv(rows, csv_name):
    # -----------------------
    # запись в CSV
    # -----------------------

    if rows:
        with Path.open(OUT_DIR / csv_name, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"{csv_name} written")
    else:
        logger.info("No finished games found")


def main():
    games_rows = extract_games()
    write_csv(games_rows, "games.csv")

    team_rows = extract_team_stats()
    write_csv(team_rows, "team_game_stats.csv")

    goalie_rows = extract_goalies()
    write_csv(goalie_rows, "goalie_game_stats.csv")

    standings_rows = extract_standings()
    write_csv(standings_rows, "standings_daily.csv")

    roster_rows = extract_rosters()
    write_csv(roster_rows, "roster_snapshot.csv")

    schedule_rows = extract_schedule()
    write_csv(schedule_rows, "schedule_games.csv")

    player_stats = extract_player_stats()
    write_csv(player_stats, "player_stats.csv")


if __name__ == "__main__":
    main()
