import json
import time
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import requests

BASE_URL = "https://api-web.nhle.com/v1"
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "raw"

ENDPOINTS = {
    "schedule": lambda d: f"/schedule/{d}",
    "landing": lambda gid: f"/gamecenter/{gid}/landing",
    "boxscore": lambda gid: f"/gamecenter/{gid}/boxscore",
    "playbyplay": lambda gid: f"/gamecenter/{gid}/play-by-play",
    "roster": lambda team, season: f"/roster/{team}/{season}",
}

HEADERS = {"User-Agent": "nhl-data-collector/1.0"}

RETRY_BACKOFF_SECONDS = 0.3
SECONDS_IN_MINUTE = 60
REQUEST_DELAY_SECONDS = 0.15
WEEK_DELAY_SECONDS = 0.15


def file_exists(folder: str, name: str) -> bool:
    return (DATA_DIR / folder / f"{name}.json").exists()


def fetch(endpoint: str, retries: int = 3, timeout: int = 10) -> dict:
    url = BASE_URL + endpoint

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == retries:
                raise
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
    return {}


def save_json(folder: str, name: str, data: dict) -> None:
    """Сохраняем JSON"""
    path = DATA_DIR / folder
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{name}.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def week_starts(start: date, end: date) -> Iterator[date]:
    """Генератор дат понедельников в диапазоне [start, end]"""
    current = start - timedelta(days=start.weekday())
    while current <= end:
        yield current
        current += timedelta(days=7)


def process_rosters(landing: dict) -> None:
    home_team = landing.get("homeTeam", {}).get("abbrev")
    away_team = landing.get("awayTeam", {}).get("abbrev")
    season = landing.get("season")

    for team in (home_team, away_team):
        if team and season:
            roster_key = f"{team}_{season}"
            if not file_exists("rosters", roster_key):
                try:
                    roster = fetch(ENDPOINTS["roster"](team, season))
                    save_json("rosters", roster_key, roster)
                except Exception as e:
                    print(f"roster failed {team}:", e)


def process_game(game: dict, seen_games: set[int]) -> bool:
    game_id = game.get("id")
    if game_id is None:
        return False

    if game_id in seen_games:
        return False

    seen_games.add(game_id)
    landing = None

    try:
        landing = fetch(ENDPOINTS["landing"](game_id))
        save_json("games", str(game_id), landing)
    except Exception as e:
        print(f"landing failed {game_id}:", e)

    try:
        boxscore = fetch(ENDPOINTS["boxscore"](game_id))
        save_json("boxscore", str(game_id), boxscore)
    except Exception as e:
        print(f"boxscore failed {game_id}:", e)

    try:
        if not file_exists("playbyplay", str(game_id)):
            pbp = fetch(ENDPOINTS["playbyplay"](game_id))
            save_json("playbyplay", str(game_id), pbp)
    except Exception as e:
        print(f"playbyplay failed {game_id}:", e)

    if landing:
        process_rosters(landing)

    time.sleep(REQUEST_DELAY_SECONDS)
    return True


def collect_season(start_date: date, end_date: date) -> None:
    """Собираем данные по всем матчам в указанном диапазоне"""
    seen_games: set[int] = set()

    for week_start in week_starts(start_date, end_date):
        week_str = week_start.isoformat()
        print(f"Week starting {week_str}")

        week_ts = time.perf_counter()

        try:
            schedule = fetch(ENDPOINTS["schedule"](week_str))
            save_json("schedule", week_str, schedule)
        except Exception as e:
            print("schedule failed:", e)
            continue

        week_total = 0
        week_new = 0

        for day in schedule.get("gameWeek", []):
            for game in day.get("games", []):
                week_total += 1
                if process_game(game, seen_games):
                    week_new += 1

        week_time = time.perf_counter() - week_ts

        if week_time < SECONDS_IN_MINUTE:
            time_str = f"{week_time:.2f}s"
        else:
            time_str = f"{week_time / SECONDS_IN_MINUTE:.2f}m"

        print(f"Week summary: {week_new}/{week_total} games collected in {time_str}")

        time.sleep(WEEK_DELAY_SECONDS)


if __name__ == "__main__":
    print(time.ctime())
    collect_season(start_date=date(2026, 1, 1), end_date=date(2026, 1, 31))
    print(time.ctime())
