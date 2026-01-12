import json
import time
from datetime import date, timedelta
from pathlib import Path

import requests

BASE_URL = "https://api-web.nhle.com/v1"
DATA_DIR = Path("data")

ENDPOINTS = {
    "schedule": lambda d: f"/schedule/{d}",
    "landing": lambda gid: f"/gamecenter/{gid}/landing",
    "boxscore": lambda gid: f"/gamecenter/{gid}/boxscore",
    "standings": lambda d: f"/standings/{d}",
}

HEADERS = {"User-Agent": "nhl-data-collector/1.0"}


def fetch(endpoint: str):
    url = BASE_URL + endpoint
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()


def save_json(folder: str, name: str, data: dict) -> None:
    path = DATA_DIR / folder
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{name}.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def daterange(start: date, end: date):
    for n in range((end - start).days + 1):
        yield start + timedelta(days=n)


def collect_season(start_date: date, end_date: date):
    for d in daterange(start_date, end_date):
        d_str = d.isoformat()
        print(f"{d_str}")

        try:
            schedule = fetch(ENDPOINTS["schedule"](d_str))
            save_json("schedule", d_str, schedule)
        except Exception:
            continue

        games = []
        for day in schedule.get("gameWeek", []):
            games.extend(day.get("games", []))

        print("Games found:", len(games))

        for game in games:
            game_id = game["id"]

            try:
                landing = fetch(ENDPOINTS["landing"](game_id))
                save_json("games", str(game_id), landing)
            except Exception as e:
                print(f"landing failed for game {game_id}: {e}")

            try:
                boxscore = fetch(ENDPOINTS["boxscore"](game_id))
                save_json("boxscore", str(game_id), boxscore)
            except Exception as e:
                print(f"boxscore failed for game {game_id}: {e}")

            time.sleep(0.2)

        try:
            standings = fetch(ENDPOINTS["standings"](d_str))
            save_json("standings", d_str, standings)
        except Exception:
            pass

        time.sleep(0.5)


if __name__ == "__main__":
    # пример: регулярный сезон 2023-2024
    collect_season(start_date=date(2024, 1, 1), end_date=date(2024, 2, 1))
