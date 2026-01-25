import json
import time
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
    "standings": lambda d: f"/standings/{d}",
}

HEADERS = {"User-Agent": "nhl-data-collector/1.0"}
SECONDS_IN_MINUTE = 60


def fetch(endpoint: str, retries: int = 3, timeout: int = 10) -> dict:
    url = BASE_URL + endpoint

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.json()

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ):
            if attempt == retries:
                raise

            sleep_time = 0.3 * attempt
            time.sleep(sleep_time)

    return {}


def save_json(folder: str, name: str, data: dict) -> None:
    path = DATA_DIR / folder
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{name}.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def week_starts(start: date, end: date):
    current = start - timedelta(days=start.weekday())
    while current <= end:
        yield current
        current += timedelta(days=7)


def collect_season(start_date: date, end_date: date):
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
                game_id = game["id"]

                if game_id in seen_games:
                    continue

                seen_games.add(game_id)
                week_new += 1

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

                time.sleep(0.15)

        week_time = time.perf_counter() - week_ts

        time_str = (
            f"{week_time:.2f}s"
            if week_time < SECONDS_IN_MINUTE
            else f"{week_time / SECONDS_IN_MINUTE:.2f}m"
        )

        print(f"Week summary: {week_new}/{week_total} games collected in {time_str}")

        time.sleep(0.2)


if __name__ == "__main__":
    print(time.ctime(time.time()))
    collect_season(start_date=date(2010, 1, 1), end_date=date(2014, 12, 31))
    print(time.ctime(time.time()))
