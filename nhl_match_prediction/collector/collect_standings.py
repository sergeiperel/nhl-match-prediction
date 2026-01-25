import json
import time
from datetime import date, timedelta
from pathlib import Path

import requests

BASE_URL = "https://api-web.nhle.com/v1"
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "raw"

ENDPOINTS = {
    "standings": lambda d: f"/standings/{d}",
}

HEADERS = {"User-Agent": "nhl-data-collector/1.0"}


def fetch(endpoint: str, retries: int = 2, timeout: int = 10) -> dict:
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
            time.sleep(0.4 * attempt)
    return {}


def save_json(folder: str, name: str, data: dict) -> None:
    path = DATA_DIR / folder
    path.mkdir(parents=True, exist_ok=True)
    with (path / f"{name}.json").open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def collect_standings(start_date: date, end_date: date):
    total = 0

    for d in daterange(start_date, end_date):
        d_str = d.isoformat()
        print(f"Standings for {d_str}")

        try:
            standings = fetch(ENDPOINTS["standings"](d_str))
            save_json("standings", d_str, standings)
            total += 1
        except Exception as e:
            print(f"Failed for {d_str}: {e}")

        time.sleep(0.2)

    print(f"Collected standings for {total} days")


if __name__ == "__main__":
    collect_standings(
        start_date=date(2010, 1, 1),
        end_date=date(2025, 12, 31),
    )
