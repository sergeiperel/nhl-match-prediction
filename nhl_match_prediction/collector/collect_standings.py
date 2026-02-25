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
    "standings": lambda d: f"/standings/{d}",
}

HEADERS = {"User-Agent": "nhl-data-collector/1.0"}

RETRY_BACKOFF_SECONDS = 0.4
REQUEST_DELAY_SECONDS = 0.2


def fetch(endpoint: str, retries: int = 2, timeout: int = 10) -> dict:
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
    with (path / f"{name}.json").open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def daterange(start: date, end: date) -> Iterator[date]:
    """Генератор дат в диапазоне [start, end]"""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def collect_standings(start_date: date, end_date: date) -> None:
    """Собрать турнирные таблицы по всем дням в диапазоне"""
    total = 0

    for current_date in daterange(start_date, end_date):
        d_str = current_date.isoformat()
        print(f"Standings for {d_str}")

        try:
            standings = fetch(ENDPOINTS["standings"](d_str))
            save_json("standings", d_str, standings)
            total += 1
        except Exception as e:
            print(f"Failed for {d_str}: {e}")

        time.sleep(REQUEST_DELAY_SECONDS)

    print(f"Collected standings for {total} days")


if __name__ == "__main__":
    print(time.ctime())
    collect_standings(
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
    )
    print(time.ctime())
