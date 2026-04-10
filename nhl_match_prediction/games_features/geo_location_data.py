import math
import re
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = BASE_DIR / "data" / "processed" / "arenas_data.csv"

arenas_df = pd.read_csv(DATA_PATH)

team_coords = {}
team_lons = {}


def dms_to_dd(coord_str: str):
    if pd.isna(coord_str):
        raise ValueError("Координаты отсутствуют")

    coord_str = coord_str.replace("\xa0", " ").strip()

    decimal_match = re.search(r"([0-9.]+)°\s*([NS])\s*([0-9.]+)°\s*([EW])", coord_str)

    if decimal_match:
        lat, lat_dir, lon, lon_dir = decimal_match.groups()
        lat = float(lat)
        lon = float(lon)

        if lat_dir == "S":
            lat = -lat
        if lon_dir == "W":
            lon = -lon

        return lat, lon

    dms_match = re.search(
        r"(\d+)°(\d+)'([\d.]+)\"\s*([NS])\s*(\d+)°(\d+)'([\d.]+)\"\s*([EW])",
        coord_str,
    )

    if dms_match:
        lat_d, lat_m, lat_s, lat_dir, lon_d, lon_m, lon_s, lon_dir = dms_match.groups()

        lat = int(lat_d) + int(lat_m) / 60 + float(lat_s) / 3600
        lon = int(lon_d) + int(lon_m) / 60 + float(lon_s) / 3600

        if lat_dir == "S":
            lat = -lat
        if lon_dir == "W":
            lon = -lon

        return lat, lon

    raise ValueError(f"Не удалось распарсить координаты: {coord_str}")  # noqa: RUF001


for _, row in arenas_df.iterrows():
    try:
        lat, lon = dms_to_dd(row["coordinates"])
        abbr = row["team_abbr"]
        team_coords[abbr] = (lat, lon)
        team_lons[abbr] = lon

    except Exception as e:
        print(f"⚠️ Пропущена команда {row['Team']}: {e}")


def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # noqa: N806
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def travel_distance(home_team: str, away_team: str) -> float:
    if home_team not in team_coords or away_team not in team_coords:
        return np.nan

    home_lat, home_lon = team_coords[home_team]
    away_lat, away_lon = team_coords[away_team]

    return round(haversine(home_lat, home_lon, away_lat, away_lon), 1)


def timezone_change(home_team: str, away_team: str) -> int:
    """
    Приближённая разница часовых поясов по долготе
    """
    home_lon = team_lons[home_team]
    away_lon = team_lons[away_team]

    return round(abs(home_lon - away_lon) / 15)
