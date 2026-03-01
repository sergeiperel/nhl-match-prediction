import math

import pandas as pd

# GOAL_X = 89  # координата ворот NHL

HIGH_DANGER_DISTANCE = 25
SLOT_SHOT_ANGLE = 15


def get_goal_x(team_id, home_id, home_defending_side):
    if team_id == home_id:
        if home_defending_side == "left":
            return 89
        return -89
    if home_defending_side == "left":
        return -89
    return 89


def calculate_distance(x, y, goal_x):
    return math.sqrt((goal_x - x) ** 2 + y**2)


def calculate_angle(x, y, goal_x):
    return math.degrees(math.atan2(abs(y), goal_x - x))


def aggregate_team_shots(shots_df: pd.DataFrame):
    if shots_df.empty:
        return {
            "shots_on_goal": 0,
            "avg_shot_distance": 0,
            "median_shot_distance": 0,
            "std_shot_distance": 0,
            "min_shot_distance": 0,
            "avg_shot_angle": 0,
            "median_shot_angle": 0,
            "high_danger_shots": 0,
            "high_danger_ratio": 0,
            "slot_shots": 0,
            "slot_ratio": 0,
        }

    distances = shots_df["distance"]
    angles = shots_df["angle"]

    shots_on_goal = len(shots_df)
    high_danger = (distances < HIGH_DANGER_DISTANCE).sum()
    slot_shots = (angles < SLOT_SHOT_ANGLE).sum()

    return {
        "shots_on_goal": shots_on_goal,
        "avg_shot_distance": distances.mean(),
        "median_shot_distance": distances.median(),
        "std_shot_distance": distances.std(ddof=0),
        "min_shot_distance": distances.min(),
        "avg_shot_angle": angles.mean(),
        "median_shot_angle": angles.median(),
        "high_danger_shots": high_danger,
        "high_danger_ratio": high_danger / shots_on_goal,
        "slot_shots": slot_shots,
        "slot_ratio": slot_shots / shots_on_goal,
    }


def extract_spatial_features(pbp_json: dict) -> dict:
    home_id = pbp_json["homeTeam"]["id"]
    away_id = pbp_json["awayTeam"]["id"]

    home_defending_side = pbp_json.get("homeTeamDefendingSide")

    if home_defending_side not in ["left", "right"]:
        home_defending_side = 0

    events = pbp_json.get("plays", [])

    shots_data = []

    for event in events:
        if event.get("typeDescKey") not in ["shot-on-goal", "goal"]:
            continue

        details = event.get("details", {})
        x = details.get("xCoord")
        y = details.get("yCoord")
        team_id = details.get("eventOwnerTeamId")

        if x is None or y is None or team_id is None:
            continue

        goal_x = get_goal_x(team_id, home_id, home_defending_side)

        distance = calculate_distance(x, y, goal_x)
        angle = calculate_angle(x, y, goal_x)

        shots_data.append(
            {
                "team_id": team_id,
                "distance": distance,
                "angle": angle,
            }
        )

    if not shots_data:
        return {}

    df = pd.DataFrame(shots_data)

    home_df = df[df["team_id"] == home_id]
    away_df = df[df["team_id"] == away_id]

    home_stats = aggregate_team_shots(home_df)
    away_stats = aggregate_team_shots(away_df)

    result = {}

    for key, value in home_stats.items():
        result[f"home_{key}"] = value

    for key, value in away_stats.items():
        result[f"away_{key}"] = value

    return result
