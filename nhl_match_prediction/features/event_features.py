# nhl_match_prediction/features/event_features.py

EVENT_MAP = {
    # goals
    "goal": "goals",
    # shots
    "shot-on-goal": "shots_on_goal",
    "missed-shot": "missed_shots",
    "blocked-shot": "blocked_shots",
    # physical
    "hit": "hits",
    # possession
    "giveaway": "giveaways",
    "takeaway": "takeaways",
    # faceoffs
    "faceoff": "faceoffs",
    # penalties
    "penalty": "penalties",
}


def _parse_penalty_minutes(details: dict) -> int:
    duration = details.get("duration", 0)

    if isinstance(duration, str) and ":" in duration:
        return int(duration.split(":")[0])

    return int(duration)


def _init_stats() -> dict:
    base_stats = [
        "goals",
        "shots_on_goal",
        "missed_shots",
        "blocked_shots",
        "hits",
        "giveaways",
        "takeaways",
        "faceoffs",
        "penalties",
        "penalty_minutes",
    ]

    stats = {}

    for side in ("home", "away"):
        for stat in base_stats:
            stats[f"{side}_{stat}"] = 0

    return stats


def extract_event_features(pbp_json: dict) -> dict:
    events = pbp_json.get("plays", [])

    home_team_id = pbp_json.get("homeTeam", {}).get("id")
    away_team_id = pbp_json.get("awayTeam", {}).get("id")

    stats = _init_stats()

    for event in events:
        event_type = event.get("typeDescKey")
        details = event.get("details", {})
        team_id = details.get("eventOwnerTeamId")

        if not team_id or not event_type:
            continue

        # определяем сторону
        if team_id == home_team_id:
            side = "home"
        elif team_id == away_team_id:
            side = "away"
        else:
            continue  # защита от мусорных данных

        if event_type in EVENT_MAP:
            stat_name = EVENT_MAP[event_type]
            stats[f"{side}_{stat_name}"] += 1

            if event_type == "penalty":
                minutes = _parse_penalty_minutes(details)
                stats[f"{side}_penalty_minutes"] += minutes

    for side in ("home", "away"):
        stats[f"{side}_shot_attempts"] = (
            stats[f"{side}_shots_on_goal"]
            + stats[f"{side}_missed_shots"]
            + stats[f"{side}_blocked_shots"]
        )

    return stats
