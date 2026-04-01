EVENT_MAP = {
    "shot-on-goal": "shots_on_goal",
    "missed-shot": "missed_shots",
    "blocked-shot": "blocked_shots",
    "hit": "hits",
    "giveaway": "giveaways",
    "takeaway": "takeaways",
    "faceoff": "faceoffs",
    "penalty": "penalties",
}

CODE_LENGTH = 4


def _parse_penalty_minutes(details: dict) -> int:
    duration = details.get("duration", 0)

    if isinstance(duration, str) and ":" in duration:
        return int(duration.split(":")[0])

    return int(duration)


def _parse_situation(situation_code: str):
    if not situation_code or len(situation_code) != CODE_LENGTH:
        return None

    return {
        "away_goalie": int(situation_code[0]),
        "away_skaters": int(situation_code[1]),
        "home_skaters": int(situation_code[2]),
        "home_goalie": int(situation_code[3]),
    }


def _init_stats():
    base = [
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
        "pp_goals",
        "pp_opportunities",
        "pk_goals_against",
        "pk_opportunities",
    ]

    stats = {}
    for side in ["home", "away"]:
        for stat in base:
            stats[f"{side}_{stat}"] = 0

    return stats


def update_penalties(active_penalties):
    for side in ["home", "away"]:
        new_list = []
        for p in active_penalties[side]:
            p["remaining"] -= 1
            if p["remaining"] > 0:
                new_list.append(p)
        active_penalties[side] = new_list


def handle_penalty(event_type, details, side, stats, active_penalties):
    if event_type != "penalty":
        return

    minutes = _parse_penalty_minutes(details)
    stats[f"{side}_penalty_minutes"] += minutes

    if details.get("typeCode") != "MIN":
        return

    active_penalties[side].append({"remaining": minutes})


def get_pp_state(active_penalties):
    home_pp = len(active_penalties["home"]) < len(active_penalties["away"])
    away_pp = len(active_penalties["away"]) < len(active_penalties["home"])
    return home_pp, away_pp


def handle_pp_opportunity(event_type, side, opp, stats, pp_state):
    if event_type != "penalty":
        return

    home_pp, away_pp = pp_state

    if (side == "home" and home_pp) or (side == "away" and away_pp):
        return

    stats[f"{opp}_pp_opportunities"] += 1
    stats[f"{side}_pk_opportunities"] += 1


def handle_goal(event_type, side, stats, home_pp, away_pp):
    if event_type != "goal":
        return

    stats[f"{side}_goals"] += 1

    if side == "home" and home_pp:
        stats["home_pp_goals"] += 1
        stats["away_pk_goals_against"] += 1

    elif side == "away" and away_pp:
        stats["away_pp_goals"] += 1
        stats["home_pk_goals_against"] += 1


def extract_event_features(pbp_json: dict) -> dict:
    events = pbp_json.get("plays", [])

    home_id = pbp_json.get("homeTeam", {}).get("id")

    stats = _init_stats()

    active_penalties = {"home": [], "away": []}

    def sort_key(e):
        pd = e.get("periodDescriptor", {}).get("number", 0)
        time = e.get("timeInPeriod", "00:00")
        return (pd, time)

    events = sorted(events, key=sort_key)

    # --------------------------------------------------
    for event in events:
        event_type = event.get("typeDescKey")
        details = event.get("details", {})
        team_id = details.get("eventOwnerTeamId")

        if not team_id:
            continue

        side = "home" if team_id == home_id else "away"
        opp = "away" if side == "home" else "home"

        # basic stats
        if event_type in EVENT_MAP:
            stats[f"{side}_{EVENT_MAP[event_type]}"] += 1

        # penalties
        handle_penalty(event_type, details, side, stats, active_penalties)

        # tick penalties
        update_penalties(active_penalties)

        # PP state
        home_pp, away_pp = get_pp_state(active_penalties)

        # opportunities
        handle_pp_opportunity(event_type, side, opp, stats, (home_pp, away_pp))

        # goals
        handle_goal(event_type, side, stats, home_pp, away_pp)

    for side in ["home", "away"]:
        stats[f"{side}_shot_attempts"] = (
            stats[f"{side}_shots_on_goal"]
            + stats[f"{side}_missed_shots"]
            + stats[f"{side}_blocked_shots"]
        )

    return stats
