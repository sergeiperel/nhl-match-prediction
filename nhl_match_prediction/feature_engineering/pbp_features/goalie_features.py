def extract_goalie_features(pbp_json: dict) -> dict:
    events = pbp_json.get("plays", [])
    home_team_id = pbp_json.get("homeTeam", {}).get("id")
    away_team_id = pbp_json.get("awayTeam", {}).get("id")

    stats = {
        "home_shots_for_goalie": 0,
        "away_shots_for_goalie": 0,
        "home_goals_against": 0,
        "away_goals_against": 0,
    }

    for event in events:
        event_type = event.get("typeDescKey")
        details = event.get("details", {})
        team_id = details.get("eventOwnerTeamId")
        if not team_id:
            continue

        # определяем сторону команды, которая атакует
        if team_id == home_team_id:
            attacking_side = "away"
        elif team_id == away_team_id:
            attacking_side = "home"
        else:
            continue

        # броски
        if event_type == "shot-on-goal":
            stats[f"{attacking_side}_shots_for_goalie"] += 1
        # гол
        elif event_type == "goal":
            stats[f"{attacking_side}_goals_against"] += 1

    # считаем сейвы и процент отражений
    stats["home_saves"] = stats["home_shots_for_goalie"] - stats["home_goals_against"]
    stats["away_saves"] = stats["away_shots_for_goalie"] - stats["away_goals_against"]

    stats["home_save_pct"] = (
        stats["home_saves"] / stats["home_shots_for_goalie"]
        if stats["home_shots_for_goalie"] > 0
        else 0
    )
    stats["away_save_pct"] = (
        stats["away_saves"] / stats["away_shots_for_goalie"]
        if stats["away_shots_for_goalie"] > 0
        else 0
    )

    return stats
