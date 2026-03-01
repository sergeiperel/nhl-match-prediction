# nhl_match_prediction/features/special_teams.py

from collections import defaultdict

# список всех типов бросков в NHL
SHOT_TYPES = [
    "deflected",
    "tip-in",
    "snap",
    "wrist",
    "backhand",
    "slap",
    "wraparound",
    "poke",
    "bat",
]


def extract_special_teams_features(pbp_json: dict) -> dict:
    """
    Извлекает количество голов для каждой команды:
    - по силовой ситуации (pp, sh, even)
    - по типу броска (wrist, slap и т.д.)
    """
    events = pbp_json.get("plays", [])
    stats = defaultdict(int)

    home_team_id = pbp_json.get("homeTeam", {}).get("id")
    away_team_id = pbp_json.get("awayTeam", {}).get("id")

    for event in events:
        if event.get("typeDescKey") != "goal":
            continue

        details = event.get("details", {})
        team_id = details.get("eventOwnerTeamId")
        shot_type = details.get("shotType", "unknown")
        situation_code = event.get("situationCode", "")

        if not team_id:
            continue

        # определяем сторону
        if team_id == home_team_id:
            side = "home"
        elif team_id == away_team_id:
            side = "away"
        else:
            continue

        # определяем силу гола по situationCode
        # пример: "0651" — power play для команды, "1551" — обычный гол
        # можно расширить правила по необходимости
        if situation_code.startswith("0"):
            strength = "pp"
        elif situation_code.startswith("1") and "sh" in situation_code:
            strength = "sh"
        else:
            strength = "even"

        # считаем гол по силе
        stats[f"{side}_{strength}_goals"] += 1

        # считаем гол по типу броска
        if shot_type in SHOT_TYPES:
            stats[f"{side}_{shot_type}_goals"] += 1
        else:
            stats[f"{side}_other_goals"] += 1

    return dict(stats)
