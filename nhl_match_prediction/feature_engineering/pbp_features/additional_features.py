import math

LAST_MINUTES_THRESHOLD = 5
FULL_STRENGTH_SKATERS = 5
CODE_LENGTH = 4
PERIODS = 3


def safe_inc(features, key, value=1):
    features[key] = features.get(key, 0) + value


def parse_situation(code: str):
    if not code or len(code) != CODE_LENGTH or not code.isdigit():
        return None

    away_goalie = int(code[0])
    away_skaters = int(code[1])
    home_skaters = int(code[2])
    home_goalie = int(code[3])

    return away_goalie, away_skaters, home_skaters, home_goalie


def compute_xg(distance: float, angle: float) -> float:
    angle_factor = math.exp(-0.02 * angle)
    xg = 0.3 * math.exp(-0.08 * distance) * angle_factor

    return max(xg, 0)


def get_goal_x(team, home_defending_side):
    if team == "home":
        return 89 if home_defending_side == "left" else -89
    return -89 if home_defending_side == "left" else 89


def compute_event_xg(details, team, context):
    if team is None:
        return 0, None

    x = details.get("xCoord")
    y = details.get("yCoord")

    if x is None or y is None:
        return 0, None

    goal_x = get_goal_x(team, context["home_defending_side"])

    distance = math.sqrt((x - goal_x) ** 2 + y**2)
    angle = abs(math.degrees(math.atan2(y, goal_x - x)))

    xg = compute_xg(distance, angle)

    return xg, (distance, angle)


def handle_last5(e, event_type, team, features, scores):
    time_remain = e.get("timeRemaining")

    period = e.get("periodDescriptor", {}).get("number")

    home_score, away_score = scores

    if period != PERIODS:
        return home_score, away_score

    if not team or time_remain is None:
        return home_score, away_score

    if event_type not in ["goal", "shot-on-goal"]:
        return home_score, away_score

    parts = time_remain.split(":")
    if len(parts) < 2:  # noqa PLR2004
        return home_score, away_score
    minutes = int(parts[0])

    if minutes > LAST_MINUTES_THRESHOLD:
        return home_score, away_score

    safe_inc(features, f"{team}_last5_shots")

    if event_type == "goal":
        safe_inc(features, f"{team}_last5_goals")

    return home_score, away_score


def handle_physical(event_type, team, features):
    if not team:
        return

    mapping = {
        "hit": "hits",
        "takeaway": "takeaways",
        "giveaway": "giveaways",
        "penalty": "penalties",
    }

    if event_type in mapping:
        safe_inc(features, f"{team}_{mapping[event_type]}")


def handle_score_state(features, home_score, away_score):
    if home_score > away_score:
        safe_inc(features, "home_events_leading")
    elif away_score > home_score:
        safe_inc(features, "away_events_leading")
    else:
        safe_inc(features, "events_tied")


def handle_period_stats(e, features):
    period_num = e.get("periodDescriptor", {}).get("number")
    period_type = e.get("periodDescriptor", {}).get("periodType")

    if not period_type:
        return None

    key = f"REG{period_num}" if period_type == "REG" and period_num else period_type

    safe_inc(features, f"events_{key}")
    return key


def determine_special_teams(home_skaters, away_skaters):
    if home_skaters > away_skaters:
        return "home", "away"
    if away_skaters > home_skaters:
        return "away", "home"
    return None, None


def handle_5v5(event_type, team, features):
    if event_type in ["shot-on-goal", "missed-shot", "blocked-shot", "goal"]:
        safe_inc(features, f"{team}_corsi_5v5")

    if event_type in ["shot-on-goal", "missed-shot", "goal"]:
        safe_inc(features, f"{team}_fenwick_5v5")


def handle_pp_sh(event_type, team, pp_team, sh_team, features):
    if team == pp_team:
        safe_inc(features, f"{team}_goals_PP" if event_type == "goal" else f"{team}_shots_PP")

    elif team == sh_team:
        safe_inc(features, f"{team}_goals_SH" if event_type == "goal" else f"{team}_shots_SH")


def handle_situations(e, event_type, team, features):
    # -------- спецбригады --------

    # Those 4 digit code are a representation of the current situation like so:
    # away goalie (1=in net, 0=pulled) - away skaters
    # home skaters - home goalie (1=in net, 0=pulled)
    # **Example: **
    # 1) 1541 Away PP (there's 5 away players
    # and 4 away players on the ice and both goalie on still in the net.)
    # 2) 0641 Away PP and Away goalie is pulled

    situation = e.get("situationCode")

    if not team or not situation:
        return

    parsed = parse_situation(situation)
    if not parsed:
        return

    away_goalie, away_skaters, home_skaters, home_goalie = parsed

    is_5v5 = (
        home_skaters == FULL_STRENGTH_SKATERS
        and away_skaters == FULL_STRENGTH_SKATERS
        and home_goalie == 1
        and away_goalie == 1
    )

    if is_5v5:
        handle_5v5(event_type, team, features)

    pp_team, sh_team = determine_special_teams(home_skaters, away_skaters)
    handle_pp_sh(event_type, team, pp_team, sh_team, features)

    if home_goalie == 0:
        safe_inc(features, "home_empty_net")
    if away_goalie == 0:
        safe_inc(features, "away_empty_net")


def handle_goals_shots(event_type, team, key, period_types, features):
    # -------- голы и броски по периодам --------
    if team and key in period_types:
        if event_type == "goal":
            safe_inc(features, f"{team}_goals_{key}")

        elif event_type == "shot-on-goal":
            safe_inc(features, f"{team}_shots_{key}")

        elif event_type == "missed-shot":
            safe_inc(features, f"{team}_missed_{key}")

        elif event_type == "blocked-shot" and key != "SO":
            safe_inc(features, f"{team}_blocked_{key}")


def handle_faceoffs(e, event_type, details, team, features):
    # -------- faceoffs --------
    if event_type == "faceoff" and team:
        zone = details.get("zoneCode")
        if zone in ["O", "D", "N"]:
            safe_inc(features, f"{team}_faceoff_{zone}")


def handle_positions(event_type, details, team, position_context, features):
    # -------- позиции игроков --------
    player_pos = position_context.get("player_pos", {})
    pos_list = position_context.get("pos_list", [])

    if event_type in ["goal", "shot-on-goal"] and team:
        if event_type == "goal":
            player_id = details.get("scoringPlayerId")
        elif event_type == "shot-on-goal":
            player_id = details.get("shootingPlayerId")
        else:
            player_id = None

        if player_id:
            pos = player_pos.get(player_id)

            if pos in pos_list:
                if event_type == "goal":
                    safe_inc(features, f"{team}_goals_{pos}")
                else:
                    safe_inc(features, f"{team}_shots_{pos}")


def process_xg(event_type, details, team, context, features):
    # xG
    if event_type in ["goal", "shot-on-goal", "missed-shot", "blocked-shot"]:
        xg, extra = compute_event_xg(details, team, context)
    else:
        xg, extra = 0, None

    if extra is not None:
        distance, angle = extra

        if team and xg is not None:
            features[f"{team}_xg_distance_sum"] = (
                features.get(f"{team}_xg_distance_sum", 0) + distance
            )
            features[f"{team}_xg_angle_sum"] = features.get(f"{team}_xg_angle_sum", 0) + angle

    if team and extra is not None:
        safe_inc(features, f"{team}_xg_sum", xg)
        safe_inc(features, f"{team}_xg_count")
        features[f"{team}_xg_max"] = max(features[f"{team}_xg_max"], xg)


def process_event(
    e: dict,
    features: dict,
    context: dict,
    home_score: int,
    away_score: int,
):
    event_type = e.get("typeDescKey")
    if not event_type:
        return home_score, away_score

    details = e.get("details", {})

    team_id = details.get("eventOwnerTeamId")

    if team_id == context["home_id"]:
        team = "home"
    elif team_id == context["away_id"]:
        team = "away"
    else:
        team = None

    process_xg(event_type, details, team, context, features)

    # handlers
    scores = (home_score, away_score)
    home_score, away_score = handle_last5(e, event_type, team, features, scores=scores)

    if team and event_type == "shot-on-goal":
        safe_inc(features, f"{team}_shots_total")
    if event_type == "stoppage":
        safe_inc(features, "stoppages_total")

    key = handle_period_stats(e, features)

    handle_goals_shots(event_type, team, key, context["period_types"], features)
    handle_faceoffs(e, event_type, details, team, features)
    handle_positions(event_type, details, team, context["position_context"], features)

    handle_physical(event_type, team, features)
    handle_situations(e, event_type, team, features)

    handle_score_state(features, home_score, away_score)

    if event_type == "goal" and team:
        if team == "home":
            home_score += 1
        else:
            away_score += 1

    return home_score, away_score


# =========================
# Основной pipeline
# =========================
def extract_additional_features(pbp_json: dict) -> dict:
    events = pbp_json.get("plays", [])

    features = {}

    context = {
        "home_id": pbp_json.get("homeTeam", {}).get("id"),
        "away_id": pbp_json.get("awayTeam", {}).get("id"),
        "home_defending_side": pbp_json.get("homeTeamDefendingSide"),
        "period_types": ["REG1", "REG2", "REG3", "OT", "SO"],
        "position_context": {"player_pos": {}, "pos_list": ["C", "L", "R", "D", "G"]},
    }

    # init
    for team in ["home", "away"]:
        features[f"{team}_shots_total"] = 0

        features[f"{team}_xg_distance_sum"] = 0
        features[f"{team}_xg_angle_sum"] = 0

        features[f"{team}_xg_sum"] = 0
        features[f"{team}_xg_count"] = 0
        features[f"{team}_xg_max"] = 0

        features[f"{team}_corsi_5v5"] = 0
        features[f"{team}_fenwick_5v5"] = 0

        features[f"{team}_last5_goals"] = 0
        features[f"{team}_last5_shots"] = 0

        features[f"{team}_hits"] = 0
        features[f"{team}_takeaways"] = 0
        features[f"{team}_giveaways"] = 0
        features[f"{team}_penalties"] = 0

        features[f"{team}_goals_PP"] = 0
        features[f"{team}_shots_PP"] = 0
        features[f"{team}_goals_SH"] = 0
        features[f"{team}_shots_SH"] = 0

        features[f"{team}_empty_net"] = 0

        for pt in ["REG1", "REG2", "REG3", "OT", "SO"]:
            for stat in ["goals", "shots", "missed", "blocked"]:
                if pt == "SO" and stat == "blocked":
                    continue
                features[f"{team}_{stat}_{pt}"] = 0

        for z in ["O", "D", "N"]:
            features[f"{team}_faceoff_{z}"] = 0

        for pos in ["C", "L", "R", "D", "G"]:
            features[f"{team}_goals_{pos}"] = 0
            features[f"{team}_shots_{pos}"] = 0

    features["home_events_leading"] = 0
    features["away_events_leading"] = 0
    features["events_tied"] = 0
    features["stoppages_total"] = 0

    home_score = 0
    away_score = 0

    for e in events:
        home_score, away_score = process_event(e, features, context, home_score, away_score)

    # averages
    for team in ["home", "away"]:
        count = features[f"{team}_xg_count"]
        features[f"{team}_xg_avg"] = features[f"{team}_xg_sum"] / count if count > 0 else 0

    # diffs
    features["diff_xg"] = features["home_xg_sum"] - features["away_xg_sum"]
    features["diff_hits"] = features["home_hits"] - features["away_hits"]
    for team in ["home", "away"]:
        shots = features[f"{team}_shots_total"]
        features[f"{team}_xg_per_shot"] = features[f"{team}_xg_sum"] / shots if shots > 0 else 0

    features["diff_corsi"] = features["home_corsi_5v5"] - features["away_corsi_5v5"]
    features["diff_fenwick"] = features["home_fenwick_5v5"] - features["away_fenwick_5v5"]

    features["diff_pp"] = features["home_goals_PP"] - features["away_goals_PP"]

    return features
