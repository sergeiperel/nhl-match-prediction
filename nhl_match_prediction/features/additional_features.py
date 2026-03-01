# nhl_match_prediction/features/extract_additional_features.py

SITUATION_CODE_LENGTH = 4
LAST_MINUTES_THRESHOLD = 5
FULL_STRENGTH_SKATERS = 5

XG_CLOSE = 20
XG_MEDIUM = 40
XG_CLOSE_VALUE = 0.18
XG_MEDIUM_VALUE = 0.09
XG_FAR_VALUE = 0.03


def parse_situation(code: str):
    if not code or len(code) != SITUATION_CODE_LENGTH or not code.isdigit():
        return None

    away_goalie = int(code[0])
    away_skaters = int(code[1])
    home_skaters = int(code[2])
    home_goalie = int(code[3])

    return away_goalie, away_skaters, home_skaters, home_goalie


def compute_xg(distance: float) -> float:
    if distance < XG_CLOSE:
        return XG_CLOSE_VALUE
    if distance < XG_MEDIUM:
        return XG_MEDIUM_VALUE
    return XG_FAR_VALUE


def handle_last5(e, event_type, team, features, score):
    # -------- последние 5 минут --------
    home_score, away_score = score
    time_remain = e.get("timeRemaining")
    if event_type in ["goal", "shot-on-goal"] and time_remain and team:
        try:
            minutes, _ = map(int, time_remain.split(":"))
            if minutes <= LAST_MINUTES_THRESHOLD:
                if event_type == "goal":
                    features[f"{team}_last5_goals"] += 1

                    # обновляем текущий счёт
                    if team == "home":
                        home_score += 1
                    elif team == "away":
                        away_score += 1

                features[f"{team}_last5_shots"] += 1
        except Exception:
            pass

    return home_score, away_score


def handle_physical(event_type, team, features):
    # -------- физика --------

    if not team:
        return

    mapping = {
        "hit": "hits",
        "takeaway": "takeaways",
        "giveaway": "giveaways",
        "penalty": "penalties",
    }

    stat = mapping.get(event_type)

    if stat:
        features[f"{team}_{stat}"] += 1


def handle_score_state(features, home_score, away_score):
    # home_score = context["home_score"]
    # away_score = context["away_score"]

    # -------- Score State tracking --------
    if home_score > away_score:
        features["home_events_leading"] += 1
    elif away_score > home_score:
        features["away_events_leading"] += 1
    else:
        features["events_tied"] += 1


def handle_period_stats(e):
    # -------- период --------
    period_num = e.get("periodDescriptor", {}).get("number")
    period_type = e.get("periodDescriptor", {}).get("periodType")
    return f"REG{period_num}" if period_type == "REG" else period_type


def determine_special_teams(home_skaters, away_skaters):
    if home_skaters > away_skaters:
        return "home", "away"
    if away_skaters > home_skaters:
        return "away", "home"
    return None, None


def handle_special_teams(e, event_type, team, features):
    # -------- спецбригады --------

    # Those 4 digit code are a representation of the current situation like so:
    # away goalie (1=in net, 0=pulled) - away skaters
    # home skaters - home goalie (1=in net, 0=pulled)
    # **Example: **
    # 1) 1541 Away PP (there's 5 away players
    # and 4 away players on the ice and both goalie on still in the net.)
    # 2) 0641 Away PP and Away goalie is pulled

    situation = e.get("situationCode")

    if team and event_type in ["goal", "shot-on-goal"] and situation:
        parsed = parse_situation(situation)

        if parsed:
            away_goalie, away_skaters, home_skaters, home_goalie = parsed

            is_5v5 = (
                home_skaters == FULL_STRENGTH_SKATERS
                and away_skaters == FULL_STRENGTH_SKATERS
                and home_goalie == 1
                and away_goalie == 1
            )

            if is_5v5 and event_type in ["shot-on-goal", "missed-shot", "blocked-shot"]:
                features[f"{team}_corsi_5v5"] += 1

            if home_goalie == 0:
                features["home_empty_net_events"] += 1
            if away_goalie == 0:
                features["away_empty_net_events"] += 1

            pp_team, sh_team = determine_special_teams(home_skaters, away_skaters)

            # если текущее событие от команды в большинстве
            if team == pp_team:
                if event_type == "goal":
                    features[f"{team}_goals_PP"] += 1
                else:
                    features[f"{team}_shots_PP"] += 1

            # если команда в меньшинстве
            elif team == sh_team:
                if event_type == "goal":
                    features[f"{team}_goals_SH"] += 1
                else:
                    features[f"{team}_shots_SH"] += 1


def handle_xg(e, event_type, details, team, features):
    # -------- xG PROXY --------
    if event_type in ["goal", "shot-on-goal", "missed-shot"] and team:
        x = details.get("xCoord")
        y = details.get("yCoord")

        if x is not None and y is not None:
            # расстояние до ворот (предполагаем ворота на x = ±89)
            goal_x = 89 if team == "home" else -89
            distance = ((x - goal_x) ** 2 + y**2) ** 0.5

            xg = compute_xg(distance)

            # ситуация (PP / SH / 5v5)
            situation = e.get("situationCode")
            parsed = parse_situation(situation) if situation else None

            is_5v5 = False

            if parsed:
                away_goalie, away_skaters, home_skaters, home_goalie = parsed

                # empty net → обнуляем xG
                if home_goalie == 0 or away_goalie == 0:
                    xg = 0

                if home_skaters == away_skaters:
                    is_5v5 = True
                elif (team == "home" and home_skaters > away_skaters) or (
                    team == "away" and away_skaters > home_skaters
                ):
                    xg *= 1.2
                else:
                    xg *= 0.8

            # добавляем к команде
            features[f"{team}_xg_proxy"] += xg

            if is_5v5:
                features[f"{team}_xg_5v5"] += xg


def handle_faceoffs(e, event_type, details, team, features):
    # -------- faceoffs --------
    if event_type == "faceoff" and team:
        zone = details.get("zoneCode")
        if zone in ["O", "D", "N"]:
            features[f"{team}_faceoff_{zone}"] += 1


def handle_positions(event_type, details, team, position_context, features):
    # -------- позиции игроков --------
    player_pos = position_context["player_pos"]
    pos_list = position_context["pos_list"]

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
                    features[f"{team}_goals_{pos}"] += 1
                else:
                    features[f"{team}_shots_{pos}"] += 1


def handle_goals_shots(event_type, team, key, period_types, features):
    # -------- голы и броски по периодам --------
    if team and key in period_types:
        if event_type == "goal":
            features[f"{team}_goals_{key}"] += 1

        elif event_type == "shot-on-goal":
            features[f"{team}_shots_{key}"] += 1

        elif event_type == "missed-shot":
            features[f"{team}_missed_{key}"] += 1

        elif event_type == "blocked-shot" and key != "SO":
            features[f"{team}_blocked_{key}"] += 1


def process_event(
    e: dict,
    features: dict,
    context: dict,
    home_score: int,
    away_score: int,
):
    event_type = e.get("typeDescKey")

    details = e.get("details", {})

    team_id = details.get("eventOwnerTeamId")

    home_id = context["home_id"]
    away_id = context["away_id"]
    period_types = context["period_types"]
    position_context = context["position_context"]

    if team_id == home_id:
        team = "home"
    elif team_id == away_id:
        team = "away"
    else:
        team = None

    # -------- stoppages (общие) --------
    if event_type == "stoppage":
        features["stoppages_total"] += 1

    home_score, away_score = handle_last5(e, event_type, team, features, (home_score, away_score))
    key = handle_period_stats(e)
    handle_goals_shots(event_type, team, key, period_types, features)
    handle_special_teams(e, event_type, team, features)
    handle_xg(e, event_type, details, team, features)

    handle_physical(event_type, team, features)
    handle_faceoffs(e, event_type, details, team, features)
    handle_positions(event_type, details, team, position_context, features)
    handle_score_state(features, home_score, away_score)

    return home_score, away_score


def extract_additional_features(pbp_json: dict) -> dict:
    events = pbp_json.get("plays", [])
    home_id = pbp_json.get("homeTeam", {}).get("id")
    away_id = pbp_json.get("awayTeam", {}).get("id")

    features: dict[str, int] = {}

    # =========================
    # Инициализация фич
    # =========================

    period_types = ["REG1", "REG2", "REG3", "OT", "SO"]
    stats = ["goals", "shots", "missed", "blocked"]

    for team in ["home", "away"]:
        for pt in period_types:
            for stat in stats:
                if pt == "SO" and stat == "blocked":
                    continue
                features[f"{team}_{stat}_{pt}"] = 0

        # последние 5 минут
        features[f"{team}_last5_goals"] = 0
        features[f"{team}_last5_shots"] = 0

        # спецбригады
        features[f"{team}_goals_PP"] = 0
        features[f"{team}_goals_SH"] = 0
        features[f"{team}_shots_PP"] = 0
        features[f"{team}_shots_SH"] = 0

        # физика и владение
        features[f"{team}_hits"] = 0
        features[f"{team}_takeaways"] = 0
        features[f"{team}_giveaways"] = 0
        features[f"{team}_penalties"] = 0

        features[f"{team}_corsi_5v5"] = 0

        features["home_empty_net_events"] = 0
        features["away_empty_net_events"] = 0

        features["home_xg_proxy"] = 0
        features["away_xg_proxy"] = 0

        features["home_xg_5v5"] = 0
        features["away_xg_5v5"] = 0

    # Faceoffs
    for team in ["home", "away"]:
        for z in ["O", "D", "N"]:
            features[f"{team}_faceoff_{z}"] = 0

    # общие stoppages
    features["stoppages_total"] = 0

    # позиции игроков
    player_pos = {p["playerId"]: p.get("positionCode") for p in pbp_json.get("rosterSpots", [])}

    pos_list = ["C", "L", "R", "D", "G"]

    position_context = {
        "player_pos": player_pos,
        "pos_list": pos_list,
    }

    context = {
        "home_id": home_id,
        "away_id": away_id,
        "period_types": period_types,
        "position_context": position_context,
    }

    for team in ["home", "away"]:
        for pos in pos_list:
            features[f"{team}_goals_{pos}"] = 0
            features[f"{team}_shots_{pos}"] = 0

    # =========================
    # Основной проход по событиям
    # =========================

    # -------- Score State --------
    home_score = 0
    away_score = 0

    features["home_events_leading"] = 0
    features["away_events_leading"] = 0
    features["events_tied"] = 0

    for e in events:
        home_score, away_score = process_event(
            e,
            features,
            context,
            home_score,
            away_score,
        )

    # =========================
    # Дифференциалы
    # =========================

    features["diff_hits"] = features["home_hits"] - features["away_hits"]
    features["diff_takeaways"] = features["home_takeaways"] - features["away_takeaways"]
    features["diff_giveaways"] = features["home_giveaways"] - features["away_giveaways"]
    features["diff_penalties"] = features["home_penalties"] - features["away_penalties"]
    features["diff_corsi_5v5"] = features["home_corsi_5v5"] - features["away_corsi_5v5"]
    features["diff_empty_net"] = (
        features["home_empty_net_events"] - features["away_empty_net_events"]
    )
    features["diff_events_leading"] = (
        features["home_events_leading"] - features["away_events_leading"]
    )
    features["diff_xg_proxy"] = features["home_xg_proxy"] - features["away_xg_proxy"]

    return features
