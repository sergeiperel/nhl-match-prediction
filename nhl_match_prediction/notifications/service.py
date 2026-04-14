def build_message(games, hours_before: int):
    lines = [f"⏰ Матчи через ~{hours_before} часов:\n"]

    for g in games:
        lines.append(
            f"🏒 <b>{g['home_team_abbr']}</b> vs <b>{g['away_team_abbr']}</b>\n"
            f"🕒 {g['game_date'].strftime('%d.%m %H:%M UTC')}\n"
        )

    return "\n".join(lines)
