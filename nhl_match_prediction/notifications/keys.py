def sent_key(game_id: int, chat_id: int, offset: str):
    return f"notif:sent:{game_id}:{chat_id}:{offset}"


def scheduled_key(game_id: int, chat_id: int, offset: str):
    return f"notif:scheduled:{game_id}:{chat_id}:{offset}"
