import pandas as pd
import redis
from celery import shared_task

from nhl_match_prediction.upcoming_features.build_upcoming_matches import get_upcoming_matches

from .keys import scheduled_key, sent_key
from .sender import send_message
from .service import build_message

r = redis.Redis(host="redis", decode_responses=True)


NOTIFICATION_OFFSETS = [
    5,  # 5 часов до матча
    # 1,
]


@shared_task
def schedule_notifications():
    lock_key = "notif:lock"

    if not r.set(lock_key, "1", nx=True, ex=50):
        return

    df = get_upcoming_matches()
    if df.empty:
        return

    df["game_date"] = pd.to_datetime(df["game_date"], utc=True)
    now = pd.Timestamp.utcnow()

    users = list(r.smembers("subscribers"))

    for _, game in df.iterrows():
        game_id = game["game_id"]

        for chat_id in users:
            for h in NOTIFICATION_OFFSETS:
                send_at = game["game_date"] - pd.Timedelta(hours=h)

                if send_at < now:
                    continue

                skey = scheduled_key(game_id, chat_id, str(h))

                if r.exists(skey):
                    continue

                r.set(skey, 1, ex=60 * 60 * 24)

                send_game_notification.apply_async(
                    args=[game_id, chat_id, h], eta=send_at.to_pydatetime()
                )


@shared_task
def send_game_notification(game_id: int, chat_id: int, hours_before: int):
    key = sent_key(game_id, chat_id, hours_before)

    if r.exists(key):
        return

    df = get_upcoming_matches()
    df = df[df["game_id"] == game_id]

    if df.empty:
        return

    df["game_date"] = pd.to_datetime(df["game_date"], utc=True)

    text = build_message(df.to_dict("records"), hours_before)

    send_message(chat_id, text)

    r.set(key, 1)
