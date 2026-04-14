import os

import requests

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in environment")


def send_message(chat_id: int, text: str, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}

    if reply_markup:
        payload["reply_markup"] = reply_markup

    r = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json=payload, timeout=10
    )

    r.raise_for_status()
