import asyncio
import os

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set in environment variables")

API_URL = os.getenv("API_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


async def on_startup(bot: Bot):
    await bot.set_my_commands(
        [
            types.BotCommand(command="start", description="Запуск бота"),
            types.BotCommand(command="help", description="Помощь"),
            types.BotCommand(command="upcoming_matches", description="Прогнозы"),
            types.BotCommand(command="accuracy", description="Точность модели"),
            types.BotCommand(command="subscribe", description="Включить уведомления"),
            types.BotCommand(command="unsubscribe", description="Отключить уведомления"),
        ]
    )

    await bot.set_chat_menu_button(menu_button=types.MenuButtonCommands())


async def fetch_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.json()


async def post_request(session: aiohttp.ClientSession, url: str):
    async with session.post(url, timeout=10) as resp:
        return resp.status


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        "🏒 Привет! Я бот прогнозов NHL\n\n"
        "Команды:\n"
        "/upcoming_matches — прогнозы\n"
        "/accuracy — точность модели\n"
        "/subscribe — включить уведомления 🔔\n"
        "/unsubscribe — отключить уведомления ❌"
    )


@dp.message(Command("subscribe"))
async def subscribe(message: types.Message):
    chat_id = message.chat.id

    async with aiohttp.ClientSession() as session:
        try:
            status = await post_request(session, f"{API_URL}/subscribe/{chat_id}")

            if status == 200:  # noqa: PLR2004
                await message.answer("✅ Ты подписался на уведомления!")
            else:
                await message.answer("❌ Ошибка подписки")

        except Exception as e:
            await message.answer(f"Ошибка: {e}")


@dp.message(Command("unsubscribe"))
async def unsubscribe(message: types.Message):
    chat_id = message.chat.id

    async with aiohttp.ClientSession() as session:
        try:
            status = await post_request(session, f"{API_URL}/unsubscribe/{chat_id}")

            if status == 200:  # noqa: PLR2004
                await message.answer("❌ Ты отписался от уведомлений")
            else:
                await message.answer("❌ Ошибка отписки")

        except Exception as e:
            await message.answer(f"Ошибка: {e}")


@dp.message(Command("upcoming_matches"))
async def upcoming(message: types.Message):
    def format_games(games, title):
        if not games:
            return ""

        text = f"{title}\n"

        for game in games:
            prob = game["prediction_prob"]

            winner = game["home_team_abbr"] if game["prediction"] == 1 else game["away_team_abbr"]

            win_prob = prob if game["prediction"] == 1 else (1 - prob)

            text += (
                "\n"
                f"🏠 {game['home_team_abbr']} vs {game['away_team_abbr']} ✈️\n"
                f"🎯 Победит: {winner} ({round(win_prob * 100, 1)}%)\n"
            )

        return text

    async with aiohttp.ClientSession() as session:
        try:
            res = await fetch_json(session, f"{API_URL}/predict_feed")

            if "message" in res:
                await message.answer(res["message"])
                return

            text = ""
            text += format_games(res.get("live", []), "🔴 ===== LIVE ===== 🔴")
            text += "\n"
            text += format_games(res.get("today_upcoming", []), "🟢 ===== Сегодня ===== 🟢")
            text += "\n"
            text += format_games(res.get("tomorrow", []), "🔵 ===== Завтра ===== 🔵")

            await message.answer(text or "Нет матчей 😢")

        except Exception as e:
            await message.answer(f"Ошибка: {e}")


@dp.message(Command("accuracy"))
async def accuracy(message: types.Message):
    async with aiohttp.ClientSession() as session:
        try:
            res = await fetch_json(session, f"{API_URL}/accuracy")
            await message.answer(f"📈 Accuracy: {res['accuracy']}%")

        except Exception as e:
            await message.answer(f"Ошибка: {e}")


async def main():
    await on_startup(bot)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
