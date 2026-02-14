"""
Telegram-бот для поиска автозапчастей.
Использует parsers.search_all_sites() и database.db.
"""

import asyncio
import logging
import sys
import os

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from dotenv import load_dotenv

from database import db
from logic import filter_results
from parsers import search_all_sites

# .env — ищем в родительской папке или рядом
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")

if not API_TOKEN:
    print("❌ Токен не найден! Добавь TELEGRAM_BOT_TOKEN в .env")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()


def _webapp_kb():
    """Клавиатура с кнопкой Mini App (если URL задан)."""
    if not WEBAPP_URL:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔧 Открыть поиск", web_app=WebAppInfo(url=WEBAPP_URL))]
    ])


@dp.message(Command("start"))
async def cmd_start(message: Message):
    await db.add_history(message.from_user.id, "/start")
    text = (
        "👋 Привет! Я ищу автозапчасти по 10 сайтам.\n\n"
        "Отправь мне:\n"
        "• **Название** — _масло моторное 5w40_\n"
        "• **Артикул** — _5Q0615301_\n\n"
        "Команды:\n"
        "/search <запрос> — поиск\n"
        "/history — история\n"
        "/favorites — избранное"
    )
    await message.answer(text, parse_mode=ParseMode.MARKDOWN, reply_markup=_webapp_kb())


@dp.message(Command("history"))
async def cmd_history(message: Message):
    history = await db.get_history(message.from_user.id, limit=10)
    if not history:
        await message.answer("📋 История пуста.")
        return
    lines = ["📋 **Последние запросы:**\n"]
    for h in history:
        lines.append(f"• `{h['query']}`")
    await message.answer("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("favorites"))
async def cmd_favorites(message: Message):
    favs = await db.get_favorites(message.from_user.id)
    if not favs:
        await message.answer("⭐ Избранное пусто.")
        return
    text = "⭐ **Избранное:**\n\n"
    for f in favs[:15]:
        title = f['title'].replace("*", "").replace("_", "")
        text += f"🔸 *{f['price_int']} ₽* | {f['source']}\n[{title}]({f['link']})\n\n"
    await message.answer(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


@dp.message(Command("search"))
async def cmd_search(message: Message):
    query = message.text.replace("/search", "").strip()
    if not query:
        await message.answer("Используй: `/search масло 5w40`", parse_mode=ParseMode.MARKDOWN)
        return
    await _do_search(message, query)


@dp.message()
async def handle_text(message: Message):
    query = message.text.strip()
    if len(query) < 3:
        await message.answer("⚠️ Запрос слишком короткий (мин. 3 символа).")
        return
    await _do_search(message, query)


async def _do_search(message: Message, query: str):
    """Основная логика поиска."""
    user_id = message.from_user.id
    await db.add_history(user_id, query)

    q_escaped = query.replace("*", "").replace("_", "")
    await message.answer(f"🔍 Ищу *{q_escaped}* по 10 сайтам...", parse_mode=ParseMode.MARKDOWN)

    # Кэш
    cached = await db.get_cache(query)
    if cached:
        results = cached
        from_cache = True
    else:
        results = await search_all_sites(query)
        if results:
            await db.save_cache(query, results)
        from_cache = False

    # Фильтрация (с альтернативной оценкой по артикулу)
    from parsers import _extract_article_query
    article_q = _extract_article_query(query) or ''
    filtered = filter_results(results, query, article_query=article_q)

    if not filtered:
        await message.answer("😔 Ничего не найдено. Попробуй уточнить запрос.")
        return

    # Формат ответа
    text = f"📦 **{q_escaped}** — {len(filtered)} результатов\n"
    if from_cache:
        text += "⚡️ _(из кэша)_\n"
    text += "\n"

    for item in filtered[:15]:
        title = item['title'].replace("*", "").replace("_", "").replace("[", "").replace("]", "")
        text += f"🔸 *{item['price']}* | {item['source']}\n"
        text += f"[{title}]({item['link']})\n\n"

    if len(filtered) > 15:
        text += f"_...и ещё {len(filtered) - 15}_"

    if len(text) > 4000:
        text = text[:4000] + "\n_(обрезано)_"

    await message.answer(text, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


async def main():
    await db.init_db()
    logging.info(f"🤖 Бот запущен. Парсеры: 10 сайтов. Mini App: {'✅' if WEBAPP_URL else '❌ (WEBAPP_URL не задан)'}")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
