"""Точка входа для телеграм-бота.

aiogram v3.5.0: polling, без state.
Получает вопрос на русском -> LLM генерит SQL -> выполняем -> отвечаем числом.
"""

from __future__ import annotations

import asyncio
import logging

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

from .config import settings
from .db import init_pool, close_pool, fetchval, run_migrations
from .nlp import query_to_sql, sanitize_sql


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    # БД + миграции
    await init_pool()
    await run_migrations()

   
    bot = Bot(token=settings.bot_token)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start(message: Message):
        await message.answer(
            "Привет! Я бот для аналитики по видео.\n\n"
            "Примеры:\n"
            "• Сколько всего видео есть в системе?\n"
            "• На сколько просмотров в сумме выросли все видео 28 ноября 2025?\n"
        )

    @dp.message(F.text)
    async def handle(message: Message):
        question = (message.text or "").strip()
        if not question:
            await message.answer("Пустой запрос. Напиши вопрос текстом.")
            return

        try:
            sql = await query_to_sql(question)
            sql = sanitize_sql(sql)

            logger.info("Generated SQL: %s", sql)

            result = await fetchval(sql)
            await message.answer(str(result if result is not None else 0))

        except Exception:
            logger.exception("Error processing query: %s", question)
            await message.answer(
                "Ошибка при обработке запроса.\n"
                "Попробуй переформулировать вопрос (или проверь, что данные загружены)."
            )

    try:
        await dp.start_polling(bot)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
