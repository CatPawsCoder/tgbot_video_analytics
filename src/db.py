"""Работа с базой данных Postgres.

Модуль содержит функции для создания подключения, выполнения миграций
и удобные обёртки для выполнения SQL‑запросов. Вся работа ведётся
асинхронно через `asyncpg`.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Optional

import asyncpg

from .config import settings


pool: Optional[asyncpg.pool.Pool] = None


def _build_dsn() -> str:
    """Преобразует DATABASE_URL в формат, который понимает asyncpg.

    sqlalchemy‑подобный вид `postgresql+asyncpg://user:pass@host:port/db` заменяется
    на `postgresql://user:pass@host:port/db`. Если суффикс +asyncpg отсутствует,
    возвращается исходная строка.
    """
    url = settings.database_url
    if "+asyncpg" in url:
        return url.replace("+asyncpg", "")
    return url


async def init_pool() -> None:
    """Создаёт пул подключений к Postgres.

    Пул сохраняется в глобальную переменную `pool` и используется другими
    функциями. Повторный вызов перезаписывает пул.
    """
    global pool
    dsn = _build_dsn()
    pool = await asyncpg.create_pool(dsn=dsn)


async def close_pool() -> None:
    """Закрывает пул подключений, если он существует."""
    global pool
    if pool is not None:
        await pool.close()
        pool = None


async def run_migrations() -> None:
    """Выполняет SQL‑скрипты миграций.

    На данный момент существует один файл `migrations/001_init.sql`. Если вы
    добавляете другие миграции, расширьте список. Функция читает содержимое
    файла и отправляет его в базу одной транзакцией.
    """
    # Определяем путь к директории проекта (два уровня выше текущего файла)
    base_dir = Path(__file__).resolve().parents[1]
    migrations_dir = base_dir / "migrations"
    filenames = ["001_init.sql"]

    # Убеждаемся, что пул создан
    if pool is None:
        await init_pool()

    async with pool.acquire() as conn:
        for fname in filenames:
            path = migrations_dir / fname
            if not path.exists():
                raise FileNotFoundError(f"migration file {fname} not found")
            sql = path.read_text(encoding="utf-8")
            # asyncpg позволяет отправлять несколько операторов за один вызов
            await conn.execute(sql)


async def fetch(sql: str, *args: Any) -> list[asyncpg.Record]:
    """Выполняет запрос и возвращает список записей."""
    if pool is None:
        await init_pool()
    assert pool is not None
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)


async def fetchval(sql: str, *args: Any) -> Any:
    """Выполняет запрос и возвращает одно значение."""
    if pool is None:
        await init_pool()
    assert pool is not None
    async with pool.acquire() as conn:
        return await conn.fetchval(sql, *args)


async def execute(sql: str, *args: Any) -> str:
    """Выполняет запрос без возврата значений (например, INSERT/UPDATE)."""
    if pool is None:
        await init_pool()
    assert pool is not None
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)


async def create_tables() -> None:
    """Совместимость: запуск миграций при обращении как модуль."""
    await run_migrations()


def main() -> None:
    """Точка входа для запуска миграций через `python -m src.db`."""
    asyncio.run(run_migrations())


if __name__ == "__main__":
    main()