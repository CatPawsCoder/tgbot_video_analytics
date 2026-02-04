#!/bin/bash
# Скрипт для запуска контейнера приложения.
# Он ждёт доступности базы данных, запускает миграции, а затем запускает бота.

set -e

echo "[entrypoint] waiting for database to be available..."

# Функция проверки подключения к БД через asyncpg
python - <<'PY'
import asyncio
import asyncpg
import os
import sys
async def check():
    url = os.environ.get('DATABASE_URL')
    # asyncpg не использует схему sqlalchemy, убираем суффикс '+asyncpg'
    if url and '+asyncpg' in url:
        url = url.replace('+asyncpg', '')
    try:
        conn = await asyncpg.connect(dsn=url)
        await conn.close()
        sys.exit(0)
    except Exception:
        sys.exit(1)
asyncio.run(check())
PY
RET=$?
while [ $RET -ne 0 ]; do
    echo "[entrypoint] Database is unavailable - sleeping"
    sleep 2
    python - <<'PY'
import asyncio
import asyncpg
import os
import sys
async def check():
    url = os.environ.get('DATABASE_URL')
    if url and '+asyncpg' in url:
        url = url.replace('+asyncpg', '')
    try:
        conn = await asyncpg.connect(dsn=url)
        await conn.close()
        sys.exit(0)
    except Exception:
        sys.exit(1)
asyncio.run(check())
PY
    RET=$?
done

echo "[entrypoint] Running migrations..."
# Запуск миграций
python -m src.db

echo "[entrypoint] Starting bot..."
exec python -m src.bot