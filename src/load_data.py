"""
Загрузка исходных данных из JSON в базу Postgres.

Поддерживаемые форматы входного JSON:
1) Массив видео:
   [
     { ...video..., "snapshots": [ ... ] },
     ...
   ]

2) Объект с ключом "videos":
   { "videos": [ { ... }, ... ] }

Скрипт можно запускать как модуль:
    python -m src.load_data /app/data/videos.json
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, List, Optional
from datetime import datetime

import ujson

from . import db


def parse_dt(value: Any) -> Optional[datetime]:
    """Парсит ISO‑дату в datetime для asyncpg (TIMESTAMPTZ ожидает datetime)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # поддержка 'Z'
        s = s.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return None
    return None


def to_int(value: Any, default: int = 0) -> int:
    """Безопасное приведение к int."""
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return default
        try:
            return int(float(s))
        except ValueError:
            return default
    return default


async def load_data(json_path: str) -> None:
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"file {json_path} not found")

    # Инициализируем БД и применяем миграции
    await db.init_pool()
    await db.run_migrations()
    assert db.pool is not None

    # Читаем JSON
    with path.open("rb") as f:
        data = ujson.load(f)

    # Поддержка формата { "videos": [ ... ] }
    if isinstance(data, dict) and "videos" in data and isinstance(data["videos"], list):
        data = data["videos"]

    if not isinstance(data, list):
        raise ValueError("expected JSON array of videos (or object with key 'videos')")

    videos_rows: List[tuple] = []
    snapshots_rows: List[tuple] = []

    for video in data:
        if not isinstance(video, dict):
            continue

        vid = video.get("id")  # UUID or string
        creator_id = video.get("creator_id")  # possibly string or int

        video_created_at = parse_dt(video.get("video_created_at"))
        created_at = parse_dt(video.get("created_at"))
        updated_at = parse_dt(video.get("updated_at"))

        views_count = to_int(video.get("views_count"))
        likes_count = to_int(video.get("likes_count"))
        comments_count = to_int(video.get("comments_count"))
        reports_count = to_int(video.get("reports_count"))

        videos_rows.append(
            (
                vid,
                creator_id,
                video_created_at,
                views_count,
                likes_count,
                comments_count,
                reports_count,
                created_at,
                updated_at,
            )
        )

        # Снапшоты
        snaps = video.get("snapshots") or []
        if isinstance(snaps, list):
            for snap in snaps:
                if not isinstance(snap, dict):
                    continue

                snapshots_rows.append(
                    (
                        snap.get("id"),  # UUID or string
                        vid,  # video_id
                        to_int(snap.get("views_count")),
                        to_int(snap.get("likes_count")),
                        to_int(snap.get("comments_count")),
                        to_int(snap.get("reports_count")),
                        to_int(snap.get("delta_views_count")),
                        to_int(snap.get("delta_likes_count")),
                        to_int(snap.get("delta_comments_count")),
                        to_int(snap.get("delta_reports_count")),
                        parse_dt(snap.get("created_at")),
                        parse_dt(snap.get("updated_at")),
                    )
                )

    if not videos_rows:
        print("Нет данных для загрузки (videos_rows пуст). Проверь JSON.")
        return

    # Вставка в транзакции
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO videos (
                    id, creator_id, video_created_at, views_count, likes_count,
                    comments_count, reports_count, created_at, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (id) DO NOTHING
                """,
                videos_rows,
            )

            if snapshots_rows:
                await conn.executemany(
                    """
                    INSERT INTO video_snapshots (
                        id, video_id, views_count, likes_count, comments_count,
                        reports_count, delta_views_count, delta_likes_count,
                        delta_comments_count, delta_reports_count,
                        created_at, updated_at
                    ) VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
                    )
                    ON CONFLICT (id) DO NOTHING
                    """,
                    snapshots_rows,
                )

    print(f"Загружено {len(videos_rows)} видео и {len(snapshots_rows)} снапшотов")


def main() -> None:
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m src.load_data path/to/data.json")
        raise SystemExit(1)
    asyncio.run(load_data(sys.argv[1]))


if __name__ == "__main__":
    main()