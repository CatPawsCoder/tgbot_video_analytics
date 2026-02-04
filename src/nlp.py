"""
Модуль для взаимодействия с LLM и преобразования естественного языка в SQL.

Поддерживает провайдеры:
- gigachat (по умолчанию в проекте)
- openai (опционально)

Возвращает ОДИН SQL‑запрос, который отдаёт ОДНО числовое значение.
"""

from __future__ import annotations

import re

import openai
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole

from .config import settings


import re

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

def sanitize_sql(sql: str) -> str:
    sql = sql.strip()

    # убрать ```sql ``` обёртки
    sql = re.sub(r"^```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\s*```$", "", sql)

    # убрать одиночные backticks `...`
    if sql.startswith("`") and sql.endswith("`"):
        sql = sql[1:-1].strip()

    # убрать префиксы типа "SQL:"
    sql = re.sub(r"^(SQL|Ответ)\s*:\s*", "", sql, flags=re.IGNORECASE).strip()

    # если модель вернула несколько запросов — берём только первый
    if ";" in sql:
        first = sql.split(";")[0].strip()
        if first:
            sql = first + ";"

    # Если встречается creator_id = 'что-то' и это НЕ UUID -> сравниваем как текст
    def repl_creator(m):
        val = m.group(1)
        if UUID_RE.match(val):
            return f"creator_id = '{val}'"
        return f"creator_id::text = '{val}'"

    sql = re.sub(r"creator_id\s*=\s*'([^']+)'", repl_creator, sql, flags=re.IGNORECASE)

    return sql


SCHEMA_DESCRIPTION = """
У тебя есть база данных PostgreSQL с двумя таблицами.

ВАЖНО:
- id, creator_id и video_id — UUID (строки вида xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
- Если в вопросе указан id как число (например "42"), трактуй это как строку: creator_id = '42'.
- Отвечай ТОЛЬКО SQL‑запросом. Никаких пояснений, списков, markdown, ``` или `...`.

Таблица videos (итоговая статистика по ролику):
  id — UUID, идентификатор видео (первичный ключ)
  creator_id — UUID, идентификатор создателя
  video_created_at — TIMESTAMPTZ, время публикации видео
  views_count — INTEGER, итоговое число просмотров
  likes_count — INTEGER, итоговое число лайков
  comments_count — INTEGER, итоговое число комментариев
  reports_count — INTEGER, итоговое число жалоб
  created_at — TIMESTAMPTZ, время создания записи
  updated_at — TIMESTAMPTZ, время обновления записи

Таблица video_snapshots (почасовые замеры по ролику):
  id — UUID, идентификатор снапшота (первичный ключ)
  video_id — UUID, ссылка на videos.id
  views_count — INTEGER, текущее число просмотров на момент замера
  likes_count — INTEGER, текущее число лайков
  comments_count — INTEGER, текущее число комментариев
  reports_count — INTEGER, текущее число жалоб
  delta_views_count — INTEGER, прирост просмотров с предыдущего снапшота
  delta_likes_count — INTEGER, прирост лайков
  delta_comments_count — INTEGER, прирост комментариев
  delta_reports_count — INTEGER, прирост жалоб
  created_at — TIMESTAMPTZ, время замера (раз в час)
  updated_at — TIMESTAMPTZ, время обновления записи

Запрос должен возвращать ОДНО числовое значение.

Примеры (без кавычек ` и без markdown):

1) Сколько всего видео есть в системе?
SELECT COUNT(*) FROM videos;

2) Сколько видео у креатора с id 42 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?
SELECT COUNT(*) FROM videos
WHERE creator_id = '42'
  AND video_created_at >= '2025-11-01'
  AND video_created_at <= '2025-11-05 23:59:59';

3) На сколько просмотров в сумме выросли все видео 28 ноября 2025?
SELECT COALESCE(SUM(delta_views_count),0)
FROM video_snapshots
WHERE DATE(created_at) = '2025-11-28';
"""


async def query_to_sql(question: str) -> str:
    """
    Преобразует текст вопроса в SQL‑запрос с помощью LLM.

    Аргументы:
        question: текст сообщения от пользователя на русском.

    Возвращает:
        Строку с SQL‑запросом. В запросе не должно быть комментариев или
        постороннего текста. Предполагается, что он возвращает одно число.
    """
    provider = settings.llm_provider.lower().strip()

    if provider == "openai":
        # Для openai v1 API создаём клиент и отправляем запрос.
        openai.api_key = settings.openai_api_key
        messages = [
            {"role": "system", "content": SCHEMA_DESCRIPTION},
            {"role": "user", "content": question.strip()},
        ]
        response = await openai.ChatCompletion.acreate(
            model=settings.llm_model,
            messages=messages,
            temperature=0.0,
            max_tokens=200,
        )
        raw = response.choices[0].message["content"]
        return sanitize_sql(raw)

    if provider == "gigachat":
        chat = Chat(
            messages=[
                Messages(role=MessagesRole.SYSTEM, content=SCHEMA_DESCRIPTION),
                Messages(role=MessagesRole.USER, content=question.strip()),
            ]
        )

        client_kwargs = {}
        # Параметры GigaChat считываются из настроек.
        if settings.gigachat_credentials:
            client_kwargs["credentials"] = settings.gigachat_credentials
        if settings.gigachat_scope:
            client_kwargs["scope"] = settings.gigachat_scope
        if settings.gigachat_model:
            client_kwargs["model"] = settings.gigachat_model
        if settings.gigachat_ca_bundle_file:
            client_kwargs["ca_bundle_file"] = settings.gigachat_ca_bundle_file
        if settings.gigachat_verify_ssl_certs is not None:
            client_kwargs["verify_ssl_certs"] = settings.gigachat_verify_ssl_certs

        async with GigaChat(**client_kwargs) as client:
            response = await client.achat(chat)
            raw = response.choices[0].message.content
            return sanitize_sql(raw)

    raise ValueError(f"Unknown LLM provider: {settings.llm_provider}")