"""Загрузка конфигурации приложения.

Все настройки читаются из переменных окружения. Для локальной разработки
удобно использовать файл `.env`, который можно создать на основе `.env.example`.
Используется Pydantic для валидации и типизации.
"""

from __future__ import annotations

from functools import lru_cache
# В pydantic v2 класс BaseSettings переехал в пакет pydantic_settings.
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    bot_token: str = Field(..., env="BOT_TOKEN")
    database_url: str = Field(..., env="DATABASE_URL")
    openai_api_key: str = Field(..., env="OPENAI_API_KEY")
    llm_model: str = Field("gpt-3.5-turbo", env="LLM_MODEL")
    llm_provider: str = Field("openai", env="LLM_PROVIDER")

    # Параметры GigaChat. Если вы используете GigaChat, заполните их в .env
    gigachat_credentials: str | None = Field(None, env="GIGACHAT_CREDENTIALS")
    gigachat_scope: str | None = Field(None, env="GIGACHAT_SCOPE")
    gigachat_model: str | None = Field(None, env="GIGACHAT_MODEL")
    gigachat_ca_bundle_file: str | None = Field(None, env="GIGACHAT_CA_BUNDLE_FILE")
    gigachat_verify_ssl_certs: bool | None = Field(None, env="GIGACHAT_VERIFY_SSL_CERTS")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    """Возвращает кешированный экземпляр настроек.

    Функция обёрнута в lru_cache, чтобы не создавать объект при каждом
    обращении. Это важно для модулей, которые импортируются несколько раз.
    """
    return Settings()


settings = get_settings()
