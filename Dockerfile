FROM python:3.11-slim AS base

# Устанавливаем зависимости системы
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends libpq-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /app

# Копируем файлы зависимостей отдельно, чтобы использовать слой кэша
COPY requirements.txt ./

# Устанавливаем Python‑зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем исходный код в контейнер
COPY src/ ./src/
COPY migrations/ ./migrations/
COPY entrypoint.sh ./entrypoint.sh

# Делаем entrypoint исполняемым
RUN chmod +x entrypoint.sh

# Устанавливаем переменные окружения
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

# Запускаемый скрипт
CMD ["/bin/bash", "entrypoint.sh"]