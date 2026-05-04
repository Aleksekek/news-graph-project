FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем только нужные зависимости
COPY services/parser/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем общий код
COPY src /app/src

# Копируем точку входа и healthcheck
COPY services/parser/app.py .
COPY services/healthcheck.py /app/healthcheck.py

# Создаём директории для логов
RUN mkdir -p /app/logs

# Healthcheck: проверяет что в /app/logs/$SERVICE_NAME.log была запись
# не старше HEALTHCHECK_MAX_AGE сек. Override threshold в docker-compose.yml.
HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD python /app/healthcheck.py || exit 1

CMD ["python", "app.py"]