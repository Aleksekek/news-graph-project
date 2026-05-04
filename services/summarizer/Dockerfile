FROM python:3.14-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY services/summarizer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем общий код
COPY src /app/src

# Копируем точку входа и healthcheck
COPY services/summarizer/app.py .
COPY services/healthcheck.py /app/healthcheck.py

# Создаём директории для логов
RUN mkdir -p /app/logs

HEALTHCHECK --interval=60s --timeout=10s --start-period=120s --retries=3 \
    CMD python /app/healthcheck.py || exit 1

CMD ["python", "app.py"]