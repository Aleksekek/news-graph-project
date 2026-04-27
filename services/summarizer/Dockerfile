FROM python:3.11-slim

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
COPY config /app/config

# Копируем точку входа
COPY services/summarizer/app.py .

# Создаём директории для логов
RUN mkdir -p /app/logs

CMD ["python", "app.py"]