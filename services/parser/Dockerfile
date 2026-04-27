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

# Копируем точку входа
COPY services/parser/app.py .

# Создаём директории для логов
RUN mkdir -p /app/logs

CMD ["python", "app.py"]