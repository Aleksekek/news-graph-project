FROM python:3.11-slim

WORKDIR /app

# Копируем зависимости
COPY services/bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем общий код
COPY src /app/src

# Копируем точку входа
COPY services/bot/app.py .

# Создаём директории для логов
RUN mkdir -p /app/logs

CMD ["python", "app.py"]