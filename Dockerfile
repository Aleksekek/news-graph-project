FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем только файлы с зависимостями (для кэширования слоев)
COPY requirements.txt pyproject.toml ./

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальной код
COPY /src /app/src
COPY /config /app/config
COPY /scripts /app/scripts

# Создаем директории для логов и данных
RUN mkdir -p /app/logs /app/data

# Команда по умолчанию (запускает планировщик)
CMD ["python", "-m", "src.domain.scheduling.runner"]