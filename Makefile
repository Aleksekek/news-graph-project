.PHONY: help install dev-install test lint format clean run-parser run-bot docker-up docker-down

help:
	@echo "Доступные команды:"
	@echo "  install       - Установка зависимостей"
	@echo "  dev-install   - Установка dev зависимостей"
	@echo "  test         - Запуск тестов"
	@echo "  lint         - Проверка кода"
	@echo "  format       - Форматирование кода"
	@echo "  clean        - Очистка временных файлов"
	@echo "  run-parser   - Запуск парсера Lenta.ru"
	@echo "  run-bot      - Запуск телеграм бота"
	@echo "  docker-up    - Запуск Docker контейнеров"
	@echo "  docker-down  - Остановка Docker контейнеров"

install:
	pip install -e .

dev-install:
	pip install -e ".[dev]"

test:
	python -m pytest tests/ -v

lint:
	python -m ruff check src/
	python -m mypy src/

format:
	python -m black src/ tests/
	python -m isort src/ tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +

run-parser:
	python -m scripts.run_parser --source lenta --limit 50

run-bot:
	python -m src.infrastructure.telegram.bot

docker-up:
	docker-compose up -d
docker-down:
	docker-compose down
docker-logs:
	docker-compose logs -f scheduler