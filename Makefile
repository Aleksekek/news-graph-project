.PHONY: help install build up down logs logs-parser logs-summarizer logs-ner logs-bot clean test

help:
	@echo "Доступные команды:"
	@echo "  install         - Установка зависимостей"
	@echo "  build           - Сборка Docker образов"
	@echo "  up              - Запуск всех сервисов"
	@echo "  down            - Остановка всех сервисов"
	@echo "  logs            - Просмотр логов всех сервисов"
	@echo "  logs-parser     - Логи парсера"
	@echo "  logs-summarizer - Логи суммаризатора"
	@echo "  logs-ner        - Логи NER-сервиса"
	@echo "  logs-bot        - Логи бота"
	@echo "  clean           - Остановка и очистка"
	@echo "  test            - Запуск тестов"

install:
	pip install -r requirements.txt

build:
	docker-compose build --no-cache

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

logs-parser:
	docker-compose logs -f parser

logs-summarizer:
	docker-compose logs -f summarizer

logs-ner:
	docker-compose logs -f ner

logs-bot:
	docker-compose logs -f bot

clean:
	docker-compose down -v
	docker system prune -f

test:
	pytest tests/ -v --cov=src --cov-report=term