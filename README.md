# News Graph Project

Агрегатор новостей с автоматическим парсингом, суммаризацией через LLM и Telegram ботом.

## 🚀 Быстрый старт

```bash
# Клонирование
git clone https://github.com/Aleksekek/news-graph-project.git
cd news_graph_project

# Настройка
cp .env.example .env
# Заполните .env своими данными

# Запуск всех сервисов
docker-compose up -d

# Или по отдельности
make up           # все сервисы
make up-parser    # только парсер
make up-bot       # только бот
```

## 📦 Сервисы

| Сервис | Описание |
|--------|----------|
| **Parser** | Сбор новостей из источников |
| **Summarizer** | Генерация сводок через LLM API |
| **Telegram Bot** | Интерактивный новостной бот |

## 🛠 Команды

```bash
make help          # список всех команд
make test          # запуск тестов
make logs          # просмотр логов
make restart-bot   # перезапуск бота
make shell-parser  # вход в контейнер парсера
```

## 📁 Структура

```
src/
├── app/           # Точки входа (scheduler, summarizer, bot)
├── core/          # Модели, константы, исключения
├── database/      # Репозитории и пул соединений
├── parsers/       # Lenta, TInvest + конвертеры
├── processing/    # LLM, суммаризация
├── utils/         # datetime, logging, retry
└── infrastructure/# Telegram бот
```

## 🧪 Тестирование

```bash
pytest tests/ -v                    # все тесты
pytest tests/unit/ -v               # только unit
pytest tests/integration/ -v -m integration  # интеграционные
pytest tests/ --cov=src --cov-report=html   # покрытие
```

## 🔧 Переменные окружения

Обязательные:
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `DEEPSEEK_API_KEY`
- `TELEGRAM_BOT_TOKEN`

Опциональные:
- `PROXY_URL` — host:port:user:pass
- `ADMIN_CHAT_ID` — для уведомлений
- `LOG_LEVEL` — DEBUG/INFO/WARNING/ERROR

## 📊 Покрытие кода

| Компонент | Покрытие |
|-----------|----------|
| core | 100% |
| parsers | 73-83% |
| utils | 70-82% |
| app | 85% |
| **Total** | **~75%** |

## 📚 Документация

- [Архитектура проекта](docs/architecture.md)
- [Работа с часовыми поясами](docs/timezone_handling.md)
- [Добавление нового источника](docs/adding_new_source.md)

## 🤝 Contributing

1. Форкните репозиторий
2. Создайте ветку `feature/your-feature`
3. Убедитесь что тесты проходят: `make test`
4. Создайте Pull Request
