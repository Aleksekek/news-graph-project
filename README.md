# News Graph Project

Агрегатор новостей с автоматическим парсингом, NER-анализом, суммаризацией через LLM и Telegram-ботом.

## Быстрый старт

```bash
git clone https://github.com/Aleksekek/news-graph-project.git
cd news_graph_project

cp .env.example .env
# Заполните .env своими данными

docker-compose up -d
```

## Сервисы

| Сервис | Описание | Расписание |
|--------|----------|------------|
| **parser** | Сбор новостей из 5 источников (Lenta, TInvest, Interfax, TASS, RBC) | Каждые 30 мин на источник (TInvest — 15 мин), staggered по 5 мин |
| **summarizer** | Генерация сводок через DeepSeek | Каждый час + дневная сводка 09:05 |
| **ner** | Извлечение именованных сущностей (Natasha) | Каждые 30 мин + очистка вс 03:00 |
| **bot** | Telegram-бот | По запросу пользователя |

## Команды

```bash
make help              # список всех команд
make test              # запуск тестов
make logs              # логи всех сервисов
make logs-parser       # логи парсера
make logs-ner          # логи NER-сервиса
make logs-summarizer   # логи суммаризатора
make logs-bot          # логи бота
```

## Структура проекта

```
src/
├── app/
│   ├── scheduler.py        # планировщик парсера
│   ├── summarizer.py       # суммаризация
│   ├── ner_processor.py    # NER-обработка статей
│   └── entity_cleanup.py   # LLM-очистка сущностей (модуль)
├── core/                   # модели, константы, исключения
├── database/
│   └── repositories/       # ArticleRepository, EntityRepository,
│                           # ProcessedArticleRepository, ArticleEntityRepository,
│                           # SummaryRepository
├── parsers/                # Lenta, TInvest, Interfax, TASS, RBC + фабрики
├── processing/             # LLM, суммаризация
├── utils/                  # datetime, logging, retry
└── infrastructure/         # Telegram-бот

scripts/
├── entity_aliases_data.py      # данные алиасов (редактировать здесь)
├── migrate_entity_aliases.py   # создание таблицы + наполнение
├── merge_entity_aliases.py     # разовое слияние дублей в entities
└── llm_entity_cleanup.py       # LLM-очистка с JSON-ревью
```

## Управление сущностями

Сущности (persons, organizations, locations) нормализуются через таблицу `entity_aliases`.

### Первоначальная настройка
```bash
# Создать таблицу и залить начальные алиасы
python scripts/migrate_entity_aliases.py

# Слить существующие дубли в entities
python scripts/merge_entity_aliases.py --dry-run   # предпросмотр
python scripts/merge_entity_aliases.py
```

### Ручная LLM-очистка
```bash
# Сгенерировать список алиасов/мусора через LLM (сохраняет JSON)
python scripts/llm_entity_cleanup.py

# Проверить JSON, затем применить
python scripts/llm_entity_cleanup.py --apply llm_cleanup_*.json

# Удалить мусорные сущности и заблокировать их в будущем
python scripts/llm_entity_cleanup.py --delete-discards llm_cleanup_*.json

# Слить дубли после применения алиасов
python scripts/merge_entity_aliases.py
```

Автоматическая очистка запускается каждое воскресенье в 03:00 МСК в NER-сервисе.

### Добавление алиасов вручную

Отредактируйте `scripts/entity_aliases_data.py`, затем:
```bash
python scripts/migrate_entity_aliases.py --seed-only
```

## Переменные окружения

| Переменная | Описание | Обязательная |
|------------|----------|:------------:|
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | PostgreSQL | ✓ |
| `DEEPSEEK_API_KEY` | DeepSeek API (суммаризация + NER-очистка) | ✓ |
| `TELEGRAM_BOT_TOKEN` | Токен Telegram-бота | ✓ |
| `PROXY_URL` | host:port:user:pass | |
| `ADMIN_CHAT_ID` | Chat ID для уведомлений об ошибках | |
| `LOG_LEVEL` | DEBUG / INFO / WARNING / ERROR | |

## Тестирование

```bash
pytest tests/ -v                              # все тесты
pytest tests/unit/ -v                         # только unit
pytest tests/ --cov=src --cov-report=html     # отчёт о покрытии
```

## Документация

- [Архитектура проекта](docs/architecture.md)
- [Работа с часовыми поясами](docs/timezone_handling.md)
- [Добавление нового источника](docs/adding_new_source.md)
