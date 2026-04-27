
# Архитектура News Graph Project

## Общая схема

```
[Lenta.ru] ──┐
             ├──► [Parser Service] ──► [PostgreSQL] ──► [Summarizer] ──► [Telegram Bot]
[TInvest] ───┘                              │
                                            │
                                      [LLM API]
                                         (DeepSeek)
```

## Принципы

1. **Асинхронность** — весь I/O код асинхронный (asyncpg, aiohttp)
2. **Единое время** — все даты хранятся в MSK (UTC+3)
3. **Чистая архитектура** — разделение на слои:
   - `core` — модели и интерфейсы
   - `database` — инфраструктура БД
   - `parsers` — адаптеры внешних источников
   - `app` — use cases и оркестрация

## Компоненты

### Parser Service
- Запускается по расписанию (APScheduler)
- Читает конфиг из `config/schedule_config.yaml`
- Поддерживаемые источники: Lenta.ru, TInvest Pulse

### Summarizer Service
- Генерирует часовые суммаризации
- Агрегирует дневные сводки из часовых
- Использует DeepSeek API (cost ~$0.0001-0.001 за час)

### Database Layer
- `DatabasePoolManager` — глобальный пул соединений
- `ArticleRepository` — CRUD для raw_articles
- `SummaryRepository` — работа с суммаризациями

## Потоки данных

1. **Парсинг** → `ParsedItem` → конвертер → `ArticleForDB` → БД
2. **Суммаризация** → выборка статей → LLM → сохранение
3. **Бот** → запросы к репозиториям → ответ пользователю

## Ключевые решения

- **Отказ от SQLAlchemy** — прямой asyncpg для максимальной производительности
- **Pydantic v2** — строгая валидация данных
- **Минимум зависимостей** — каждый сервис имеет свой requirements.txt
- **Markdown v2 в Telegram** — безопасное экранирование через `telegram_helpers.py`

## Расширение

### Добавление нового источника

1. Создать парсер в `src/parsers/newsource/parser.py`
2. Создать конвертер в `src/parsers/newsource/converter.py`
3. Зарегистрировать в `ParserFactory` и `ConverterFactory`
4. Добавить в `constants.py` ID источника
5. Написать тесты

### Добавление нового типа суммаризации

1. Расширить `SummaryRepository`
2. Добавить метод в `SummarizationService`
3. Добавить задачу в `summarizer.py`