# Архитектура News Graph Project

## Общая схема

```
[Lenta.ru]  ──┐
[TInvest]   ──┤
[Interfax]  ──┼──► [Parser] ──► raw_articles ──► [Summarizer] ──► summarizations ──► [Bot]
[TASS]      ──┤                      │
[RBC]       ──┘                      ▼
                              [NER Processor] ──► entities + article_entities
                                     │
                              entity_aliases ◄─── [LLM Cleanup / еженед.]
```

## Сервисы

### parser
- Парсит 5 источников (Lenta, TInvest, Interfax, TASS, RBC) по расписанию (APScheduler, [src/config/schedules.py](../src/config/schedules.py); опциональный YAML override в `config/schedule_config.yaml`)
- Записывает в `raw_articles` со статусом `raw`. Дедупликация по `url` + `ON CONFLICT DO NOTHING` для гонок
- Расписание: каждые 30 мин на источник (TInvest — каждые 15 мин), staggered по 5 мин
- Свежие статьи: где есть RSS-with-full-text — берём полный текст из фида (RBC `rbc_news_full-text`, TASS `yandex_full-text`); иначе — индивидуальный fetch HTML
- Архив: per-source стратегии (Lenta paginated `/news/YYYY/MM/DD/`, RBC AJAX search API, Interfax search, TASS `sitemap_news{N}.xml`)

### summarizer
- Каждый час берёт статьи за прошедший час → DeepSeek → `summarizations` (period_type=`hour`)
- В 09:05 агрегирует дневную сводку из часовых → period_type=`day`

### ner
- Каждые 30 мин забирает пачку `raw_articles` и прогоняет через Natasha NER
- Извлечённые сущности записываются в `entities` + `article_entities`
- При записи автоматически резолвит алиасы через `entity_aliases`
- Каждое воскресенье в 03:00 МСК запускает LLM-очистку (`entity_cleanup.py`)

### bot
- Telegram-бот, отвечает на запросы пользователей
- Читает из `raw_articles`, `summarizations`

## Слои

```
┌───────────────────────────────────────┐
│  services/{parser,summarizer,ner,bot} │  точки входа, APScheduler
├───────────────────────────────────────┤
│  src/app/                             │  use cases: parse_source, ner_processor,
│                                       │  summarizer, entity_cleanup
├───────────────────────────────────────┤
│  src/database/repositories/           │  CRUD: Article, ProcessedArticle,
│                                       │  Entity, ArticleEntity, Summary
├───────────────────────────────────────┤
│  src/core/                            │  модели (Pydantic v2), константы
└───────────────────────────────────────┘
```

## Принципы

- **Асинхронность** — весь I/O код через asyncio (asyncpg, aiohttp)
- **Единое время** — все даты хранятся в MSK naive datetime (см. [timezone_handling.md](timezone_handling.md))
- **Минимум зависимостей** — у каждого сервиса свой `requirements.txt`
- **Прямой asyncpg** — без ORM, для максимальной производительности

## База данных (ключевые таблицы)

| Таблица | Описание |
|---------|----------|
| `raw_articles` | Сырые статьи из источников |
| `processed_articles` | Обработанные статьи (NER-флаги) |
| `entities` | Уникальные именованные сущности (normalized_name, type) |
| `article_entities` | Связь статья↔сущность (count, importance_score) |
| `entity_aliases` | Нормализация: алиас → каноническое имя |
| `summarizations` | Часовые и дневные сводки |
| `watchlists` | **schema only** — пользовательские списки сущностей для мониторинга |
| `risk_scores` | **schema only** — риск-оценки статей по сущностям |

## NER engine (Natasha vs LLM)

Поддерживается два движка через фабрику `src/processing/ner/factory.py`:

| | `natasha` (default) | `llm` |
|---|---|---|
| Реализация | [NatashaClient](../src/processing/ner/natasha_client.py) | [LLMNERClient](../src/processing/ner/llm_client.py) (DeepSeek API) |
| Async | sync | async |
| Канонизация | через `entity_aliases` (post-extract) | at-extract-time |
| Расход | бесплатно | ~$0.0001 / статья |
| Типы | person/organization/location | + event |
| Importance | по позиции в тексте (1.0/0.7/0.3) | LLM решает (subject/key/mention) |

Переключение через `NER_ENGINE` в `.env`. Размер и параллелизм батча — `NER_BATCH_SIZE` и `NER_BATCH_CONCURRENCY`.

## Нормализация сущностей (legacy: entity_aliases)

Используется только в режиме `NER_ENGINE=natasha`. После миграции на LLM становится не нужна.

Таблица `entity_aliases` хранит маппинг `(alias_name, alias_type) → (canonical_name, canonical_type)`.
`EntityRepository.upsert()` ищет алиас и подставляет каноническое имя/тип; `canonical_type='discard'`
полностью отбрасывает сущность.

CLI-инструменты алиасной workflow вынесены в [`scripts/legacy/`](../scripts/legacy/):

```
scripts/legacy/
├── entity_aliases_data.py      # seed-данные алиасов
├── migrate_entity_aliases.py   # создание/наполнение entity_aliases
├── merge_entity_aliases.py     # слияние дублей в entities
├── llm_entity_cleanup.py       # ручной LLM-cleanup с JSON-ревью
└── ner_diagnostic.py           # диагностика бакетов ошибок Natasha
```

Автоматический weekly cleanup ([src/app/entity_cleanup.py](../src/app/entity_cleanup.py))
крутится в NER-сервисе по воскресеньям 03:00 МСК (Natasha-режим).

## Потоки данных

1. **Парсинг** → `ParsedItem` → конвертер → `raw_articles`
2. **NER** (Natasha): `raw_articles` → Natasha → `processed_articles` + `entities` + `article_entities`,
   с alias-lookup на upsert.
   **NER** (LLM): то же, но без alias-lookup — DeepSeek канонизирует сразу.
3. **Суммаризация** → `raw_articles` → DeepSeek → `summarizations`
4. **Бот** → запросы к репозиториям → ответ пользователю

## Расширение

### Добавление нового источника

1. Создать парсер в `src/parsers/newsource/parser.py`
2. Создать конвертер в `src/parsers/newsource/converter.py`
3. Зарегистрировать в `ParserFactory` и `ConverterFactory`
4. Добавить ID в `src/core/constants.py`
5. Написать тесты

Подробнее: [adding_new_source.md](adding_new_source.md)

### Добавление алиасов сущностей (legacy / Natasha-режим)

Отредактировать `scripts/legacy/entity_aliases_data.py`, затем:
```bash
python scripts/legacy/migrate_entity_aliases.py --seed-only
```
