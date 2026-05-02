# database

Всё необходимое для запуска и инициализации базы данных проекта.

## Быстрый старт

```bash
cp .env.example .env
# Задайте POSTGRES_PASSWORD в .env
docker compose up -d
```

После старта контейнер автоматически выполнит `init.sql` и создаст всю схему.

## Файлы

| Файл | Назначение |
|------|-----------|
| `docker-compose.yml` | PostgreSQL 18 + pgvector, лимиты: 1GB RAM / 0.5 CPU |
| `.env.example` | Шаблон переменных окружения |
| `init.sql` | Полная схема БД: таблицы, расширения, индексы |
| `pg_hba.conf` | Правила аутентификации (MD5 снаружи, trust локально) |
| `postgresql.conf` | Настройки PostgreSQL (соединения, память, логирование) |
| `scripts/` | Отдельные SQL-файлы по каждой таблице — для справки и ручных миграций |

## Схема

```
sources
  └── raw_articles (source_id → sources.id)
        └── processed_articles (raw_article_id → raw_articles.id)
              └── article_entities (processed_article_id → processed_articles.id)

entities
  └── article_entities (entity_id → entities.id)
```

### Таблицы

- **sources** — источники новостей (RSS, API, сайты)
- **raw_articles** — сырые статьи как есть из источника
- **processed_articles** — очищенный текст, NLP-атрибуты, векторный эмбеддинг
- **entities** — нормализованные сущности (персоны, организации, локации)
- **entity_aliases** — псевдонимы сущностей для нормализации (alias → canonical)
- **article_entities** — связи статья ↔ сущность с контекстом упоминания

Расширения: `pg_trgm` (нечёткий поиск по сущностям), `vector` (эмбеддинги, dim=384).

## scripts/

Порядок применения при ручной инициализации:

1. `sources_table.sql`
2. `entities_table.sql`
3. `entity_aliases_table.sql`
4. `raw_articles_table.sql`
5. `processed_articles_table.sql`
6. `article_entities_table.sql`
7. `summarizations_table.sql`
8. `sources_insert.sql` — начальное наполнение источников
