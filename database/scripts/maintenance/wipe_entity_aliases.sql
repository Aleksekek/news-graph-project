-- Очистка таблицы entity_aliases.
-- При переходе с Natasha на LLM-NER алиасы не нужны: LLM сам нормализует упоминания
-- к каноническому имени (см. llm_client.py — поле "canonical" в ответе).
--
-- Что происходит:
--   TRUNCATE entity_aliases RESTART IDENTITY → таблица пустая, id-счётчик в 1.
--   FK-зависимостей у этой таблицы нет (на неё никто не ссылается), поэтому CASCADE не нужен.
--
-- Запуск:
--   docker-compose exec postgres psql -U news_user -d news_db -f /path/to/wipe_entity_aliases.sql

BEGIN;

TRUNCATE entity_aliases RESTART IDENTITY;

SELECT 'entity_aliases' AS table_name, COUNT(*) AS rows FROM entity_aliases;

COMMIT;
