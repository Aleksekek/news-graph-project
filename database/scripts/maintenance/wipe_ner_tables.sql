-- ВНИМАНИЕ: УНИЧТОЖАЕТ ВСЕ NER-РЕЗУЛЬТАТЫ.
-- Применяется ОДНОКРАТНО перед перепрогоном NER (например, при переходе с Natasha на LLM).
--
-- Что происходит:
--   1. TRUNCATE article_entities, entities, processed_articles → таблицы очищены, id-счётчики сброшены.
--      CASCADE автоматически разрывает FK-зависимости (если бы были другие таблицы со ссылками).
--   2. raw_articles.status сбрасывается обратно в 'raw' для всех processed/failed
--      → NER-сервис подхватит их и обработает заново.
--
-- Что НЕ затрагивается:
--   - raw_articles (только status переустанавливается; сам контент сохраняется)
--   - sources, summarizations, telegram_subscribers
--   - entity_aliases — оставляется по умолчанию (там seed-данные/ручные override).
--     Если хочешь полностью очистить — раскомментируй соответствующую строку.
--
-- Запуск:
--   docker-compose exec postgres psql -U news_user -d news_db -f /path/to/wipe_ner_tables.sql

BEGIN;

-- 1. Очистка NER-таблиц (id-счётчики уходят в 1)
TRUNCATE article_entities, entities, processed_articles RESTART IDENTITY CASCADE;

-- 2. Опционально — wipe алиасов. Раскомментируй если хочешь чистую таблицу.
-- TRUNCATE entity_aliases RESTART IDENTITY;

-- 3. Возвращаем raw_articles в pending state
UPDATE raw_articles
SET    status = 'raw'
WHERE  status IN ('processed', 'failed', 'processing');

-- Информация для проверки в DBeaver
SELECT 'raw_articles' AS table_name, status, COUNT(*) AS rows FROM raw_articles GROUP BY status
UNION ALL
SELECT 'entities', '-', COUNT(*) FROM entities
UNION ALL
SELECT 'article_entities', '-', COUNT(*) FROM article_entities
UNION ALL
SELECT 'processed_articles', '-', COUNT(*) FROM processed_articles
UNION ALL
SELECT 'entity_aliases', '-', COUNT(*) FROM entity_aliases;

COMMIT;
