-- Сброс auto-increment счётчиков (sequences) до значения, согласованного с MAX(id) таблицы.
-- Полезно когда:
--   - INSERT'ы выполнялись повторно и нагнали лишние id (как со sources)
--   - Строки удалялись через DBeaver и счётчик ушёл вперёд
--   - После TRUNCATE без RESTART IDENTITY (но обычно RESTART делается автоматически)
--
-- Логика setval(seq, value, false):
--   - Следующий nextval() вернёт ровно value
--   - Поэтому передаём COALESCE(MAX(id), 0) + 1
--   - На пустой таблице → next id = 1; на таблице с MAX=5 → next id = 6
--
-- pg_get_serial_sequence(table, column) автоматически находит имя последовательности
-- для SERIAL/BIGSERIAL колонок (обычно "<table>_<column>_seq").

BEGIN;

-- ── sources (упомянутый случай: задвоен INSERT'ами) ─────────────────────────
SELECT setval(
    pg_get_serial_sequence('sources', 'id'),
    COALESCE((SELECT MAX(id) FROM sources), 0) + 1,
    false
);

-- ── raw_articles ────────────────────────────────────────────────────────────
SELECT setval(
    pg_get_serial_sequence('raw_articles', 'id'),
    COALESCE((SELECT MAX(id) FROM raw_articles), 0) + 1,
    false
);

-- ── NER-таблицы (после TRUNCATE RESTART IDENTITY вызов лишний, но безопасный) ─
SELECT setval(
    pg_get_serial_sequence('processed_articles', 'id'),
    COALESCE((SELECT MAX(id) FROM processed_articles), 0) + 1,
    false
);
SELECT setval(
    pg_get_serial_sequence('entities', 'id'),
    COALESCE((SELECT MAX(id) FROM entities), 0) + 1,
    false
);
SELECT setval(
    pg_get_serial_sequence('article_entities', 'id'),
    COALESCE((SELECT MAX(id) FROM article_entities), 0) + 1,
    false
);
SELECT setval(
    pg_get_serial_sequence('entity_aliases', 'id'),
    COALESCE((SELECT MAX(id) FROM entity_aliases), 0) + 1,
    false
);

-- ── summarizations ──────────────────────────────────────────────────────────
SELECT setval(
    pg_get_serial_sequence('summarizations', 'id'),
    COALESCE((SELECT MAX(id) FROM summarizations), 0) + 1,
    false
);

-- ── telegram_subscribers ────────────────────────────────────────────────────
SELECT setval(
    pg_get_serial_sequence('telegram_subscribers', 'id'),
    COALESCE((SELECT MAX(id) FROM telegram_subscribers), 0) + 1,
    false
);

-- Проверка: какие значения теперь будут у следующих nextval() для каждой таблицы.
-- is_called берём напрямую из sequence-relation — в системном view pg_sequences
-- этой колонки нет, она только у самой последовательности.
-- После setval(..., false) ожидаем: last_value=N, is_called=false → следующий
-- nextval() вернёт ровно N (а не N+1).
SELECT 'sources'              AS table_name, last_value, is_called FROM sources_id_seq
UNION ALL
SELECT 'raw_articles',         last_value, is_called FROM raw_articles_id_seq
UNION ALL
SELECT 'processed_articles',   last_value, is_called FROM processed_articles_id_seq
UNION ALL
SELECT 'entities',             last_value, is_called FROM entities_id_seq
UNION ALL
SELECT 'article_entities',     last_value, is_called FROM article_entities_id_seq
UNION ALL
SELECT 'entity_aliases',       last_value, is_called FROM entity_aliases_id_seq
UNION ALL
SELECT 'summarizations',       last_value, is_called FROM summarizations_id_seq
UNION ALL
SELECT 'telegram_subscribers', last_value, is_called FROM telegram_subscribers_id_seq;

COMMIT;
