CREATE TABLE article_entities (
    id BIGSERIAL PRIMARY KEY,
    processed_article_id BIGINT NOT NULL REFERENCES processed_articles(id) ON DELETE CASCADE,
    entity_id BIGINT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    
    -- Контекст упоминания
    count INTEGER DEFAULT 1,                 -- Сколько раз упомянута в статье
    importance_score REAL,                   -- Оценка важности сущности в статье (например, на основе позиции в тексте)
    context_snippet TEXT,                    -- Фрагмент текста, где встречается сущность
    sentiment_in_context REAL,               -- Тональность именно в этом контексте

    UNIQUE(processed_article_id, entity_id)  -- Чтобы не дублировать записи
);

CREATE INDEX idx_ae_entity ON article_entities(entity_id);
CREATE INDEX idx_ae_article ON article_entities(processed_article_id);