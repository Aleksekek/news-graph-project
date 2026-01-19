CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE entities (
    id BIGSERIAL PRIMARY KEY,
    normalized_name VARCHAR(500) NOT NULL, -- Нормализованное имя (например, "Сбербанк")
    type VARCHAR(50) NOT NULL,             -- 'person', 'organization', 'location', 'product'
    original_name VARCHAR(500),            -- Имя как оно встретилось в тексте впервые ("Сбер")
    wiki_link TEXT,                        -- Ссылка на Википедию или другой источник истины
    external_ids JSONB,                    -- Внешние ID: {'inn': '7707083893', 'wikidata': 'Q205012'}
    meta JSONB,                            -- Дополнительная информация: страна, сектор экономики
    UNIQUE(normalized_name, type)          -- Одна и та же сущность — одна запись
);

CREATE INDEX idx_entities_name ON entities USING GIN(normalized_name gin_trgm_ops); -- Для нечёткого поиска
CREATE INDEX idx_entities_type ON entities(type);