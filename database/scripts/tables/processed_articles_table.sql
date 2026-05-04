CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE processed_articles (
    id BIGSERIAL PRIMARY KEY,
    raw_article_id BIGINT UNIQUE REFERENCES raw_articles(id) ON DELETE CASCADE,
    
    -- Очищенный контент
    title TEXT NOT NULL,
    text TEXT NOT NULL,
    summary TEXT,
    clean_text_tsvector TSVECTOR,
    
    -- NLP-атрибуты
    topic VARCHAR(100),
    sentiment_score REAL,
    sentiment_label VARCHAR(20),
    embedding vector(384),
    
    -- Флаги этапов обработки: {"ner": true, "sentiment": false, "embedding": false}
    processing_flags JSONB DEFAULT '{}',

    -- Временные метки
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_raw_article FOREIGN KEY(raw_article_id) REFERENCES raw_articles(id)
);

-- Индексы
CREATE INDEX idx_processed_topic ON processed_articles(topic);
CREATE INDEX idx_processed_sentiment ON processed_articles(sentiment_label);
CREATE INDEX idx_processed_published ON processed_articles(published_at);
CREATE INDEX idx_processed_topic_published ON processed_articles(topic, published_at DESC); -- Составной индекс для частых запросов