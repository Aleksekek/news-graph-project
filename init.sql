-- Создаём расширение для fuzzy поиска (из entities_table.sql)
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS vector;


-- Таблица источников (первая, т.к. raw_articles ссылается на неё)
CREATE TABLE sources (
    id SERIAL PRIMARY KEY,           -- Внутренний уникальный ID
    external_id VARCHAR(255),        -- Внешний ID (например, username канала в TG)
    name VARCHAR(255) NOT NULL,      -- Человекочитаемое имя (например, "РИА Новости")
    type VARCHAR(50) NOT NULL,       -- Тип: 'rss', 'telegram', 'website', 'api'
    url TEXT,                        -- Базовый URL или ссылка на канал
    is_active BOOLEAN DEFAULT TRUE,  -- Флаг активности сбора
    last_checked_at TIMESTAMP WITH TIME ZONE, -- Когда последний раз проверяли
    meta_info JSONB                  -- Доп. инфа: описание, язык, категория
);


-- Таблица сущностей (нужна для article_entities)
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


-- Таблица сырых статей (зависит от sources)
CREATE TABLE raw_articles (
    id BIGSERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id) ON DELETE SET NULL,
    
    -- Идентификаторы и ссылки
    original_id VARCHAR(512),        -- ID из источника (если есть, для дедупликации)
    url TEXT UNIQUE,                 -- URL статьи. UNIQUE чтобы не парсить дважды
    
    -- Сырые данные
    raw_title TEXT,                  -- Заголовок "как есть"
    raw_text TEXT,                   -- Текст "как есть" (может быть HTML)
    raw_html TEXT,                   -- Полный HTML (если нужен для перепарсинга)
    media_content JSONB,             -- Ссылки на изображения/видео [{'url':..., 'type':'image'}]
    
    -- Метаданные статьи из источника
    published_at TIMESTAMP WITH TIME ZONE, -- Дата публикации на источнике
    retrieved_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- Когда мы её скачали
    author VARCHAR(255),             -- Автор (если указан)
    language VARCHAR(2) DEFAULT 'ru',   -- Код языка
    
    -- Технические метаданные
    headers JSONB,                   -- HTTP-заголовки ответа (может пригодиться)
    meta_info JSONB,                 -- Метаданные
    status VARCHAR(20) DEFAULT 'raw' -- Статус: 'raw', 'processing', 'failed', 'parsed'
);


-- Таблица обработанных статей (зависит от raw_articles)
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
    
    -- Временные метки
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT fk_raw_article FOREIGN KEY(raw_article_id) REFERENCES raw_articles(id)
);

-- Таблица связей статья-сущности (последняя, зависит от processed_articles и entities)
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

-- Создание индексов
CREATE INDEX idx_entities_name ON entities USING GIN(normalized_name gin_trgm_ops); -- Для нечёткого поиска
CREATE INDEX idx_entities_type ON entities(type);

CREATE INDEX idx_processed_topic ON processed_articles(topic);
CREATE INDEX idx_processed_sentiment ON processed_articles(sentiment_label);
CREATE INDEX idx_processed_published ON processed_articles(published_at);
CREATE INDEX idx_processed_topic_published ON processed_articles(topic, published_at DESC); -- Составной индекс для частых запросов

CREATE INDEX idx_ae_entity ON article_entities(entity_id);
CREATE INDEX idx_ae_article ON article_entities(processed_article_id);


-- Заполняем источники данными (из sources_insert.sql)
INSERT INTO sources (name, type, external_id, url, meta_info)
VALUES (
  'Тинькофф Пульс (TInvest)',
  'api',
  'tinvest_pulse',
  'https://www.tinvest.ru/pulse/',
  '{"sector": "financial_social", "language": "ru", "note": "Парсер через API/скрапинг TInvest, тикер-ориентированный"}'
)
RETURNING id; -- id 1


INSERT INTO sources (name, type, external_id, url, meta_info)
VALUES (
  'Лента ру',
  'rss+parsing',
  'lenta_ru',
  'https://lenta.ru/',
  '{"sector": "politics", "language": "ru", "note": "Парсер через RSS + скрапинг по ссылкам из RSS"}'
)
RETURNING id; -- id 2