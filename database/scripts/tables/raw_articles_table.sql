CREATE TABLE raw_articles (
    id BIGSERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES sources(id) ON DELETE SET NULL,
    
    -- Идентификаторы и ссылки
    original_id VARCHAR(512),        -- ID из источника (если есть, для дедупликации)
    url TEXT UNIQUE,                 -- URL статьи. UNIQUE чтобы не парсить дважды
    canonical_url TEXT,              -- Каноническая ссылка (если отличается от url)
    
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
    status VARCHAR(20) DEFAULT 'raw' -- Статус: 'raw', 'processing', 'failed', 'parsed'
);