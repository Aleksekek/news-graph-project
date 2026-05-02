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
