-- Таблица суммаризаций
CREATE TABLE summarizations (
    id BIGSERIAL PRIMARY KEY,
    
    -- Период, за который сделана суммаризация
    period_start TIMESTAMP WITH TIME ZONE NOT NULL,
    period_end TIMESTAMP WITH TIME ZONE NOT NULL,
    period_type VARCHAR(20) NOT NULL CHECK (period_type IN ('hour', 'halfday', 'day')),
    
    -- Данные суммаризации (JSONB — гибко, можно расширять)
    content JSONB NOT NULL,
    -- Структура content:
    -- {
    --   "topics": ["тема1", "тема2", "тема3"],
    --   "summary": "главное за период (2-3 предложения)",
    --   "trend": "ключевой тренд",
    --   "important_events": ["событие1", "событие2"],
    --   "key_articles_ids": [123, 456]  -- опционально, ссылки на raw_articles
    -- }
    
    -- Метаданные
    model_used VARCHAR(50) DEFAULT 'deepseek-v4-flash',
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    cost_usd NUMERIC(10, 6),
    
    -- Технические поля
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_delivered BOOLEAN DEFAULT FALSE,  -- был ли отправлен в Telegram (для ежедневной рассылки)
    
    -- Уникальность: один период не суммаризируем дважды
    UNIQUE(period_start, period_type)
);

-- Индексы для быстрых выборок
CREATE INDEX idx_summarizations_period_start ON summarizations(period_start DESC);
CREATE INDEX idx_summarizations_period_type ON summarizations(period_type);
CREATE INDEX idx_summarizations_created_at ON summarizations(created_at);
CREATE INDEX idx_summarizations_is_delivered ON summarizations(is_delivered);