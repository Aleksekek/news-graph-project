-- Telegram подписчики: замена in-memory dict в боте
-- Хранит настройки ежедневной рассылки

CREATE TABLE IF NOT EXISTS telegram_subscribers (
    chat_id BIGINT PRIMARY KEY,
    digest_time TIME NOT NULL DEFAULT '20:00',
    timezone VARCHAR(50) NOT NULL DEFAULT 'Europe/Moscow',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    subscribed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_sent_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_subscribers_active ON telegram_subscribers(is_active)
    WHERE is_active = TRUE;
