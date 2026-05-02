-- Watchlist: пользователь следит за набором сущностей
-- Связь user (telegram_chat_id) → entity

CREATE TABLE IF NOT EXISTS watchlists (
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,                         -- Telegram chat_id пользователя
    entity_id BIGINT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    risk_threshold SMALLINT NOT NULL DEFAULT 70,     -- Порог алерта: 0-100
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(chat_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_watchlists_chat ON watchlists(chat_id);
CREATE INDEX IF NOT EXISTS idx_watchlists_entity ON watchlists(entity_id);
