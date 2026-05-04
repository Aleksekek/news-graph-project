-- Risk scores: оценка риска для конкретной сущности в конкретной статье
-- Связь processed_article → entity → score

CREATE TABLE IF NOT EXISTS risk_scores (
    id BIGSERIAL PRIMARY KEY,
    processed_article_id BIGINT NOT NULL REFERENCES processed_articles(id) ON DELETE CASCADE,
    entity_id BIGINT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,

    score SMALLINT NOT NULL CHECK (score BETWEEN 0 AND 100),
    label VARCHAR(20) NOT NULL CHECK (label IN ('low', 'medium', 'high')),
    -- low: 0-29, medium: 30-69, high: 70-100

    reason TEXT,                   -- Краткое объяснение от LLM (1 предложение)
    action_template TEXT,          -- Рекомендуемое действие: "Проверьте контракты с X"

    model_used VARCHAR(100),
    scored_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(processed_article_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_risk_article ON risk_scores(processed_article_id);
CREATE INDEX IF NOT EXISTS idx_risk_entity ON risk_scores(entity_id);
CREATE INDEX IF NOT EXISTS idx_risk_score ON risk_scores(score DESC);
CREATE INDEX IF NOT EXISTS idx_risk_label ON risk_scores(label);
CREATE INDEX IF NOT EXISTS idx_risk_entity_score ON risk_scores(entity_id, score DESC);
