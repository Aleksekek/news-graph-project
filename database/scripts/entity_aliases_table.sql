CREATE TABLE IF NOT EXISTS entity_aliases (
    id             SERIAL PRIMARY KEY,
    alias_name     TEXT NOT NULL,
    alias_type     TEXT,
    canonical_name TEXT NOT NULL,
    canonical_type TEXT NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_aliases_unique
    ON entity_aliases (alias_name, COALESCE(alias_type, ''));

CREATE INDEX IF NOT EXISTS idx_entity_aliases_lookup
    ON entity_aliases (lower(alias_name));
