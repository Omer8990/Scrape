CREATE TABLE IF NOT EXISTS content_insights (
    id SERIAL PRIMARY KEY,
    insight_type VARCHAR(50),
    title TEXT,
    description TEXT,
    sources TEXT[],
    related_content_ids TEXT[],
    score FLOAT,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);