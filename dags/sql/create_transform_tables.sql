-- Table for transformed news articles
CREATE TABLE IF NOT EXISTS transformed_news_articles (
    id SERIAL PRIMARY KEY,
    source_normalized VARCHAR(255),
    category_normalized VARCHAR(255),
    title TEXT,
    clean_title TEXT,
    content TEXT,
    clean_content TEXT,
    title_tokens TEXT[],
    content_tokens TEXT[],
    content_length INTEGER,
    url TEXT,
    published_date TIMESTAMP,
    published_date_normalized DATE,
    extracted_date TIMESTAMP,
    sentiment_score FLOAT,
    transformation_date TIMESTAMP
);

-- Table for transformed reddit posts
CREATE TABLE IF NOT EXISTS transformed_reddit_posts (
    id SERIAL PRIMARY KEY,
    subreddit VARCHAR(255),
    subreddit_normalized VARCHAR(255),
    title TEXT,
    clean_title TEXT,
    title_tokens TEXT[],
    author VARCHAR(255),
    url TEXT,
    score INTEGER,
    popularity_level VARCHAR(50),
    created_utc TIMESTAMP,
    created_date DATE,
    extracted_date TIMESTAMP,
    sentiment_score FLOAT,
    transformation_date TIMESTAMP
);

-- Table for transformed scholarly articles
CREATE TABLE IF NOT EXISTS transformed_scholarly_articles (
    id SERIAL PRIMARY KEY,
    source VARCHAR(255),
    source_normalized VARCHAR(255),
    category VARCHAR(255),
    category_normalized VARCHAR(255),
    title TEXT,
    clean_title TEXT,
    title_tokens TEXT[],
    authors TEXT,
    author_count INTEGER,
    abstract TEXT,
    clean_abstract TEXT,
    abstract_tokens TEXT[],
    abstract_length INTEGER,
    url TEXT,
    published_date TIMESTAMP,
    published_date_normalized DATE,
    extracted_date TIMESTAMP,
    sentiment_score FLOAT,
    transformation_date TIMESTAMP
);

-- Table for unified content from all sources
CREATE TABLE IF NOT EXISTS unified_content (
    unified_id VARCHAR(255) PRIMARY KEY,
    id INTEGER,
    source VARCHAR(255),
    category VARCHAR(255),
    title TEXT,
    content TEXT,
    url TEXT,
    published_date DATE,
    sentiment_score FLOAT,
    content_type VARCHAR(50),
    processed_timestamp TIMESTAMP
);
