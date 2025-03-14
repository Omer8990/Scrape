# dags/news_etl_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

import sys
import os

# Add project directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import our custom modules
from etl.extractor.api_extractor import ScienceNewsExtractor, CryptoMarketExtractor
from etl.transformer.spark_transformer import NewsTransformer
from etl.extractor.reddit_extractor import RedditExtractor
from etl.loader.telegram_bot import NewsBot

# Define default arguments
default_args = {
    'owner': 'omer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'news_etl',
    'user': 'postgres',
    'password': 'your_password'  # In production, use Airflow Variables or Connections
}

TELEGRAM_TOKEN = "your_telegram_bot_token"  # In production, use Airflow Variables
CHAT_ID = "your_chat_id"  # Your Telegram chat ID

# Create DAG
dag = DAG(
    'news_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for personalized news updates',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'news'],
)


# Task functions
def extract_science_news(**kwargs):
    """Extract science news from API"""
    extractor = ScienceNewsExtractor(
        base_url="https://newsapi.org/v2",
        api_key="your_news_api_key"  # In production, use Airflow Variables
    )
    news_items = extractor.extract()

    # Push to XCom for next task
    kwargs['ti'].xcom_push(key='science_news', value=news_items)
    return len(news_items)


def extract_crypto_news(**kwargs):
    """Extract cryptocurrency updates"""
    extractor = CryptoMarketExtractor(
        base_url="https://pro-api.coinmarketcap.com/v1",
        api_key="your_coinmarketcap_api_key"  # In production, use Airflow Variables
    )
    crypto_updates = extractor.extract()

    # Push to XCom for next task
    kwargs['ti'].xcom_push(key='crypto_updates', value=crypto_updates)
    return len(crypto_updates)


def extract_reddit_content(**kwargs):
    """Extract content from various Reddit subreddits"""

    # Initialize Reddit extractor
    extractor = RedditExtractor(
        client_id="your_reddit_client_id",  # In production, use Airflow Variables
        client_secret="your_reddit_client_secret"  # In production, use Airflow Variables
    )

    # Extract from different categories
    science_posts = extractor.extract_science()
    crypto_posts = extractor.extract_crypto()
    tech_posts = extractor.extract_tech()

    # Combine all posts
    all_posts = science_posts + crypto_posts + tech_posts

    # Push to XCom for next task
    kwargs['ti'].xcom_push(key='reddit_posts', value=all_posts)
    return len(all_posts)


# Add this task to the DAG
extract_reddit = PythonOperator(
    task_id='extract_reddit_content',
    python_callable=extract_reddit_content,
    provide_context=True,
    dag=dag,
)


# Modify the load_raw_news_to_db function to include Reddit posts
def load_raw_news_to_db(**kwargs):
    """Load raw news data to PostgreSQL"""

    ti = kwargs['ti']
    science_news = ti.xcom_pull(task_ids='extract_science_news', key='science_news')
    crypto_updates = ti.xcom_pull(task_ids='extract_crypto_news', key='crypto_updates')
    reddit_posts = ti.xcom_pull(task_ids='extract_reddit_content', key='reddit_posts')

    # Combine all news sources
    all_news = science_news + crypto_updates + reddit_posts


def load_raw_news_to_db(**kwargs):
    """Load raw news data to PostgreSQL"""
    from psycopg2 import extras
    import psycopg2
    import json

    ti = kwargs['ti']
    science_news = ti.xcom_pull(task_ids='extract_science_news', key='science_news')
    crypto_updates = ti.xcom_pull(task_ids='extract_crypto_news', key='crypto_updates')

    all_news = science_news + crypto_updates

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # Use batch insert with psycopg2 extras
            insert_query = """
                INSERT INTO news_etl.raw_news 
                (id, title, source, category, url, published_date, summary, raw_data)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
            """

            # Prepare data tuples
            template = '(%s, %s, %s, %s, %s, %s, %s, %s)'
            values = [(
                item['id'],
                item['title'],
                item['source'],
                item['category'],
                item['url'],
                item['published_date'],
                item['summary'],
                json.dumps(item['raw_data'])
            ) for item in all_news]

            extras.execute_values(cur, insert_query, values, template)
            conn.commit()

            return len(values)
    finally:
        conn.close()


def transform_news_data(**kwargs):
    """Transform news data using Spark"""
    from pyspark.sql import SparkSession

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("NewsETLTransform") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Read raw news from PostgreSQL
    jdbc_url = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

    raw_news_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "news_etl.raw_news") \
        .option("user", DB_CONFIG['user']) \
        .option("password", DB_CONFIG['password']) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Apply transformations
    transformer = NewsTransformer(spark)
    processed_df = transformer.transform(raw_news_df)

    # Select only necessary columns for processed_news table
    processed_news_df = processed_df.select(
        "id", "title_clean", "summary_clean", "sentiment",
        "science_subtopic", "interest_score", "relevance_score"
    )

    # Write transformed data back to PostgreSQL
    processed_news_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "news_etl.processed_news") \
        .option("user", DB_CONFIG['user']) \
        .option("password", DB_CONFIG['password']) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    return processed_df.count()


def send_telegram_notifications(**kwargs):
    """Send high-relevance news to Telegram"""
    import psycopg2

    # Initialize bot
    news_bot = NewsBot(token=TELEGRAM_TOKEN, db_config=DB_CONFIG)

    # Get high-relevance news from today
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    r.id, r.title, r.source, r.category, r.url, 
                    r.published_date, r.summary, p.relevance_score
                FROM 
                    news_FROM 
                    news_etl.raw_news r
                JOIN 
                    news_etl.processed_news p ON r.id = p.id
                LEFT JOIN
                    news_etl.notifications n ON r.id = n.news_id
                WHERE 
                    p.relevance_score >= 0.7
                    AND r.published_date >= %s
                    AND n.id IS NULL  -- Only get news not yet sent
                ORDER BY 
                    p.relevance_score DESC, r.published_date DESC
                LIMIT 5
            """, (datetime.now() - timedelta(days=1),))

            columns = [desc[0] for desc in cur.description]
            news_items = [dict(zip(columns, row)) for row in cur.fetchall()]

        # Send notifications
        sent_count = 0
        for item in news_items:
            if news_bot.send_notification(CHAT_ID, item):
                sent_count += 1

        return sent_count
    finally:
        conn.close()


# Define tasks
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',  # Set up this connection in Airflow
    sql='sql/create_tables.sql',  # Point to the SQL file we created earlier
    dag=dag,
)

extract_science = PythonOperator(
    task_id='extract_science_news',
    python_callable=extract_science_news,
    provide_context=True,
    dag=dag,
)

extract_crypto = PythonOperator(
    task_id='extract_crypto_news',
    python_callable=extract_crypto_news,
    provide_context=True,
    dag=dag,
)

load_raw_data = PythonOperator(
    task_id='load_raw_news',
    python_callable=load_raw_news_to_db,
    provide_context=True,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_news_data',
    python_callable=transform_news_data,
    provide_context=True,
    dag=dag,
)

send_notifications = PythonOperator(
    task_id='send_telegram_notifications',
    python_callable=send_telegram_notifications,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
create_tables >> [extract_science, extract_crypto, extract_reddit] >> load_raw_data >> transform_data >> send_notifications
