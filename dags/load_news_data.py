import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv


load_dotenv()


TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_insights_to_telegram',
    default_args=default_args,
    description='Process unified content and send insights to Telegram',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Generate insights with Spark
generate_insights = SparkSubmitOperator(
    task_id='generate_content_insights',
    application='/opt/spark/work-dir/generate_insight.py',
    conn_id='spark_default',
    name='generate_content_insights',
    conf={
        'spark.master': 'local[*]',
        'spark.dynamicAllocation.enabled': 'false',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '2g',
        "spark.env.JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-arm64",
        "spark.jars": "/opt/airflow/jars/postgresql-42.3.1.jar"  # Changed path
    },
    driver_class_path='/opt/airflow/jars/postgresql-42.3.1.jar',  # Changed path

    dag=dag,
)

# Store insights in PostgreSQL
create_insights_table = PostgresOperator(
    task_id='create_insights_table',
    postgres_conn_id='postgres_default',
    sql='sql/create_transform_tables.sql',
    dag=dag,
)


# Python function to send Telegram messages
def send_telegram_insights(**kwargs):
    import telegram
    import pandas as pd
    from sqlalchemy import create_engine

    # Connect to PostgreSQL

    # "jdbc:postgresql://postgres:5432/airflow"
    engine = create_engine('postgresql://airflow:airflow@scrape-postgres-1:5432/airflow')

    # Get today's insights
    insights_df = pd.read_sql(
        "SELECT * FROM content_insights WHERE generated_at >= CURRENT_DATE ORDER BY score DESC LIMIT 10",
        engine
    )

    # Initialize Telegram bot
    bot = telegram.Bot(token=TELEGRAM_TOKEN)
    chat_id = TELEGRAM_CHAT_ID

    # Format and send insights
    bot.send_message(chat_id=chat_id, text="🤖 *JARVIS DAILY INSIGHTS* 🤖")

    for _, insight in insights_df.iterrows():
        message = f"""
*{insight['insight_type'].upper()}*: {insight['title']}

{insight['description']}

Sources: {', '.join(insight['sources'][:3])}
Relevance Score: {insight['score']:.2f}
        """
        bot.send_message(chat_id=chat_id, text=message)

    # Send summary
    bot.send_message(
        chat_id=chat_id,
        text=f"End of today's insights. Would you like to explore any topic in more detail?",
    )


# Task to send insights to Telegram
telegram_task = PythonOperator(
    task_id='send_telegram_insights',
    python_callable=send_telegram_insights,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
create_insights_table >> generate_insights >> telegram_task
