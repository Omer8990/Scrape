from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
import os


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create transform DAG
dag = DAG(
    'transform_news_data',
    default_args=default_args,
    description='Transform extracted news data using Spark',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Wait for the extract DAG to complete
# wait_for_extract = ExternalTaskSensor(
#     task_id='wait_for_extract',
#     external_dag_id='extract_news_data_v2',
#     external_task_id=None,  # Wait for the entire DAG to complete
#     timeout=600,
#     poke_interval=60,
#     dag=dag,
# )

create_transform_tables = PostgresOperator(
    task_id='create_transform_tables',
    postgres_conn_id='postgres_default',
    sql='sql/create_transform_tables.sql',
    dag=dag,
)

# Transform news articles
transform_news_articles = SparkSubmitOperator(
    task_id='transform_news_articles',
    application='/opt/spark/work-dir/news_transformations.py',  # Remove "spark_scripts/" from path
    conn_id='spark_default',
    application_args=['news_articles'],
    name='transform_news_articles_job',

    executor_cores=2,
    executor_memory='2g',
    num_executors=2,
    conf={
        "spark.env.JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-arm64",
        "spark.jars": "/opt/airflow/jars/postgresql-42.3.1.jar"  # Changed path
    },
    driver_class_path='/opt/airflow/jars/postgresql-42.3.1.jar',  # Changed path
    dag=dag,
)

# Transform reddit posts
transform_reddit_posts = SparkSubmitOperator(
    task_id='transform_reddit_posts',
    application='/opt/spark/work-dir/news_transformations.py',
    conn_id='spark_default',
    application_args=['reddit_posts'],
    name='transform_reddit_posts_job',
    executor_cores=2,
    executor_memory='2g',
    num_executors=2,
    conf={
        "spark.env.JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-arm64",
        "spark.jars": "/opt/airflow/jars/postgresql-42.3.1.jar"  # Changed path
    },
    driver_class_path='/opt/airflow/jars/postgresql-42.3.1.jar',  # Changed path
    dag=dag,
)

# Transform scholarly articles
transform_scholarly_articles = SparkSubmitOperator(
    task_id='transform_scholarly_articles',
    application='/opt/spark/work-dir/news_transformations.py',  # Remove "spark_scripts/" from path
    conn_id='spark_default',
    application_args=['scholarly_articles'],
    name='transform_scholarly_articles_job',
    executor_cores=2,
    executor_memory='2g',
    num_executors=2,
    conf={
        "spark.env.JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-arm64",
        "spark.jars": "/opt/airflow/jars/postgresql-42.3.1.jar"  # Changed path
    },
    driver_class_path='/opt/airflow/jars/postgresql-42.3.1.jar',  # Changed path
    dag=dag,
)

# Task to merge all transformed data
merge_transformed_data = SparkSubmitOperator(
    task_id='merge_transformed_data',
    application='/opt/spark/work-dir/news_transformations.py',  # Remove "spark_scripts/" from path
    conn_id='spark_default',
    application_args=['merge_all'],
    name='merge_transformed_data_job',
    executor_cores=2,
    executor_memory='2g',
    num_executors=2,
    conf={
        "spark.env.JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-arm64",
        "spark.jars": "/opt/airflow/jars/postgresql-42.3.1.jar"  # Changed path
    },
    driver_class_path='/opt/airflow/jars/postgresql-42.3.1.jar',  # Changed path
    dag=dag,
)

# Define task dependencies
create_transform_tables >> [transform_news_articles, transform_reddit_posts, transform_scholarly_articles] >> merge_transformed_data
