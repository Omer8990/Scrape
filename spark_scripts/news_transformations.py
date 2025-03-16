# spark_scripts/news_transformations.py
"""
News data transformation script using PySpark.
This script performs transformations on extracted news data.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.connect.functions import lit
from pyspark.sql.functions import col, to_date, to_timestamp, regexp_replace, lower, trim
from pyspark.sql.functions import udf, explode, split, length, when, current_timestamp
from pyspark.sql.types import StringType, ArrayType, IntegerType, size
import pyspark.sql.functions as F
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NewsDataTransformation") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar") \
    .getOrCreate()

# Download NLTK resources
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('vader_lexicon')

# Database connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
connection_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}


# Define UDFs (User Defined Functions)
def clean_text(text):
    if text is None:
        return None
    # Remove special characters and numbers
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    # Convert to lowercase and remove extra spaces
    text = text.lower().strip()
    return text


clean_text_udf = udf(clean_text, StringType())


def tokenize_text(text):
    if text is None:
        return []
    tokens = word_tokenize(text)
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word.lower() not in stop_words and len(word) > 2]
    return filtered_tokens


tokenize_udf = udf(tokenize_text, ArrayType(StringType()))


def get_sentiment(text):
    if text is None:
        return None
    sia = SentimentIntensityAnalyzer()
    sentiment = sia.polarity_scores(text)
    return sentiment['compound']


sentiment_udf = udf(get_sentiment, IntegerType())


def transform_news_articles():
    """Transform the news articles data from the database"""
    # Read data from PostgreSQL
    df = spark.read.jdbc(
        url=jdbc_url,
        table="news_articles",
        properties=connection_properties
    )

    # Data validation and cleaning
    df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
    df = df.filter(col("content").isNotNull() & (length(col("content")) > 0))

    # Transform data
    transformed_df = df \
        .withColumn("clean_title", clean_text_udf(col("title"))) \
        .withColumn("clean_content", clean_text_udf(col("content"))) \
        .withColumn("title_tokens", tokenize_udf(col("clean_title"))) \
        .withColumn("content_tokens", tokenize_udf(col("clean_content"))) \
        .withColumn("content_length", length(col("content"))) \
        .withColumn("sentiment_score", sentiment_udf(col("content"))) \
        .withColumn("source_normalized", lower(trim(col("source")))) \
        .withColumn("category_normalized", lower(trim(col("category")))) \
        .withColumn("published_date_normalized", to_date(col("published_date"))) \
        .withColumn("transformation_date", current_timestamp())

    # Write transformed data back to PostgreSQL
    transformed_df.write \
        .jdbc(
        url=jdbc_url,
        table="transformed_news_articles",
        mode="overwrite",
        properties=connection_properties
    )

    return transformed_df


def transform_reddit_posts():
    """Transform the reddit posts data from the database"""
    # Read data from PostgreSQL
    df = spark.read.jdbc(
        url=jdbc_url,
        table="reddit_posts",
        properties=connection_properties
    )

    # Data validation and cleaning
    df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))

    # Transform data
    transformed_df = df \
        .withColumn("clean_title", clean_text_udf(col("title"))) \
        .withColumn("title_tokens", tokenize_udf(col("clean_title"))) \
        .withColumn("subreddit_normalized", lower(trim(col("subreddit")))) \
        .withColumn("popularity_level", when(col("score") > 1000, "high")
                    .when(col("score") > 100, "medium")
                    .otherwise("low")) \
        .withColumn("created_date", to_date(col("created_utc"))) \
        .withColumn("sentiment_score", sentiment_udf(col("title"))) \
        .withColumn("transformation_date", current_timestamp())

    # Write transformed data back to PostgreSQL
    transformed_df.write \
        .jdbc(
        url=jdbc_url,
        table="transformed_reddit_posts",
        mode="overwrite",
        properties=connection_properties
    )

    return transformed_df


def transform_scholarly_articles():
    """Transform the scholarly articles data from the database"""
    # Read data from PostgreSQL
    df = spark.read.jdbc(
        url=jdbc_url,
        table="scholarly_articles",
        properties=connection_properties
    )

    # Data validation and cleaning
    df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
    df = df.filter(col("abstract").isNotNull())

    # Transform data
    transformed_df = df \
        .withColumn("clean_title", clean_text_udf(col("title"))) \
        .withColumn("clean_abstract", clean_text_udf(col("abstract"))) \
        .withColumn("title_tokens", tokenize_udf(col("clean_title"))) \
        .withColumn("abstract_tokens", tokenize_udf(col("clean_abstract"))) \
        .withColumn("abstract_length", length(col("abstract"))) \
        .withColumn("author_count", size(split(col("authors"), ","))) \
        .withColumn("source_normalized", lower(trim(col("source")))) \
        .withColumn("category_normalized", lower(trim(col("category")))) \
        .withColumn("published_date_normalized", to_date(col("published_date"))) \
        .withColumn("sentiment_score", sentiment_udf(col("abstract"))) \
        .withColumn("transformation_date", current_timestamp())

    # Write transformed data back to PostgreSQL
    transformed_df.write \
        .jdbc(
        url=jdbc_url,
        table="transformed_scholarly_articles",
        mode="overwrite",
        properties=connection_properties
    )

    return transformed_df


def merge_all_data():
    """Merge all transformed data into a unified dataset"""
    # Read transformed data
    news_df = spark.read.jdbc(
        url=jdbc_url,
        table="transformed_news_articles",
        properties=connection_properties
    ).select(
        col("id"),
        col("source_normalized").alias("source"),
        col("category_normalized").alias("category"),
        col("clean_title").alias("title"),
        col("clean_content").alias("content"),
        col("url"),
        col("published_date_normalized").alias("published_date"),
        col("sentiment_score"),
        lit("news").alias("content_type")
    )

    reddit_df = spark.read.jdbc(
        url=jdbc_url,
        table="transformed_reddit_posts",
        properties=connection_properties
    ).select(
        col("id"),
        col("subreddit_normalized").alias("category"),
        lit("reddit").alias("source"),
        col("clean_title").alias("title"),
        lit(None).alias("content"),
        col("url"),
        col("created_date").alias("published_date"),
        col("sentiment_score"),
        lit("social_media").alias("content_type")
    )

    scholarly_df = spark.read.jdbc(
        url=jdbc_url,
        table="transformed_scholarly_articles",
        properties=connection_properties
    ).select(
        col("id"),
        col("source_normalized").alias("source"),
        col("category_normalized").alias("category"),
        col("clean_title").alias("title"),
        col("clean_abstract").alias("content"),
        col("url"),
        col("published_date_normalized").alias("published_date"),
        col("sentiment_score"),
        lit("academic").alias("content_type")
    )

    # Union all dataframes
    unified_df = news_df.unionByName(
        reddit_df, allowMissingColumns=True
    ).unionByName(
        scholarly_df, allowMissingColumns=True
    )

    # Add unified ID
    unified_df = unified_df.withColumn(
        "unified_id",
        F.concat(col("content_type"), lit("_"), col("id"))
    )

    # Add processing timestamp
    unified_df = unified_df.withColumn("processed_timestamp", current_timestamp())

    # Write unified data to a new table
    unified_df.write \
        .jdbc(
        url=jdbc_url,
        table="unified_content",
        mode="overwrite",
        properties=connection_properties
    )

    return unified_df


if __name__ == "__main__":
    # Get the data type to transform from command line arguments
    if len(sys.argv) > 1:
        data_type = sys.argv[1]

        if data_type == "news_articles":
            transform_news_articles()
        elif data_type == "reddit_posts":
            transform_reddit_posts()
        elif data_type == "scholarly_articles":
            transform_scholarly_articles()
        elif data_type == "merge_all":
            merge_all_data()
        else:
            print(f"Unknown data type: {data_type}")
    else:
        print(
            "No data type specified. Please specify one of: news_articles, reddit_posts, scholarly_articles, merge_all")

    # Stop Spark session
    spark.stop()
