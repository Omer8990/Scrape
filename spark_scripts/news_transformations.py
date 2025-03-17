# # spark_scripts/news_transformations.py
# """
# News data transformation script using PySpark.
# This script performs transformations on extracted news data.
# """
import sys

from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, length, lower, trim, when, current_timestamp, concat
from pyspark.sql.functions import udf, regexp_replace, split, size, lit, monotonically_increasing_id, concat
from pyspark.sql.types import StringType, ArrayType
import re
# spark_scripts/news_transformations.py
# """
# News data transformation script using PySpark.
# This script performs transformations on extracted news data.
# """
# import sys
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, length, lower, trim, when, current_timestamp
# from pyspark.sql.functions import udf, regexp_replace, split, size, lit, monotonically_increasing_id
# from pyspark.sql.types import StringType, ArrayType
# import re
#
# # Create Spark Session with appropriate configs
# spark = SparkSession.builder \
#     .appName("NewsDataTransformation") \
#     .master("local[1]") \
#     .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar") \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.driver.memory", "2g") \
#     .getOrCreate()
#
#
# # Database connection properties
# jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
# connection_properties = {
#     "user": "airflow",
#     "password": "airflow",
#     "driver": "org.postgresql.Driver"
# }
#
# # SIMPLIFIED UDFs
# # Simple text cleaning function
# @udf(StringType())
# def clean_text_udf(text):
#     if text is None:
#         return None
#     # Remove special characters and numbers
#     text = re.sub(r'[^a-zA-Z\s]', '', text)
#     # Convert to lowercase and remove extra spaces
#     text = text.lower().strip()
#     return text
#
# # Simplified tokenization function - no stopwords
# @udf(ArrayType(StringType()))
# def simple_tokenize_udf(text):
#     if text is None or not isinstance(text, str):
#         return []
#     # Simple tokenization by splitting on whitespace
#     words = text.split()
#     # Only filter by length
#     filtered_tokens = [word for word in words if len(word) > 2]
#     return filtered_tokens
#
# # Transform functions
# def transform_news_articles():
#     """Transform the news articles data from the database"""
#     # Read data from PostgreSQL
#     df = spark.read.jdbc(
#         url=jdbc_url,
#         table="news_articles",
#         properties=connection_properties
#     )
#
#     # Data validation and cleaning
#     df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
#     df = df.filter(col("content").isNotNull() & (length(col("content")) > 0))
#
#     # Transform data - without sentiment analysis
#     transformed_df = df \
#         .withColumn("clean_title", clean_text_udf(col("title"))) \
#         .withColumn("clean_content", clean_text_udf(col("content"))) \
#         .withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
#         .withColumn("content_tokens", simple_tokenize_udf(col("clean_content"))) \
#         .withColumn("content_length", length(col("content"))) \
#         .withColumn("source_normalized", lower(trim(col("source")))) \
#         .withColumn("category_normalized", lower(trim(col("category")))) \
#         .withColumn("published_date_normalized", to_date(col("published_date"))) \
#         .withColumn("transformation_date", current_timestamp()) \
#         .withColumn("sentiment_score", lit(0.0))  # Placeholder instead of complex sentiment analysis
#
#     # Write transformed data back to PostgreSQL
#     transformed_df.write \
#         .jdbc(
#         url=jdbc_url,
#         table="transformed_news_articles",
#         mode="overwrite",
#         properties=connection_properties
#     )
#
#     return transformed_df
#
#
# def transform_reddit_posts():
#     """Transform the reddit posts data from the database"""
#     # Read data from PostgreSQL
#     df = spark.read.jdbc(
#         url=jdbc_url,
#         table="reddit_posts",
#         properties=connection_properties
#     )
#
#     # Data validation and cleaning
#     df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
#
#     # Transform data - without sentiment analysis
#     transformed_df = df \
#         .withColumn("clean_title", clean_text_udf(col("title"))) \
#         .withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
#         .withColumn("subreddit_normalized", lower(trim(col("subreddit")))) \
#         .withColumn("popularity_level", when(col("score") > 1000, "high")
#                     .when(col("score") > 100, "medium")
#                     .otherwise("low")) \
#         .withColumn("created_date", to_date(col("created_utc"))) \
#         .withColumn("transformation_date", current_timestamp()) \
#         .withColumn("sentiment_score", lit(0.0))  # Placeholder instead of complex sentiment analysis
#
#     # Write transformed data back to PostgreSQL
#     transformed_df.write \
#         .jdbc(
#         url=jdbc_url,
#         table="transformed_reddit_posts",
#         mode="overwrite",
#         properties=connection_properties
#     )
#
#     return transformed_df
#
#
# def transform_scholarly_articles():
#     """Transform the scholarly articles data from the database"""
#     # Read data from PostgreSQL
#     df = spark.read.jdbc(
#         url=jdbc_url,
#         table="scholarly_articles",
#         properties=connection_properties
#     )
#
#     # Data validation and cleaning
#     df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
#     df = df.filter(col("abstract").isNotNull())
#
#     # Apply clean_text UDF
#     df = df.withColumn("clean_title", clean_text_udf(col("title"))) \
#         .withColumn("clean_abstract", clean_text_udf(col("abstract")))
#
#     # Use simple_tokenize_udf instead of split
#     df = df.withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
#         .withColumn("abstract_tokens", simple_tokenize_udf(col("clean_abstract")))
#
#     # Basic transformations
#     transformed_df = df \
#         .withColumn("abstract_length", length(col("abstract"))) \
#         .withColumn("author_count", size(split(col("authors"), ","))) \
#         .withColumn("source_normalized", lower(trim(col("source")))) \
#         .withColumn("category_normalized", lower(trim(col("category")))) \
#         .withColumn("published_date_normalized", to_date(col("published_date"))) \
#         .withColumn("transformation_date", current_timestamp()) \
#         .withColumn("sentiment_score", lit(0.0))
#
#     # Write transformed data back to PostgreSQL
#     transformed_df.write \
#         .jdbc(
#         url=jdbc_url,
#         table="transformed_scholarly_articles",
#         mode="overwrite",
#         properties=connection_properties
#     )
#
#     return transformed_df
#
# def merge_all_data():
#     """Merge all transformed data into a unified dataset"""
#     # Read transformed data
#     news_df = spark.read.jdbc(
#         url=jdbc_url,
#         table="transformed_news_articles",
#         properties=connection_properties
#     ).select(
#         col("id"),
#         col("source_normalized").alias("source"),
#         col("category_normalized").alias("category"),
#         col("clean_title").alias("title"),
#         col("clean_content").alias("content"),
#         col("url"),
#         col("published_date_normalized").alias("published_date"),
#         col("sentiment_score"),
#         lit("news").alias("content_type")
#     )
#
#     reddit_df = spark.read.jdbc(
#         url=jdbc_url,
#         table="transformed_reddit_posts",
#         properties=connection_properties
#     ).select(
#         col("id"),
#         col("subreddit_normalized").alias("category"),
#         lit("reddit").alias("source"),
#         col("clean_title").alias("title"),
#         lit(None).alias("content"),
#         col("url"),
#         col("created_date").alias("published_date"),
#         col("sentiment_score"),
#         lit("social_media").alias("content_type")
#     )
#
#     scholarly_df = spark.read.jdbc(
#         url=jdbc_url,
#         table="transformed_scholarly_articles",
#         properties=connection_properties
#     ).select(
#         col("id"),
#         col("source_normalized").alias("source"),
#         col("category_normalized").alias("category"),
#         col("clean_title").alias("title"),
#         col("clean_abstract").alias("content"),
#         col("url"),
#         col("published_date_normalized").alias("published_date"),
#         col("sentiment_score"),
#         lit("academic").alias("content_type")
#     )
#
#     # Union all dataframes
#     unified_df = news_df.unionByName(
#         reddit_df, allowMissingColumns=True
#     ).unionByName(
#         scholarly_df, allowMissingColumns=True
#     )
#
#     # Add unified ID
#     unified_df = unified_df.withColumn(
#         "unified_id",
#         lit(col("content_type")).concat(lit("_")).concat(col("id"))
#     )
#
#     # Add processing timestamp
#     unified_df = unified_df.withColumn("processed_timestamp", current_timestamp())
#
#     # Write unified data to a new table
#     unified_df.write \
#         .jdbc(
#         url=jdbc_url,
#         table="unified_content",
#         mode="overwrite",
#         properties=connection_properties
#     )
#
#     return unified_df
#
#
# if __name__ == "__main__":
#     # Get the data type to transform from command line arguments
#     if len(sys.argv) > 1:
#         data_type = sys.argv[1]
#
#         if data_type == "news_articles":
#             transform_news_articles()
#         elif data_type == "reddit_posts":
#             transform_reddit_posts()
#         elif data_type == "scholarly_articles":
#             transform_scholarly_articles()
#         elif data_type == "merge_all":
#             merge_all_data()
#         else:
#             print(f"Unknown data type: {data_type}")
#     else:
#         print(
#             "No data type specified. Please specify one of: news_articles, reddit_posts, scholarly_articles, merge_all")
#
#     # Stop Spark session
#     spark.stop()
#
# # Create Spark Session with appropriate configs
# spark = SparkSession.builder \
#     .appName("NewsDataTransformation") \
#     .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar") \
#     .config("spark.network.timeout", "600s") \
#     .config("spark.executor.heartbeatInterval", "60s") \
#     .config("spark.dynamicAllocation.enabled", "false") \
#     .config("spark.executor.memory", "512m") \
#     .config("spark.driver.memory", "512m") \
#     .config("spark.executor.cores", "1") \
#     .config("spark.driver.cores", "1") \
#     .getOrCreate()
#
# # Database connection properties
# jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
# connection_properties = {
#     "user": "airflow",
#     "password": "airflow",
#     "driver": "org.postgresql.Driver"
# }
#
# # SIMPLIFIED UDFs
# # Simple text cleaning function
# @udf(StringType())
# def clean_text_udf(text):
#     if text is None:
#         return None
#     # Remove special characters and numbers
#     text = re.sub(r'[^a-zA-Z\s]', '', text)
#     # Convert to lowercase and remove extra spaces
#     text = text.lower().strip()
#     return text
#
# # Simplified tokenization function - no stopwords
# @udf(ArrayType(StringType()))
# def simple_tokenize_udf(text):
#     if text is None or not isinstance(text, str):
#         return []
#     # Simple tokenization by splitting on whitespace
#     words = text.split()
#     # Only filter by length
#     filtered_tokens = [word for word in words if len(word) > 2]
#     return filtered_tokens
#
# # Transform functions
# def transform_news_articles():
#     """Transform the news articles data from the database"""
#     # Read data from PostgreSQL
#     df = spark.read.jdbc(
#         url=jdbc_url,
#         table="news_articles",
#         properties=connection_properties
#     )
#
#     # Data validation and cleaning
#     df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
#     df = df.filter(col("content").isNotNull() & (length(col("content")) > 0))
#
#     # Transform data - without sentiment analysis
#     transformed_df = df \
#         .withColumn("clean_title", clean_text_udf(col("title"))) \
#         .withColumn("clean_content", clean_text_udf(col("content"))) \
#         .withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
#         .withColumn("content_tokens", simple_tokenize_udf(col("clean_content"))) \
#         .withColumn("content_length", length(col("content"))) \
#         .withColumn("source_normalized", lower(trim(col("source")))) \
#         .withColumn("category_normalized", lower(trim(col("category")))) \
#         .withColumn("published_date_normalized", to_date(col("published_date"))) \
#         .withColumn("transformation_date", current_timestamp()) \
#         .withColumn("sentiment_score", lit(0.0))  # Placeholder instead of complex sentiment analysis
#
#     # Write transformed data back to PostgreSQL
#     transformed_df.write \
#         .jdbc(
#         url=jdbc_url,
#         table="transformed_news_articles",
#         mode="overwrite",
#         properties=connection_properties
#     )
#
#     return transformed_df
#
#
# def transform_reddit_posts():
#     """Transform the reddit posts data from the database"""
#     # Read data from PostgreSQL
#     df = spark.read.jdbc(
#         url=jdbc_url,
#         table="reddit_posts",
#         properties=connection_properties
#     )
#
#     # Data validation and cleaning
#     df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
#
#     # Transform data - without sentiment analysis
#     transformed_df = df \
#         .withColumn("clean_title", clean_text_udf(col("title"))) \
#         .withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
#         .withColumn("subreddit_normalized", lower(trim(col("subreddit")))) \
#         .withColumn("popularity_level", when(col("score") > 1000, "high")
#                     .when(col("score") > 100, "medium")
#                     .otherwise("low")) \
#         .withColumn("created_date", to_date(col("created_utc"))) \
#         .withColumn("transformation_date", current_timestamp()) \
#         .withColumn("sentiment_score", lit(0.0))  # Placeholder instead of complex sentiment analysis
#
#     # Write transformed data back to PostgreSQL
#     transformed_df.write \
#         .jdbc(
#         url=jdbc_url,
#         table="transformed_reddit_posts",
#         mode="overwrite",
#         properties=connection_properties
#     )
#
#     return transformed_df
#
#
# def transform_scholarly_articles():
#     """Transform the scholarly articles data from the database"""
#     # Read data from PostgreSQL
#     df = spark.read.jdbc(
#         url=jdbc_url,
#         table="scholarly_articles",
#         properties=connection_properties
#     )
#
#     # Data validation and cleaning
#     df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
#     df = df.filter(col("abstract").isNotNull())
#
#     # Apply clean_text UDF
#     df = df.withColumn("clean_title", clean_text_udf(col("title"))) \
#         .withColumn("clean_abstract", clean_text_udf(col("abstract")))
#
#     # Use simple_tokenize_udf instead of split
#     df = df.withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
#         .withColumn("abstract_tokens", simple_tokenize_udf(col("clean_abstract")))
#
#     # Basic transformations
#     transformed_df = df \
#         .withColumn("abstract_length", length(col("abstract"))) \
#         .withColumn("author_count", size(split(col("authors"), ","))) \
#         .withColumn("source_normalized", lower(trim(col("source")))) \
#         .withColumn("category_normalized", lower(trim(col("category")))) \
#         .withColumn("published_date_normalized", to_date(col("published_date"))) \
#         .withColumn("transformation_date", current_timestamp()) \
#         .withColumn("sentiment_score", lit(0.0))
#
#     # Write transformed data back to PostgreSQL
#     transformed_df.write \
#         .jdbc(
#         url=jdbc_url,
#         table="transformed_scholarly_articles",
#         mode="overwrite",
#         properties=connection_properties
#     )
#
#     return transformed_df
#
# def merge_all_data():
#     """Merge all transformed data into a unified dataset"""
#     # Read transformed data
#     news_df = spark.read.jdbc(
#         url=jdbc_url,
#         table="transformed_news_articles",
#         properties=connection_properties
#     ).select(
#         col("id"),
#         col("source_normalized").alias("source"),
#         col("category_normalized").alias("category"),
#         col("clean_title").alias("title"),
#         col("clean_content").alias("content"),
#         col("url"),
#         col("published_date_normalized").alias("published_date"),
#         col("sentiment_score"),
#         lit("news").alias("content_type")
#     )
#
#     reddit_df = spark.read.jdbc(
#         url=jdbc_url,
#         table="transformed_reddit_posts",
#         properties=connection_properties
#     ).select(
#         col("id"),
#         col("subreddit_normalized").alias("category"),
#         lit("reddit").alias("source"),
#         col("clean_title").alias("title"),
#         lit(None).alias("content"),
#         col("url"),
#         col("created_date").alias("published_date"),
#         col("sentiment_score"),
#         lit("social_media").alias("content_type")
#     )
#
#     scholarly_df = spark.read.jdbc(
#         url=jdbc_url,
#         table="transformed_scholarly_articles",
#         properties=connection_properties
#     ).select(
#         col("id"),
#         col("source_normalized").alias("source"),
#         col("category_normalized").alias("category"),
#         col("clean_title").alias("title"),
#         col("clean_abstract").alias("content"),
#         col("url"),
#         col("published_date_normalized").alias("published_date"),
#         col("sentiment_score"),
#         lit("academic").alias("content_type")
#     )
#
#     # Union all dataframes
#     unified_df = news_df.unionByName(
#         reddit_df, allowMissingColumns=True
#     ).unionByName(
#         scholarly_df, allowMissingColumns=True
#     )
#
#     # Add unified ID
#     unified_df = unified_df.withColumn(
#         "unified_id",
#         lit(col("content_type")).concat(lit("_")).concat(col("id"))
#     )
#
#     # Add processing timestamp
#     unified_df = unified_df.withColumn("processed_timestamp", current_timestamp())
#
#     # Write unified data to a new table
#     unified_df.write \
#         .jdbc(
#         url=jdbc_url,
#         table="unified_content",
#         mode="overwrite",
#         properties=connection_properties
#     )
#
#     return unified_df
#
#
# if __name__ == "__main__":
#     # Get the data type to transform from command line arguments
#     if len(sys.argv) > 1:
#         data_type = sys.argv[1]
#
#         if data_type == "news_articles":
#             transform_news_articles()
#         elif data_type == "reddit_posts":
#             transform_reddit_posts()
#         elif data_type == "scholarly_articles":
#             transform_scholarly_articles()
#         elif data_type == "merge_all":
#             merge_all_data()
#         else:
#             print(f"Unknown data type: {data_type}")
#     else:
#         print(
#             "No data type specified. Please specify one of: news_articles, reddit_posts, scholarly_articles, merge_all")
#
#     # Stop Spark session
#     spark.stop()

import sys
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, length, lower, trim, when, current_timestamp
from pyspark.sql.functions import udf, split, size, lit
from pyspark.sql.types import StringType, ArrayType

# Create a minimal Spark Session with reduced resources
spark = SparkSession.builder \
    .appName("NewsDataTransformation") \
    .master("local[1]") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Reduce logging to minimize overhead
spark.sparkContext.setLogLevel("ERROR")

# Database connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
connection_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
    "fetchsize": "100"  # Reduce fetch size to minimize memory usage
}


# SIMPLIFIED UDFs
# Simple text cleaning function
@udf(StringType())
def clean_text_udf(text):
    if text is None:
        return None
    # Remove special characters and numbers
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    # Convert to lowercase and remove extra spaces
    text = text.lower().strip()
    return text


# Simplified tokenization function
@udf(ArrayType(StringType()))
def simple_tokenize_udf(text):
    if text is None or not isinstance(text, str):
        return []
    # Simple tokenization by splitting on whitespace
    words = text.split()
    # Only filter by length
    filtered_tokens = [word for word in words if len(word) > 2]
    return filtered_tokens


# Transform functions with optimizations
def transform_news_articles():
    """Transform the news articles data from the database with optimizations"""
    # Read data from PostgreSQL with only necessary columns
    df = spark.read.jdbc(
        url=jdbc_url,
        table="news_articles",
        properties=connection_properties
    )

    # Basic data validation and cleaning
    df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
    df = df.filter(col("content").isNotNull() & (length(col("content")) > 0))

    # Transform data with minimized operations
    transformed_df = df \
        .withColumn("clean_title", clean_text_udf(col("title"))) \
        .withColumn("clean_content", clean_text_udf(col("content"))) \
        .withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
        .withColumn("content_tokens", simple_tokenize_udf(col("clean_content"))) \
        .withColumn("content_length", length(col("content"))) \
        .withColumn("source_normalized", lower(trim(col("source")))) \
        .withColumn("category_normalized", lower(trim(col("category")))) \
        .withColumn("published_date_normalized", to_date(col("published_date"))) \
        .withColumn("transformation_date", current_timestamp()) \
        .withColumn("sentiment_score", lit(0.0))  # Placeholder

    # Write transformed data in smaller batches
    transformed_df.coalesce(1).write \
        .jdbc(
        url=jdbc_url,
        table="transformed_news_articles",
        mode="overwrite",
        properties=connection_properties
    )

    return transformed_df


def transform_reddit_posts():
    """Transform the reddit posts data from the database with optimizations"""
    # Read data from PostgreSQL
    df = spark.read.jdbc(
        url=jdbc_url,
        table="reddit_posts",
        properties=connection_properties
    )

    # Data validation
    df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))

    # Transform data with reduced operations
    transformed_df = df \
        .withColumn("clean_title", clean_text_udf(col("title"))) \
        .withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
        .withColumn("subreddit_normalized", lower(trim(col("subreddit")))) \
        .withColumn("popularity_level", when(col("score") > 1000, "high")
                    .when(col("score") > 100, "medium")
                    .otherwise("low")) \
        .withColumn("created_date", to_date(col("created_utc"))) \
        .withColumn("transformation_date", current_timestamp()) \
        .withColumn("sentiment_score", lit(0.0))

    # Write transformed data
    transformed_df.coalesce(1).write \
        .jdbc(
        url=jdbc_url,
        table="transformed_reddit_posts",
        mode="overwrite",
        properties=connection_properties
    )

    return transformed_df


def transform_scholarly_articles():
    """Transform the scholarly articles data from the database with optimizations"""
    # Read data from PostgreSQL
    df = spark.read.jdbc(
        url=jdbc_url,
        table="scholarly_articles",
        properties=connection_properties
    )

    # Basic data validation
    df = df.filter(col("title").isNotNull() & (length(col("title")) > 0))
    df = df.filter(col("abstract").isNotNull())

    # Apply transformations
    df = df.withColumn("clean_title", clean_text_udf(col("title"))) \
        .withColumn("clean_abstract", clean_text_udf(col("abstract")))

    df = df.withColumn("title_tokens", simple_tokenize_udf(col("clean_title"))) \
        .withColumn("abstract_tokens", simple_tokenize_udf(col("clean_abstract")))

    transformed_df = df \
        .withColumn("abstract_length", length(col("abstract"))) \
        .withColumn("author_count", size(split(col("authors"), ","))) \
        .withColumn("source_normalized", lower(trim(col("source")))) \
        .withColumn("category_normalized", lower(trim(col("category")))) \
        .withColumn("published_date_normalized", to_date(col("published_date"))) \
        .withColumn("transformation_date", current_timestamp()) \
        .withColumn("sentiment_score", lit(0.0))

    # Write transformed data
    transformed_df.coalesce(1).write \
        .jdbc(
        url=jdbc_url,
        table="transformed_scholarly_articles",
        mode="overwrite",
        properties=connection_properties
    )

    return transformed_df


def merge_all_data():
    """Merge all transformed data into a unified dataset with optimizations"""
    # Read transformed data with only necessary columns
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
        concat(col("content_type"), lit("_"), col("id"))
    )

    # Add processing timestamp
    unified_df = unified_df.withColumn("processed_timestamp", current_timestamp())

    # Write unified data to a new table with reduced partitions
    unified_df.coalesce(1).write \
        .jdbc(
        url=jdbc_url,
        table="unified_content",
        mode="overwrite",
        properties=connection_properties
    )

    return unified_df


if __name__ == "__main__":
    # Make sure we use spark.conf rather than global settings
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")

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

    # Stop Spark session to release resources
    spark.stop()
