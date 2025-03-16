# spark_scripts/news_transformations.py
"""
News data transformation script using PySpark.
This script performs transformations on extracted news data.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, to_date, to_timestamp, regexp_replace, lower, trim
from pyspark.sql.functions import udf, explode, split, length, when, current_timestamp
from pyspark.sql.types import StringType, ArrayType, IntegerType, FloatType, size
import pyspark.sql.functions as F
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Download NLTK resources
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('vader_lexicon')

# Initialize these objects once at the driver level
stop_words = set(stopwords.words('english'))
sia = SentimentIntensityAnalyzer()


# Broadcast variables to workers
def initialize_broadcast_vars(spark):
    # Create broadcast variables for the stop words (can be accessed across the cluster)
    return {
        "stop_words": spark.sparkContext.broadcast(list(stop_words))
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


# Create Spark Session with appropriate configs
spark = SparkSession.builder \
    .appName("NewsDataTransformation") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

# Broadcast variables
bc_vars = initialize_broadcast_vars(spark)

# Database connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
connection_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

# Register UDFs
clean_text_udf = udf(clean_text, StringType())


# Fixed tokenize function to use broadcast variables
def tokenize_text(text):
    if text is None:
        return []
    tokens = word_tokenize(text)
    # Use the broadcast stop words
    broadcast_stop_words = bc_vars["stop_words"].value
    filtered_tokens = [word for word in tokens if word.lower() not in broadcast_stop_words and len(word) > 2]
    return filtered_tokens


tokenize_udf = udf(tokenize_text, ArrayType(StringType()))


# Precompute sentiments on driver instead of in UDF
def get_sentiment_scores(texts):
    return [sia.polarity_scores(text)['compound'] if text else 0.0 for text in texts]


# Transform functions
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

    # Transform data - first without sentiment
    transformed_df = df \
        .withColumn("clean_title", clean_text_udf(col("title"))) \
        .withColumn("clean_content", clean_text_udf(col("content"))) \
        .withColumn("title_tokens", tokenize_udf(col("clean_title"))) \
        .withColumn("content_tokens", tokenize_udf(col("clean_content"))) \
        .withColumn("content_length", length(col("content"))) \
        .withColumn("source_normalized", lower(trim(col("source")))) \
        .withColumn("category_normalized", lower(trim(col("category")))) \
        .withColumn("published_date_normalized", to_date(col("published_date"))) \
        .withColumn("transformation_date", current_timestamp())

    # Calculate sentiment on driver to avoid serialization issues
    content_data = [row.content for row in transformed_df.select("content").collect()]
    sentiment_scores = get_sentiment_scores(content_data)

    # Create a new DataFrame with sentiment scores
    sentiment_df = spark.createDataFrame(
        [(i, float(score)) for i, score in enumerate(sentiment_scores)],
        ["row_id", "sentiment_score"]
    )

    # Add row index to the transformed DataFrame
    transformed_df = transformed_df.withColumn("row_id", F.monotonically_increasing_id())

    # Join the sentiment data back
    transformed_df = transformed_df.join(sentiment_df, "row_id").drop("row_id")

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

    # Transform data - first without sentiment
    transformed_df = df \
        .withColumn("clean_title", clean_text_udf(col("title"))) \
        .withColumn("title_tokens", tokenize_udf(col("clean_title"))) \
        .withColumn("subreddit_normalized", lower(trim(col("subreddit")))) \
        .withColumn("popularity_level", when(col("score") > 1000, "high")
                    .when(col("score") > 100, "medium")
                    .otherwise("low")) \
        .withColumn("created_date", to_date(col("created_utc"))) \
        .withColumn("transformation_date", current_timestamp())

    # Calculate sentiment on driver to avoid serialization issues
    title_data = [row.title for row in transformed_df.select("title").collect()]
    sentiment_scores = get_sentiment_scores(title_data)

    # Create a new DataFrame with sentiment scores
    sentiment_df = spark.createDataFrame(
        [(i, float(score)) for i, score in enumerate(sentiment_scores)],
        ["row_id", "sentiment_score"]
    )

    # Add row index to the transformed DataFrame
    transformed_df = transformed_df.withColumn("row_id", F.monotonically_increasing_id())

    # Join the sentiment data back
    transformed_df = transformed_df.join(sentiment_df, "row_id").drop("row_id")

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

    # Transform data - first without sentiment
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
        .withColumn("transformation_date", current_timestamp())

    # Calculate sentiment on driver to avoid serialization issues
    abstract_data = [row.abstract for row in transformed_df.select("abstract").collect()]
    sentiment_scores = get_sentiment_scores(abstract_data)

    # Create a new DataFrame with sentiment scores
    sentiment_df = spark.createDataFrame(
        [(i, float(score)) for i, score in enumerate(sentiment_scores)],
        ["row_id", "sentiment_score"]
    )

    # Add row index to the transformed DataFrame
    transformed_df = transformed_df.withColumn("row_id", F.monotonically_increasing_id())

    # Join the sentiment data back
    transformed_df = transformed_df.join(sentiment_df, "row_id").drop("row_id")

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
