import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, explode, split, desc, datediff, current_date
from pyspark.sql.functions import regexp_replace, expr, current_timestamp, lit, array
from pyspark.ml.feature import CountVectorizer, Tokenizer, StopWordsRemover
from pyspark.ml.clustering import LDA
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType, DoubleType, TimestampType

# Initialize Spark session with reduced resources and optimized configs
spark = SparkSession.builder \
    .appName("Content Insights Generator") \
    .master("local[1]") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.sql.shuffle.partitions", "10") \
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

# Read the unified content with optimized approach
try:
    unified_df = spark.read.jdbc(
        url=jdbc_url,
        table="unified_content",
        properties=connection_properties
    )

    # Register table for SQL queries
    unified_df.createOrReplaceTempView("unified_content")

    # 1. Find trending topics - using DataFrame API instead of SQL for better control
    trending_topics = unified_df.filter(
        datediff(current_date(), col("published_date")) <= 3
    ).groupBy("category").agg(
        count("*").alias("count"),
        avg("sentiment_score").alias("avg_sentiment")
    ).orderBy(desc("count"), desc("avg_sentiment")).limit(10)

    # 2. Find most positive content
    positive_content = unified_df.filter(
        (col("sentiment_score") > 0.7) &
        (datediff(current_date(), col("published_date")) <= 5)
    ).select(
        "unified_id", "title", "source", "sentiment_score", "url"
    ).orderBy(desc("sentiment_score")).limit(15)

    # 3. Recent important scholarly content
    scholarly_insights = unified_df.filter(
        (col("content_type") == "academic") &
        (datediff(current_date(), col("published_date")) <= 14)
    ).select(
        "unified_id", "title", "source", "sentiment_score", "content_type", "url"
    ).orderBy(desc("sentiment_score"), desc("published_date")).limit(10)

    # 4. Process for topic modeling
    # Prepare documents for topic modeling
    documents = unified_df.select("unified_id", "title", "content").filter(col("content").isNotNull())
    documents = documents.withColumn("text", regexp_replace(col("content"), "[^a-zA-Z\\s]", ""))

    # Tokenize text into words
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    tokenized_docs = tokenizer.transform(documents)

    # Remove stop words
    stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    tokenized_docs = stopwords_remover.transform(tokenized_docs)

    # Vectorization - use filtered_words instead of text
    cv = CountVectorizer(inputCol="filtered_words", outputCol="features", vocabSize=1000, minDF=2.0)
    cv_model = cv.fit(tokenized_docs)
    vectorized_docs = cv_model.transform(tokenized_docs)

    # LDA model with reduced iterations for performance
    lda = LDA(k=5, maxIter=10, optimizer="online")
    model = lda.fit(vectorized_docs)
    topics = model.describeTopics()
    vocabulary = cv_model.vocabulary

    # Create content_insights table if it doesn't exist
    insight_schema = StructType([
        StructField("id", LongType(), False),
        StructField("insight_type", StringType(), False),
        StructField("title", StringType(), False),
        StructField("description", StringType(), False),
        StructField("sources", ArrayType(StringType()), False),
        StructField("related_content_ids", ArrayType(StringType()), True),
        StructField("score", DoubleType(), False),
        StructField("generated_at", TimestampType(), False)
    ])

    # Create empty DataFrame with the schema
    empty_insights_df = spark.createDataFrame([], insight_schema)

    # Try to write it to create the table if it doesn't exist
    empty_insights_df.write.jdbc(
        url=jdbc_url,
        table="content_insights",
        mode="append",
        properties=connection_properties
    )


    # Function to add insights directly with DataFrame operations
    def add_insight(insight_type, title, description, sources, related_content_ids=None, score=0.0):
        import time
        from datetime import datetime  # Add this import

        # Generate a unique ID based on timestamp
        insight_id = int(time.time() * 1000) % 2147483647  # Ensure it's within INTEGER range

        # Create data for the insight
        data = [(
            insight_id,
            insight_type,
            title,
            description,
            sources if isinstance(sources, list) else [sources],
            related_content_ids if related_content_ids and isinstance(related_content_ids, list) else [],
            float(score),
            datetime.now()  # Use Python's datetime.now() instead of Spark's current_timestamp()
        )]

        # Create DataFrame with the insight data
        insight_df = spark.createDataFrame(data, insight_schema)

        # Write directly to PostgreSQL
        insight_df.write.jdbc(
            url=jdbc_url,
            table="content_insights",
            mode="append",
            properties=connection_properties
        )


    # Process trending topics
    for row in trending_topics.collect():
        add_insight(
            insight_type="trending_topic",
            title=row['category'],
            description=f"This category has {row['count']} recent articles with an average sentiment of {row['avg_sentiment']:.2f}",
            sources=['multiple sources'],
            score=float(row['count'])
        )

    # Process positive content
    for row in positive_content.collect():
        add_insight(
            insight_type="positive_content",
            title=row['title'],
            description="This content has a very positive sentiment score",
            sources=[row['source']],
            related_content_ids=[row['unified_id']],
            score=float(row['sentiment_score'])
        )

    # Process topic modeling insights
    for i, topic in enumerate(topics.collect()):
        term_indices = [int(idx) for idx in topic["termIndices"]]
        terms = [vocabulary[idx] for idx in term_indices]
        weights = topic["termWeights"]

        # Add topic insight
        add_insight(
            insight_type="discovered_topic",
            title=f"Topic {i + 1}: {', '.join(terms[:3])}",
            description=f"Discovered topic with key terms: {', '.join(terms[:10])}",
            sources=['topic modeling'],
            score=float(sum(weights[:5]))
        )

    print("Script completed successfully")

except Exception as e:
    print(f"Error occurred: {str(e)}")
    sys.exit(1)
finally:
    # Stop Spark session to release resources
    spark.stop()