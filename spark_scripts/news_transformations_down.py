import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, length, lower, trim, when, current_timestamp
from pyspark.sql.functions import lit, regexp_replace, split, size
from pyspark.sql.types import StringType, ArrayType
import re

# Create Spark Session with minimal configs
spark = SparkSession.builder \
    .appName("NewsDataTransformation") \
    .master("local[1]") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.1.jar") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

# Reduce Spark logging level
spark.sparkContext.setLogLevel("WARN")

# Database connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/airflow"
connection_properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
    "fetchsize": "1000"  # Smaller batch size for fetching
}


def transform_scholarly_articles():
    """Transform the scholarly articles data from the database"""
    try:
        # First, check if the source table exists and has data
        table_count = spark.read \
            .jdbc(
            url=jdbc_url,
            table="(SELECT COUNT(*) as count FROM scholarly_articles) as count_query",
            properties=connection_properties
        ).collect()[0][0]

        print(f"Found {table_count} records in scholarly_articles table")

        if table_count == 0:
            print("No data in source table. Exiting transformation.")
            return None

        # Read data from PostgreSQL with single partition
        df = spark.read \
            .option("numPartitions", 1) \
            .jdbc(
            url=jdbc_url,
            table="scholarly_articles",
            properties=connection_properties
        )

        initial_count = df.count()
        print(f"Initial dataframe count: {initial_count}")

        # Apply filters and see how many records remain
        filtered_df = df \
            .filter(col("title").isNotNull() & (length(col("title")) > 0)) \
            .filter(col("abstract").isNotNull())

        filtered_count = filtered_df.count()
        print(f"After filtering: {filtered_count} records")

        if filtered_count == 0:
            print("No records remain after filtering. Check your data quality.")
            return None

        # Basic transformations only - simplified from original code
        transformed_df = filtered_df \
            .withColumn("clean_title", regexp_replace(lower(trim(col("title"))), r'[^a-zA-Z\s]', '')) \
            .withColumn("clean_abstract", regexp_replace(lower(trim(col("abstract"))), r'[^a-zA-Z\s]', '')) \
            .withColumn("abstract_length", length(col("abstract"))) \
            .withColumn("author_count", size(split(col("authors"), ","))) \
            .withColumn("source_normalized", lower(trim(col("source")))) \
            .withColumn("category_normalized", lower(trim(col("category")))) \
            .withColumn("published_date_normalized", to_date(col("published_date"))) \
            .withColumn("transformation_date", current_timestamp()) \
            .withColumn("sentiment_score", lit(0.0))

        # Cache the transformed dataframe
        transformed_df.cache()

        final_count = transformed_df.count()
        print(f"Final transformed count: {final_count} records")

        # Write transformed data back to PostgreSQL
        transformed_df.coalesce(1) \
            .write \
            .jdbc(
            url=jdbc_url,
            table="transformed_scholarly_articles",
            mode="overwrite",
            properties=connection_properties
        )

        print("Data successfully written to transformed_scholarly_articles table")

        # Verify the write was successful
        verify_count = spark.read \
            .jdbc(
            url=jdbc_url,
            table="(SELECT COUNT(*) as count FROM transformed_scholarly_articles) as verify_query",
            properties=connection_properties
        ).collect()[0][0]

        print(f"Verification: {verify_count} records in transformed_scholarly_articles table")

        return transformed_df

    except Exception as e:
        print(f"Error in transform_scholarly_articles: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Get the data type to transform from command line arguments
    if len(sys.argv) > 1:
        data_type = sys.argv[1]

        if data_type == "scholarly_articles":
            result = transform_scholarly_articles()
            if result is not None:
                print("Transformation completed successfully")
            else:
                print("Transformation failed or produced no results")
        else:
            print(f"Unknown data type: {data_type}")
    else:
        print("No data type specified. Please specify: scholarly_articles")

    # Stop Spark session
    spark.stop()
