# transformer/spark_transformer.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, lower, regexp_replace, when
from pyspark.sql.types import StringType, FloatType, BooleanType
import re
from typing import List


class NewsTransformer:
    """Transformer for processing and enriching news data using Spark"""

    def __init__(self, spark: SparkSession = None):
        """Initialize with an existing SparkSession or create a new one"""
        if spark:
            self.spark = spark
        else:
            self.spark = SparkSession.builder \
                .appName("NewsETL") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()

    def _process_reddit_content(self, df: DataFrame) -> DataFrame:
        """Add Reddit-specific processing"""

        # Extract subreddit from source
        def extract_subreddit(source):
            if source and source.startswith('reddit_r/'):
                return source.replace('reddit_r/', '')
            return None

        extract_subreddit_udf = udf(extract_subreddit, StringType())

        # Calculate a Reddit engagement score
        def calculate_reddit_engagement(raw_data):
            if not raw_data:
                return 0.0

            try:
                import json
                data = json.loads(raw_data)
                score = data.get('score', 0)
                comments = data.get('num_comments', 0)

                # Simple engagement formula: normalize to 0-1 range
                # Higher scores for posts with both upvotes and comments
                upvote_component = min(score / 1000, 1.0) * 0.7
                comment_component = min(comments / 100, 1.0) * 0.3

                return upvote_component + comment_component
            except:
                return 0.0

        calculate_engagement_udf = udf(calculate_reddit_engagement, FloatType())

        # Apply Reddit-specific transformations
        return df.withColumn(
            "subreddit",
            when(col("source").like("reddit%"), extract_subreddit_udf(col("source")))
            .otherwise(None)
        ).withColumn(
            "engagement_score",
            when(col("source").like("reddit%"), calculate_engagement_udf(col("raw_data")))
            .otherwise(
                # For non-Reddit sources, use a placeholder engagement formula
                when(col("category") == "science", lit(0.7))
                .when(col("category") == "cryptocurrency", lit(0.8))
                .otherwise(lit(0.5))
            )
        )

    def transform(self, news_df: DataFrame) -> DataFrame:
        """Transform news data by cleaning text, scoring relevance, etc."""
        # Clean text fields
        cleaned_df = self._clean_text(news_df)

        # Apply source-specific processing
        if 'source' in news_df.columns:
            cleaned_df = self._process_reddit_content(cleaned_df)

        # Enrich with additional features
        enriched_df = self._enrich_data(cleaned_df)

        # Score relevance based on keywords and categories
        scored_df = self._score_relevance(enriched_df)

        # Adjust final relevance score based on engagement
        final_df = scored_df.withColumn(
            "relevance_score",
            when(col("engagement_score").isNotNull(),
                 (col("relevance_score") * 0.7) + (col("engagement_score") * 0.3))
            .otherwise(col("relevance_score"))
        )

        return final_df
    def _clean_text(self, df: DataFrame) -> DataFrame:
        """Clean and normalize text fields"""

        # Remove HTML tags
        def remove_html(text):
            if text:
                return re.sub(r'<.*?>', '', text)
            return text

        remove_html_udf = udf(remove_html, StringType())

        return df.withColumn("title_clean", remove_html_udf(col("title"))) \
            .withColumn("summary_clean", remove_html_udf(col("summary"))) \
            .withColumn("title_clean", regexp_replace(col("title_clean"), r'[^\w\s]', '')) \
            .withColumn("summary_clean", regexp_replace(col("summary_clean"), r'[^\w\s]', ''))

    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with additional features"""

        # Add sentiment analysis (simplified example)
        def simple_sentiment(text):
            if not text:
                return 0.0

            positive_words = ['breakthrough', 'discovery', 'advance', 'success', 'positive',
                              'improvement', 'gain', 'rise', 'grow', 'increase']
            negative_words = ['decline', 'fail', 'crash', 'negative', 'problem', 'issue',
                              'decrease', 'drop', 'fall', 'loss']

            text_lower = text.lower()
            positive_count = sum(1 for word in positive_words if word in text_lower)
            negative_count = sum(1 for word in negative_words if word in text_lower)

            if positive_count + negative_count == 0:
                return 0.0

            return (positive_count - negative_count) / (positive_count + negative_count)

        sentiment_udf = udf(simple_sentiment, FloatType())

        # Categorize news
        def categorize_science_subtopic(title, summary):
            if not title and not summary:
                return "general"

            combined = (title or "") + " " + (summary or "")
            combined = combined.lower()

            if any(term in combined for term in ['ai', 'artificial intelligence', 'machine learning', 'neural']):
                return "ai_ml"
            elif any(term in combined for term in ['physics', 'quantum', 'particle', 'energy']):
                return "physics"
            elif any(term in combined for term in ['bio', 'gene', 'cell', 'medicine', 'health']):
                return "biology_medicine"
            elif any(term in combined for term in ['space', 'nasa', 'galaxy', 'planet', 'astronomy']):
                return "space"
            elif any(term in combined for term in ['climate', 'environment', 'earth', 'sustainable']):
                return "climate_environment"
            else:
                return "other_science"

        categorize_udf = udf(categorize_science_subtopic, StringType())

        return df.withColumn("sentiment", sentiment_udf(col("summary_clean"))) \
            .withColumn("science_subtopic",
                        when(col("category") == "science",
                             categorize_udf(col("title_clean"), col("summary_clean")))
                        .otherwise(None))

    def _score_relevance(self, df: DataFrame, user_interests: List[str] = None) -> DataFrame:
        """Score relevance of news items based on user interests"""
        # Default interests if none specified
        if not user_interests:
            user_interests = [
                "data engineering", "big data", "etl", "spark", "hadoop",
                "artificial intelligence", "machine learning", "cryptocurrency",
                "blockchain", "python", "programming"
            ]

        # Calculate interest match score
        def interest_score(title, summary):
            if not title and not summary:
                return 0.0

            combined = ((title or "") + " " + (summary or "")).lower()
            match_count = sum(1 for interest in user_interests if interest.lower() in combined)
            return min(match_count / 3, 1.0)  # Normalize to max of 1.0

        interest_udf = udf(interest_score, FloatType())

        # Calculate final relevance score combining multiple factors
        return df.withColumn("interest_score", interest_udf(col("title_clean"), col("summary_clean"))) \
            .withColumn("relevance_score",
                        (col("interest_score") * 0.6) +
                        (when(col("sentiment").isNotNull(), col("sentiment").abs() * 0.4).otherwise(0.0)))
