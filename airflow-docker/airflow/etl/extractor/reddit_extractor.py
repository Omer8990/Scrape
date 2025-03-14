import logging
import praw
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta


class RedditExtractor:
    """Extract posts from Reddit subreddits"""

    def __init__(
            self,
            client_id: str,
            client_secret: str,
            user_agent: str = "PersonalNewsETL/1.0"
    ):
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # Initialize Reddit API client
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )

    def extract_from_subreddit(
            self,
            subreddit_name: str,
            category: str,
            time_filter: str = "day",
            limit: int = 25,
            min_score: int = 50,
            min_comments: int = 10
    ) -> List[Dict]:
        """
        Extract top posts from a subreddit

        Args:
            subreddit_name: Name of the subreddit (without r/)
            category: Category to assign to the posts (e.g., "science", "cryptocurrency")
            time_filter: One of "hour", "day", "week", "month", "year", "all"
            limit: Maximum number of posts to retrieve
            min_score: Minimum upvotes/score required
            min_comments: Minimum comments required

        Returns:
            List of standardized post dictionaries
        """
        results = []

        try:
            # Get subreddit instance
            subreddit = self.reddit.subreddit(subreddit_name)

            # Get top posts
            for post in subreddit.top(time_filter=time_filter, limit=limit):
                # Filter by minimum score and comments
                if post.score < min_score or post.num_comments < min_comments:
                    continue

                # Convert to timestamps
                created_at = datetime.fromtimestamp(post.created_utc)

                # Format into standardized dictionary
                results.append({
                    'id': f"reddit_{post.id}",
                    'title': post.title,
                    'source': f"reddit_r/{subreddit_name}",
                    'category': category,
                    'url': f"https://www.reddit.com{post.permalink}",
                    'published_date': created_at.isoformat(),
                    'summary': post.selftext[
                               :500] if post.selftext else f"Post by u/{post.author.name} with {post.score} upvotes and {post.num_comments} comments",
                    'raw_data': {
                        'id': post.id,
                        'score': post.score,
                        'num_comments': post.num_comments,
                        'subreddit': subreddit_name,
                        'author': post.author.name if post.author else "[deleted]",
                        'created_utc': post.created_utc,
                        'is_self': post.is_self,
                        'domain': post.domain,
                        'permalink': post.permalink,
                        'link_flair_text': post.link_flair_text
                    }
                })

            self.logger.info(f"Extracted {len(results)} posts from r/{subreddit_name}")
            return results

        except Exception as e:
            self.logger.error(f"Error extracting from r/{subreddit_name}: {str(e)}")
            return []

    def extract_multi_subreddits(self, subreddit_config: List[Dict]) -> List[Dict]:
        """
        Extract posts from multiple subreddits

        Args:
            subreddit_config: List of dictionaries with subreddit configs
                Example: [
                    {
                        "name": "science",
                        "category": "science",
                        "min_score": 100
                    },
                    {
                        "name": "cryptocurrency",
                        "category": "cryptocurrency",
                        "time_filter": "day"
                    }
                ]

        Returns:
            Combined list of posts from all subreddits
        """
        all_posts = []

        for config in subreddit_config:
            # Extract required parameters
            subreddit_name = config.pop("name")
            category = config.pop("category", subreddit_name)  # Default to subreddit name

            # Extract posts using remaining parameters as kwargs
            posts = self.extract_from_subreddit(
                subreddit_name=subreddit_name,
                category=category,
                **config
            )

            all_posts.extend(posts)

        return all_posts

    def extract_science(self) -> List[Dict]:
        """Extract posts from science-related subreddits"""
        subreddit_config = [
            {"name": "science", "category": "science", "min_score": 500},
            {"name": "askscience", "category": "science", "min_score": 200},
            {"name": "physics", "category": "science", "min_score": 100},
            {"name": "biology", "category": "science", "min_score": 100},
            {"name": "space", "category": "science", "min_score": 300},
            {"name": "artificial", "category": "science", "min_score": 100},
            {"name": "machinelearning", "category": "science", "min_score": 200}
        ]

        return self.extract_multi_subreddits(subreddit_config)

    def extract_crypto(self) -> List[Dict]:
        """Extract posts from cryptocurrency-related subreddits"""
        subreddit_config = [
            {"name": "cryptocurrency", "category": "cryptocurrency", "min_score": 300},
            {"name": "CryptoMarkets", "category": "cryptocurrency", "min_score": 100},
            {"name": "bitcoin", "category": "cryptocurrency", "min_score": 300},
            {"name": "ethereum", "category": "cryptocurrency", "min_score": 200}
        ]

        return self.extract_multi_subreddits(subreddit_config)

    def extract_tech(self) -> List[Dict]:
        """Extract posts from tech and programming subreddits"""
        subreddit_config = [
            {"name": "programming", "category": "technology", "min_score": 300},
            {"name": "python", "category": "technology", "min_score": 200},
            {"name": "dataengineering", "category": "technology", "min_score": 100},
            {"name": "datascience", "category": "technology", "min_score": 200},
            {"name": "devops", "category": "technology", "min_score": 100},
            {"name": "linux", "category": "technology", "min_score": 200}
        ]

        return self.extract_multi_subreddits(subreddit_config)
