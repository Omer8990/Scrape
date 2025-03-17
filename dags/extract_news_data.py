from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import feedparser
import json
import os
from bs4 import BeautifulSoup
import pandas as pd
from dotenv import load_dotenv
import time
import random
import re

# Load environment variables
load_dotenv()

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'extract_news_data_v2',
    default_args=default_args,
    description='Extract news data from various sources without Reddit API',
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Create PostgreSQL tables
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql='''
        CREATE TABLE IF NOT EXISTS news_articles (
            id SERIAL PRIMARY KEY,
            source VARCHAR(255),
            category VARCHAR(255),
            title TEXT,
            content TEXT,
            url TEXT,
            published_date TIMESTAMP,
            extracted_date TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reddit_posts (
            id SERIAL PRIMARY KEY,
            subreddit VARCHAR(255),
            title TEXT,
            author VARCHAR(255),
            url TEXT,
            score INTEGER,
            created_utc TIMESTAMP,
            extracted_date TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS scholarly_articles (
            id SERIAL PRIMARY KEY,
            source VARCHAR(255),
            category VARCHAR(255),
            title TEXT,
            authors TEXT,
            abstract TEXT,
            url TEXT,
            published_date TIMESTAMP,
            extracted_date TIMESTAMP
        );
    ''',
    dag=dag,
)


# Function to extract news from RSS feeds (unchanged from original)
def extract_rss_news(**kwargs):
    rss_feeds = {
        'science': [
            'http://feeds.nature.com/nature/rss/current',
            'http://feeds.sciencedaily.com/sciencedaily',
        ],
        'technology': [
            'https://www.wired.com/feed/rss',
            'https://feeds.feedburner.com/TechCrunch',
        ],
        'crypto': [
            'https://cointelegraph.com/rss',
            'https://coindesk.com/arc/outboundfeeds/rss/',
        ],
        'stock_market': [
            'https://seekingalpha.com/feed.xml',
            'https://www.investing.com/rss/news.rss',
        ],
        'general_news': [
            'http://rss.cnn.com/rss/cnn_topstories.rss',
            'https://www.npr.org/rss/rss.php?id=1001',
        ],
    }

    articles = []

    for category, feeds in rss_feeds.items():
        for feed_url in feeds:
            try:
                feed = feedparser.parse(feed_url)
                source = feed.feed.title if hasattr(feed.feed, 'title') else 'Unknown'

                for entry in feed.entries[:10]:  # Get top 10 articles
                    title = entry.title if hasattr(entry, 'title') else 'No title'
                    content = entry.summary if hasattr(entry, 'summary') else 'No content'
                    url = entry.link if hasattr(entry, 'link') else 'No URL'
                    published_date = entry.published if hasattr(entry, 'published') else datetime.now().isoformat()

                    articles.append({
                        'source': source,
                        'category': category,
                        'title': title,
                        'content': content,
                        'url': url,
                        'published_date': published_date,
                        'extracted_date': datetime.now().isoformat()
                    })
            except Exception as e:
                print(f"Error extracting from {feed_url}: {e}")

    # Store to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for article in articles:
        cursor.execute(
            """
            INSERT INTO news_articles (source, category, title, content, url, published_date, extracted_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                article['source'],
                article['category'],
                article['title'],
                article['content'],
                article['url'],
                article['published_date'],
                article['extracted_date']
            )
        )

    conn.commit()
    cursor.close()
    conn.close()

    return f"Extracted {len(articles)} articles from RSS feeds"


# Replacement function for Reddit API - using HackerNews API
def extract_hackernews_data(**kwargs):
    """Extract top stories from HackerNews API"""

    # HackerNews API endpoints
    top_stories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    item_url = "https://hacker-news.firebaseio.com/v0/item/{}.json"

    # Get list of top story IDs
    response = requests.get(top_stories_url)
    story_ids = response.json()[:50]  # Get top 50 stories

    posts = []
    categories = ['technology', 'programming', 'science', 'business', 'general']

    # Function to guess category based on title keywords
    def guess_category(title):
        title_lower = title.lower()
        if any(term in title_lower for term in ['code', 'programming', 'developer', 'software', 'git']):
            return 'programming'
        elif any(term in title_lower for term in ['ai', 'tech', 'computer', 'app', 'digital']):
            return 'technology'
        elif any(term in title_lower for term in ['science', 'research', 'study', 'physics', 'biology']):
            return 'science'
        elif any(term in title_lower for term in ['startup', 'business', 'money', 'finance', 'market']):
            return 'business'
        else:
            return 'general'

    # Fetch details for each story
    for story_id in story_ids:
        try:
            response = requests.get(item_url.format(story_id))
            item = response.json()

            # Skip items that aren't stories or don't have required fields
            if 'title' not in item:
                continue

            category = guess_category(item['title'])

            posts.append({
                'subreddit': category,  # Using category as "subreddit" for compatibility
                'title': item['title'],
                'author': item.get('by', 'anonymous'),
                'url': item.get('url', f"https://news.ycombinator.com/item?id={story_id}"),
                'score': item.get('score', 0),
                'created_utc': datetime.fromtimestamp(item.get('time', 0)).isoformat(),
                'extracted_date': datetime.now().isoformat()
            })
        except Exception as e:
            print(f"Error extracting HackerNews item {story_id}: {e}")

    # Store to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for post in posts:
        cursor.execute(
            """
            INSERT INTO reddit_posts (subreddit, title, author, url, score, created_utc, extracted_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                post['subreddit'],
                post['title'],
                post['author'],
                post['url'],
                post['score'],
                post['created_utc'],
                post['extracted_date']
            )
        )

    conn.commit()
    cursor.close()
    conn.close()

    return f"Extracted {len(posts)} posts from HackerNews"


# Alternative: Reddit scraper using BeautifulSoup (no API required)
def scrape_reddit_data(**kwargs):
    """Scrape Reddit using BeautifulSoup instead of the API"""

    subreddits = [
        'science', 'technology', 'CryptoCurrency', 'wallstreetbets',
        'investing', 'news', 'worldnews', 'datascience'
    ]

    posts = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for subreddit in subreddits:
        try:
            # Access the subreddit
            url = f"https://old.reddit.com/r/{subreddit}/hot/"
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed to access r/{subreddit}: Status code {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all posts (each post is in a div with class "thing")
            post_elements = soup.find_all('div', class_='thing')[:10]  # Get top 10 posts

            for post in post_elements:
                try:
                    # Extract post data
                    title_element = post.find('a', class_='title')
                    title = title_element.text if title_element else "No title"

                    url = title_element['href'] if title_element and 'href' in title_element.attrs else ""
                    if url and not url.startswith(('http://', 'https://')):
                        url = f"https://reddit.com{url}"

                    author_element = post.find('a', class_='author')
                    author = author_element.text if author_element else "Unknown"

                    score_element = post.find('div', class_='score unvoted')
                    score_text = score_element.get('title', '0') if score_element else '0'
                    try:
                        score = int(score_text)
                    except ValueError:
                        score = 0

                    # Using current time as a fallback since we can't easily get the exact post time
                    created_utc = datetime.now().isoformat()

                    posts.append({
                        'subreddit': subreddit,
                        'title': title,
                        'author': author,
                        'url': url,
                        'score': score,
                        'created_utc': created_utc,
                        'extracted_date': datetime.now().isoformat()
                    })
                except Exception as e:
                    print(f"Error parsing post in r/{subreddit}: {e}")

            # Add a small delay to avoid hitting rate limits
            time.sleep(random.uniform(1, 2))

        except Exception as e:
            print(f"Error scraping r/{subreddit}: {e}")

    # Store to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for post in posts:
        cursor.execute(
            """
            INSERT INTO reddit_posts (subreddit, title, author, url, score, created_utc, extracted_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                post['subreddit'],
                post['title'],
                post['author'],
                post['url'],
                post['score'],
                post['created_utc'],
                post['extracted_date']
            )
        )

    conn.commit()
    cursor.close()
    conn.close()

    return f"Scraped {len(posts)} posts from Reddit"

# Improved news website scraper with better article extraction
def improved_news_scraper(**kwargs):
    """Enhanced version of news website scraper with better article detection"""

    news_websites = {
        'technology': [
            {'url': 'https://techcrunch.com', 'article_selector': 'article', 'title_selector': 'h2, h3', 'link_selector': 'a'},
            {'url': 'https://www.theverge.com', 'article_selector': 'h2', 'title_selector': None, 'link_selector': 'a'},
        ],
        'science': [
            {'url': 'https://www.scientificamerican.com/latest-news/', 'article_selector': 'article', 'title_selector': 'h2', 'link_selector': 'a'},
            {'url': 'https://phys.org', 'article_selector': 'article', 'title_selector': 'h3', 'link_selector': 'a'},
        ],
        'crypto': [
            {'url': 'https://decrypt.co/', 'article_selector': 'article', 'title_selector': 'h3', 'link_selector': 'a'},
            {'url': 'https://www.coindesk.com', 'article_selector': 'article', 'title_selector': 'h3, h4', 'link_selector': 'a'},
        ],
        'stock_market': [
            {'url': 'https://www.marketwatch.com', 'article_selector': 'div.article-wrap', 'title_selector': 'h3', 'link_selector': 'a'},
            {'url': 'https://www.investors.com/news/', 'article_selector': 'article', 'title_selector': 'h3', 'link_selector': 'a'},
        ],
    }

    articles = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for category, websites in news_websites.items():
        for site_config in websites:
            website = site_config['url']
            article_selector = site_config['article_selector']
            title_selector = site_config['title_selector']
            link_selector = site_config['link_selector']

            try:
                print(f"Scraping {website}...")
                response = requests.get(website, headers=headers)

                if response.status_code != 200:
                    print(f"Failed to access {website}: Status code {response.status_code}")
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')

                # Find article elements
                article_elements = soup.select(article_selector)[:10]  # Get up to 10 articles

                for article_element in article_elements:
                    try:
                        # Extract title
                        if title_selector:
                            title_element = article_element.select_one(title_selector)
                            title = title_element.get_text().strip() if title_element else None
                        else:
                            title = article_element.get_text().strip()

                        # Skip if no valid title found
                        if not title or len(title) < 10:
                            continue

                        # Extract link
                        link_element = article_element.select_one(link_selector) if link_selector else article_element
                        url = link_element.get('href') if link_element and 'href' in link_element.attrs else None

                        # Skip if no valid URL found
                        if not url:
                            continue

                        # Fix relative URLs
                        if not url.startswith(('http://', 'https://')):
                            base_url = re.match(r'(https?://[^/]+)', website).group(1)
                            url = base_url + (url if url.startswith('/') else '/' + url)

                        # Extract brief content (if available)
                        content_element = article_element.select_one('p, .summary, .excerpt')
                        content = content_element.get_text().strip() if content_element else "No content preview available"

                        articles.append({
                            'source': website,
                            'category': category,
                            'title': title,
                            'content': content[:500],  # Limit content length
                            'url': url,
                            'published_date': datetime.now().isoformat(),  # Use current time as fallback
                            'extracted_date': datetime.now().isoformat()
                        })
                    except Exception as e:
                        print(f"Error extracting article from {website}: {e}")

                # Add a small delay to avoid hitting rate limits
                time.sleep(random.uniform(1, 3))

            except Exception as e:
                print(f"Error scraping {website}: {e}")

    # Store to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for article in articles:
        cursor.execute(
            """
            INSERT INTO news_articles (source, category, title, content, url, published_date, extracted_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                article['source'],
                article['category'],
                article['title'],
                article['content'],
                article['url'],
                article['published_date'],
                article['extracted_date']
            )
        )

    conn.commit()
    cursor.close()
    conn.close()

    return f"Extracted {len(articles)} articles from news websites"

# New function to extract scholarly articles
def extract_scholarly_articles(**kwargs):
    """Extract recent scholarly articles from various open sources"""
    # Sources for scholarly articles
    sources = [
        {
            'name': 'arXiv',
            'url': 'https://arxiv.org/list/cs/recent',
            'category': 'computer_science',
            'article_selector': 'dl',
            'title_selector': 'dt .list-title',
            'author_selector': 'div.list-authors',
            'abstract_selector': 'p.mathjax',
            'link_selector': 'a[title="Abstract"]',
            'base_url': 'https://arxiv.org'
        },
        {
            'name': 'arXiv',
            'url': 'https://arxiv.org/list/physics/recent',
            'category': 'physics',
            'article_selector': 'dl',
            'title_selector': 'dt .list-title',
            'author_selector': 'div.list-authors',
            'abstract_selector': 'p.mathjax',
            'link_selector': 'a[title="Abstract"]',
            'base_url': 'https://arxiv.org'
        },
        {
            'name': 'PLOS ONE',
            'url': 'https://journals.plos.org/plosone/browse/recent',
            'category': 'multidisciplinary',
            'article_selector': 'article',
            'title_selector': 'h2.title',
            'author_selector': 'div.authors',
            'abstract_selector': 'div.abstract',
            'link_selector': 'a.title-link',
            'base_url': 'https://journals.plos.org'
        },
        {
            'name': 'PubMed Central',
            'url': 'https://www.ncbi.nlm.nih.gov/pmc/about/new-in-pmc/',
            'category': 'biomedical',
            'article_selector': 'div.rprt',
            'title_selector': 'a.rprt_title',
            'author_selector': 'span.rprt_authors',
            'abstract_selector': None,  # Not available in list view
            'link_selector': 'a.rprt_title',
            'base_url': 'https://www.ncbi.nlm.nih.gov'
        }
    ]

    articles = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for source in sources:
        try:
            print(f"Scraping {source['name']} ({source['category']})...")
            response = requests.get(source['url'], headers=headers)

            if response.status_code != 200:
                print(f"Failed to access {source['url']}: Status code {response.status_code}")
                continue

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find article elements
            article_elements = soup.select(source['article_selector'])

            for article_element in article_elements[:10]:  # Get up to 10 articles
                try:
                    # Extract title
                    title_element = article_element.select_one(source['title_selector'])
                    if title_element:
                        title = title_element.get_text().strip()
                        title = title.replace('Title:', '').strip()  # Clean up arXiv titles
                    else:
                        continue  # Skip if no title found

                    # Extract authors
                    author_element = article_element.select_one(source['author_selector'])
                    authors = author_element.get_text().strip() if author_element else "No author information"
                    authors = authors.replace('Authors:', '').strip()  # Clean up

                    # Extract abstract if available
                    abstract = "No abstract available"
                    if source['abstract_selector']:
                        abstract_element = article_element.select_one(source['abstract_selector'])
                        if abstract_element:
                            abstract = abstract_element.get_text().strip()

                    # Extract URL
                    link_element = article_element.select_one(source['link_selector'])
                    if link_element and 'href' in link_element.attrs:
                        url = link_element['href']
                        if not url.startswith(('http://', 'https://')):
                            url = source['base_url'] + (url if url.startswith('/') else '/' + url)
                    else:
                        continue  # Skip if no URL found

                    articles.append({
                        'source': source['name'],
                        'category': source['category'],
                        'title': title,
                        'authors': authors,
                        'abstract': abstract,
                        'url': url,
                        'published_date': datetime.now().isoformat(),  # Use current date as approximate
                        'extracted_date': datetime.now().isoformat()
                    })
                except Exception as e:
                    print(f"Error extracting article from {source['name']}: {e}")

            # Add a small delay to avoid hitting rate limits
            time.sleep(random.uniform(2, 3))

        except Exception as e:
            print(f"Error scraping {source['name']}: {e}")

    # Store to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for article in articles:
        cursor.execute(
            """
            INSERT INTO scholarly_articles (source, category, title, authors, abstract, url, published_date, extracted_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                article['source'],
                article['category'],
                article['title'],
                article['authors'],
                article['abstract'],
                article['url'],
                article['published_date'],
                article['extracted_date']
            )
        )

    conn.commit()
    cursor.close()
    conn.close()

    return f"Extracted {len(articles)} scholarly articles"

# Create PythonOperator tasks
extract_rss_task = PythonOperator(
    task_id='extract_rss_news',
    python_callable=extract_rss_news,
    dag=dag,
)

# Option 1: HackerNews API as replacement for Reddit API
extract_hackernews_task = PythonOperator(
    task_id='extract_hackernews_data',
    python_callable=extract_hackernews_data,
    dag=dag,
)

# Option 2: Web scraping for Reddit
scrape_reddit_task = PythonOperator(
    task_id='scrape_reddit_data',
    python_callable=scrape_reddit_data,
    dag=dag,
)

# Improved web scraping for news websites
improved_news_scraper_task = PythonOperator(
    task_id='improved_news_scraper',
    python_callable=improved_news_scraper,
    dag=dag,
)

# New task for scholarly articles
extract_scholarly_task = PythonOperator(
    task_id='extract_scholarly_articles',
    python_callable=extract_scholarly_articles,
    dag=dag,
)

# Define task dependencies
create_tables >> [
    extract_rss_task,
    extract_hackernews_task,  # Use this OR scrape_reddit_task
    # scrape_reddit_task,     # Uncomment to use Reddit scraping instead of HackerNews
    improved_news_scraper_task,
    extract_scholarly_task
]