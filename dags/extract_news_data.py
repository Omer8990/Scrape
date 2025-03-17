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
def extract_enhanced_scholarly_data(**kwargs):
    """Extract tech, science, and innovation articles from reliable sources that Tony Stark would find interesting,
    plus Israeli news in Hebrew"""
    import requests
    import random
    import time
    import re
    from datetime import datetime
    from bs4 import BeautifulSoup
    print("Starting enhanced scholarly and tech news extraction...")

    # Sources organized by category - updated with more reliable sources
    sources = [
        # Tech Innovation sources
        {
            'name': 'HackerNews',
            'url': 'https://news.ycombinator.com/',
            'category': 'tech_innovation',
            'article_selector': '.athing',
            'title_selector': '.titleline > a',
            'author_selector': '.hnuser',
            'abstract_selector': '.subtext',
            'link_prefix': '',  # Absolute URLs
            'language': 'en'
        },
        {
            'name': 'ArXiv CS',
            'url': 'https://arxiv.org/list/cs.AI/recent',
            'category': 'ai_research',
            'article_selector': 'dl',
            'title_selector': 'dt .list-title',
            'author_selector': 'dd.meta',
            'abstract_selector': 'p.mathjax',
            'link_prefix': 'https://arxiv.org',
            'language': 'en'
        },
        {
            'name': 'Nature',
            'url': 'https://www.nature.com/nature/articles',
            'category': 'science',
            'article_selector': 'article',
            'title_selector': 'h3 a',
            'author_selector': 'ul.c-author-list',
            'abstract_selector': 'div.c-card__summary',
            'link_prefix': 'https://www.nature.com',
            'language': 'en'
        },
        {
            'name': 'TechCrunch',
            'url': 'https://techcrunch.com/',
            'category': 'tech_news',
            'article_selector': 'article',
            'title_selector': 'h2 a',
            'author_selector': 'span.river-byline__authors',
            'abstract_selector': 'div.post-block__content',
            'link_prefix': '',  # Absolute URLs
            'language': 'en'
        },
        {
            'name': 'Wired',
            'url': 'https://www.wired.com/category/science/',
            'category': 'science_tech',
            'article_selector': 'div.SummaryItemWrapper-gdEuvf',
            'title_selector': 'h3 a',
            'author_selector': 'span.byline-name',
            'abstract_selector': 'div.summary-item__dek',
            'link_prefix': 'https://www.wired.com',
            'language': 'en'
        },
        {
            'name': 'VentureBeat',
            'url': 'https://venturebeat.com/',
            'category': 'tech_business',
            'article_selector': 'article',
            'title_selector': 'h2 a',
            'author_selector': 'span.author',
            'abstract_selector': 'p.entry-excerpt',
            'link_prefix': '',  # Absolute URLs
            'language': 'en'
        },
        {
            'name': 'The Verge',
            'url': 'https://www.theverge.com/tech',
            'category': 'consumer_tech',
            'article_selector': 'div.duet--content-cards--content-card',
            'title_selector': 'h2 a',
            'author_selector': 'span.duet--attribution--author-name',
            'abstract_selector': 'p.duet--content-cards--content-card-description',
            'link_prefix': 'https://www.theverge.com',
            'language': 'en'
        },
        {
            'name': 'Geektime Israel',
            'url': 'https://www.geektime.co.il/',
            'category': 'israel_tech',
            'article_selector': 'article',
            'title_selector': 'h2 a',
            'author_selector': 'span.author',
            'abstract_selector': 'div.entry-summary',
            'link_prefix': '',  # Absolute URLs
            'language': 'he'
        },
        {
            'name': 'Globes Tech',
            'url': 'https://www.globes.co.il/news/home.aspx?fid=594',
            'category': 'israel_tech',
            'article_selector': 'div.news-item-container',
            'title_selector': 'h3 a',
            'author_selector': 'span.author',
            'abstract_selector': 'p.news-item-content',
            'link_prefix': 'https://www.globes.co.il',
            'language': 'he'
        }
    ]

    # Implement a fallback mechanism for when specific selectors fail
    fallback_selectors = {
        'article_selector': ['article', '.article', '.post', '.item', '.entry', 'div.card', 'div.news-item',
                             'li.news-item'],
        'title_selector': ['h1 a', 'h2 a', 'h3 a', 'h4 a', '.title a', '.headline a', '.entry-title a', 'a.title'],
        'author_selector': ['.author', '.byline', '.meta-author', '.writer', '.contributor', 'span.by'],
        'abstract_selector': ['p', '.summary', '.excerpt', '.description', '.teaser', '.intro', '.snippet']
    }

    articles = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5,he;q=0.3',
        'Referer': 'https://www.google.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

    session = requests.Session()

    for source in sources:
        try:
            print(f"Scraping {source['name']} from {source['url']}...")

            # Add a random delay before each request to avoid rate limiting
            time.sleep(random.uniform(2, 5))

            # Use a session to maintain cookies and improve reliability
            response = session.get(source['url'], headers=headers, timeout=30)

            # Check the response status
            if response.status_code != 200:
                print(f"Failed to access {source['url']}: Status code {response.status_code}")
                continue

            # Parse the HTML content
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find all article elements using the primary selector
            article_elements = soup.select(source['article_selector'])

            # If no elements found, try fallback selectors
            if not article_elements:
                for fallback_selector in fallback_selectors['article_selector']:
                    article_elements = soup.select(fallback_selector)
                    if article_elements:
                        print(f"Used fallback article selector '{fallback_selector}' for {source['name']}")
                        break

            print(f"Found {len(article_elements)} article elements for {source['name']}")

            # If still no elements found, try a more general approach
            if not article_elements:
                # Look for common article patterns
                article_elements = soup.find_all(['article', 'div', 'li'], class_=lambda c: c and any(
                    pattern in c.lower() for pattern in ['post', 'article', 'item', 'entry', 'card', 'news']))
                print(f"Found {len(article_elements)} article elements using general patterns for {source['name']}")

            # Process articles
            for article_element in article_elements[:10]:  # Get up to 10 articles per source
                try:
                    # Extract title using primary selector
                    title_element = article_element.select_one(source['title_selector'])

                    # If no title found, try fallback selectors
                    if not title_element:
                        for fallback_selector in fallback_selectors['title_selector']:
                            title_element = article_element.select_one(fallback_selector)
                            if title_element:
                                break

                    # If still no title found, look for any heading with text
                    if not title_element:
                        title_element = article_element.find(['h1', 'h2', 'h3', 'h4', 'h5'])

                    # Skip if no title found or title is empty
                    if not title_element or not title_element.get_text().strip():
                        continue

                    title = title_element.get_text().strip()

                    # Extract link
                    link = None
                    # Try to get link from title element
                    if hasattr(title_element, 'get') and title_element.get('href'):
                        link = title_element.get('href')
                    # Try to get link from parent of title element
                    elif title_element.parent and hasattr(title_element.parent, 'get') and title_element.parent.get(
                            'href'):
                        link = title_element.parent.get('href')
                    # Try to find any link in the article element
                    else:
                        link_element = article_element.find('a')
                        if link_element and hasattr(link_element, 'get'):
                            link = link_element.get('href')

                    # Skip if no link found
                    if not link:
                        print(f"No link found for article '{title[:30]}...' in {source['name']}")
                        continue

                    # Fix relative URLs
                    if link and not link.startswith(('http://', 'https://')):
                        link = source['link_prefix'] + (link if link.startswith('/') else '/' + link)

                    # Extract author/source info
                    author_element = article_element.select_one(source['author_selector'])

                    # If no author found, try fallback selectors
                    if not author_element:
                        for fallback_selector in fallback_selectors['author_selector']:
                            author_element = article_element.select_one(fallback_selector)
                            if author_element:
                                break

                    authors = author_element.get_text().strip() if author_element else f"Unknown author - {source['name']}"

                    # Extract abstract/summary
                    abstract_element = article_element.select_one(source['abstract_selector'])

                    # If no abstract found, try fallback selectors
                    if not abstract_element:
                        for fallback_selector in fallback_selectors['abstract_selector']:
                            abstract_element = article_element.select_one(fallback_selector)
                            if abstract_element:
                                break

                    # If still no abstract, use any paragraph text
                    if not abstract_element:
                        abstract_element = article_element.find('p')

                    abstract = abstract_element.get_text().strip() if abstract_element else f"No abstract available - {source['name']}"

                    # Limit abstract length
                    abstract = abstract[:1000] if abstract else f"No abstract available - {source['name']}"

                    # Try to extract published date
                    date_element = article_element.select_one('.date, .time, time, .published, .post-date')

                    # If no date element found, try looking for any elements with 'date' in their class name
                    if not date_element:
                        date_elements = article_element.find_all(class_=lambda c: c and 'date' in c.lower())
                        if date_elements:
                            date_element = date_elements[0]

                    # Use the current date if no date element found
                    published_date = date_element.get_text().strip() if date_element else datetime.now().isoformat()

                    # Ensure the date is in ISO format
                    if not re.match(r'\d{4}-\d{2}-\d{2}', published_date):
                        published_date = datetime.now().isoformat()

                    print(
                        f"Successfully extracted: '{title[:50]}{'...' if len(title) > 50 else ''}' from {source['name']}")

                    articles.append({
                        'source': source['name'],
                        'category': source['category'],
                        'title': title,
                        'authors': authors,
                        'abstract': abstract,
                        'url': link,
                        'published_date': published_date,
                        'extracted_date': datetime.now().isoformat(),
                        'language': source['language']
                    })

                except Exception as e:
                    print(f"Error processing individual article from {source['name']}: {str(e)}")
                    continue

        except Exception as e:
            print(f"Error scraping {source['name']}: {str(e)}")
            continue

    # Additional sources - RSS feeds which are more reliable than web scraping
    rss_sources = [
        {
            'name': 'ArXiv AI',
            'url': 'http://export.arxiv.org/rss/cs.AI',
            'category': 'ai_research',
            'language': 'en'
        },
        {
            'name': 'MIT Technology Review',
            'url': 'https://www.technologyreview.com/feed/',
            'category': 'tech_innovation',
            'language': 'en'
        },
        {
            'name': 'ScienceDaily',
            'url': 'https://www.sciencedaily.com/rss/computers_math/artificial_intelligence.xml',
            'category': 'ai_news',
            'language': 'en'
        },
        {
            'name': 'IEEE Spectrum',
            'url': 'https://spectrum.ieee.org/feeds/feed.rss',
            'category': 'computing',
            'language': 'en'
        }
    ]

    # Process RSS feeds which are more reliable
    for source in rss_sources:
        try:
            print(f"Fetching RSS feed from {source['name']} at {source['url']}...")

            # Add a random delay
            time.sleep(random.uniform(2, 4))

            response = session.get(source['url'], headers=headers, timeout=30)

            if response.status_code != 200:
                print(f"Failed to access RSS feed {source['url']}: Status code {response.status_code}")
                continue

            # Parse the RSS feed
            soup = BeautifulSoup(response.content, 'xml')
            if not soup.find('item'):  # If not XML, try parsing as HTML
                soup = BeautifulSoup(response.content, 'html.parser')

            # Find all items in the RSS feed
            items = soup.find_all('item')
            print(f"Found {len(items)} items in RSS feed from {source['name']}")

            for item in items[:10]:  # Process up to 10 items
                try:
                    # Extract title
                    title_tag = item.find('title')
                    if not title_tag or not title_tag.text.strip():
                        continue
                    title = title_tag.text.strip()

                    # Extract link
                    link_tag = item.find('link')
                    if not link_tag or not link_tag.text.strip():
                        continue
                    link = link_tag.text.strip()

                    # Extract author
                    author_tag = item.find(['author', 'dc:creator'])
                    authors = author_tag.text.strip() if author_tag else f"Unknown author - {source['name']}"

                    # Extract description/abstract
                    description_tag = item.find(['description', 'content:encoded', 'summary'])
                    abstract = description_tag.text.strip() if description_tag else f"No abstract available - {source['name']}"

                    # Clean HTML from abstract if present
                    if '<' in abstract and '>' in abstract:
                        abstract_soup = BeautifulSoup(abstract, 'html.parser')
                        abstract = abstract_soup.get_text().strip()

                    # Limit abstract length
                    abstract = abstract[:1000]

                    # Extract publication date
                    date_tag = item.find(['pubDate', 'dc:date', 'published'])
                    published_date = date_tag.text.strip() if date_tag else datetime.now().isoformat()

                    # Try to convert to ISO format if not already
                    try:
                        if not re.match(r'\d{4}-\d{2}-\d{2}', published_date):
                            parsed_date = datetime.strptime(published_date, "%a, %d %b %Y %H:%M:%S %z")
                            published_date = parsed_date.isoformat()
                    except:
                        published_date = datetime.now().isoformat()

                    print(
                        f"Successfully extracted RSS item: '{title[:50]}{'...' if len(title) > 50 else ''}' from {source['name']}")

                    articles.append({
                        'source': source['name'],
                        'category': source['category'],
                        'title': title,
                        'authors': authors,
                        'abstract': abstract,
                        'url': link,
                        'published_date': published_date,
                        'extracted_date': datetime.now().isoformat(),
                        'language': source['language']
                    })

                except Exception as e:
                    print(f"Error processing RSS item from {source['name']}: {str(e)}")
                    continue

        except Exception as e:
            print(f"Error fetching RSS feed from {source['name']}: {str(e)}")
            continue

    print(f"Total articles extracted: {len(articles)}")

    # Only proceed with database operations if we have articles
    if not articles:
        print("No articles were extracted. Skipping database operations.")
        return "No scholarly articles were extracted. Check logs for details."

    # Store to PostgreSQL
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Check if language column exists, and if not, add it
        try:
            cursor.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name='scholarly_articles' AND column_name='language'")
            language_column_exists = cursor.fetchone() is not None

            if not language_column_exists:
                cursor.execute("ALTER TABLE scholarly_articles ADD COLUMN language VARCHAR(10) DEFAULT 'en'")
                conn.commit()
                print("Added language column to scholarly_articles table")
        except Exception as e:
            print(f"Error checking or adding language column: {str(e)}")
            # Continue anyway, worst case the language data won't be stored

        inserted_count = 0
        for article in articles:
            try:
                # If language column exists, include it in the INSERT
                if language_column_exists:
                    cursor.execute(
                        """
                        INSERT INTO scholarly_articles 
                        (source, category, title, authors, abstract, url, published_date, extracted_date, language)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            article['source'],
                            article['category'],
                            article['title'],
                            article['authors'],
                            article['abstract'],
                            article['url'],
                            article['published_date'],
                            article['extracted_date'],
                            article['language']
                        )
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO scholarly_articles 
                        (source, category, title, authors, abstract, url, published_date, extracted_date)
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
                inserted_count += 1
            except Exception as e:
                print(f"Error inserting article '{article['title'][:30]}...': {str(e)}")
                continue

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Successfully stored {inserted_count} articles in database")
    except Exception as e:
        print(f"Database error: {str(e)}")
        return f"Failed to store articles in database: {str(e)}"

    return f"Successfully extracted and stored {len(articles)} scholarly and tech articles from reliable sources"

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
    python_callable=extract_enhanced_scholarly_data,
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