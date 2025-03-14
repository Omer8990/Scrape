import requests
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional


class BaseAPIExtractor:
    """Base class for API extractors with common functionality"""

    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url
        self.api_key = api_key
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict:
        """Make API request with error handling and retries"""
        if params is None:
            params = {}

        if self.api_key:
            params['api_key'] = self.api_key

        try:
            response = requests.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            # In a production system, we might implement retries here
            raise

    def extract(self) -> List[Dict]:
        """To be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement extract()")


class ScienceNewsExtractor(BaseAPIExtractor):
    """Extract science news from a specific API"""

    def extract(self) -> List[Dict]:
        """Extract recent science articles"""
        results = []
        raw_data = self._make_request("articles", {
            "category": "science",
            "published_after": (datetime.now().date().isoformat())
        })

        # Transform into standard format
        for item in raw_data.get('articles', []):
            results.append({
                'id': item.get('id', ''),
                'title': item.get('title', ''),
                'source': 'science_news_api',
                'category': 'science',
                'url': item.get('url', ''),
                'published_date': item.get('published_at', ''),
                'summary': item.get('summary', ''),
                'raw_data': item  # Keep raw data for future processing
            })

        self.logger.info(f"Extracted {len(results)} science news articles")
        return results


class CryptoMarketExtractor(BaseAPIExtractor):
    """Extract cryptocurrency data from market APIs"""

    def extract(self) -> List[Dict]:
        """Extract crypto market updates"""
        results = []
        # Get top coins by market cap
        raw_data = self._make_request("cryptocurrency/listings/latest", {
            "limit": 20,
            "convert": "USD"
        })

        for coin in raw_data.get('data', []):
            # Calculate 24h price change percentage
            price_change_24h = coin.get('quote', {}).get('USD', {}).get('percent_change_24h', 0)

            # Only include significant price movements (>5%) or top 10 coins
            if abs(price_change_24h) > 5.0 or coin.get('cmc_rank', 99) <= 10:
                results.append({
                    'id': f"crypto_{coin.get('id', '')}",
                    'title': f"{coin.get('name', '')} ({coin.get('symbol', '')}) {price_change_24h:.2f}% in 24h",
                    'source': 'crypto_market_api',
                    'category': 'cryptocurrency',
                    'url': f"https://coinmarketcap.com/currencies/{coin.get('slug', '')}",
                    'published_date': datetime.now().isoformat(),
                    'summary': f"Current price: ${coin.get('quote', {}).get('USD', {}).get('price', 0):.2f}, " +
                               f"Market Cap: ${coin.get('quote', {}).get('USD', {}).get('market_cap', 0) / 1000000:.2f}M",
                    'raw_data': coin
                })

        self.logger.info(f"Extracted {len(results)} cryptocurrency updates")
        return results
