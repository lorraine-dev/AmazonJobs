"""
Core scraping functionality for Amazon Jobs Scraper
"""

from .amazon_scraper import AmazonJobsScraper
from .config import ScraperConfig

__all__ = ["AmazonJobsScraper", "ScraperConfig"]
