"""
Amazon Jobs Scraper Package

A production-ready web scraper for Amazon job listings with automated scheduling capabilities.
"""

__version__ = "1.0.0"
__author__ = "Amazon Jobs Scraper Team"
__description__ = "A robust web scraper for Amazon job listings"

# Import main components
try:
    from .scraper.amazon_scraper import AmazonJobsScraper
    from .utils.health_check import check_scraper_health
except ImportError:
    # Allow import even if dependencies aren't installed
    pass

__all__ = [
    'AmazonJobsScraper',
    'check_scraper_health',
] 