#!/usr/bin/env python3
"""
Main execution script for Amazon Jobs Scraper
"""

import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.amazon_scraper import AmazonJobsScraper
from scraper.config import ScraperConfig
from utils.logging_utils import setup_logging, log_scraper_stats
from utils.health_check import check_scraper_health
import time

def main():
    """Main execution function."""
    
    # Setup logging
    logger = setup_logging(
        level="INFO",
        log_file="logs/scraper.log",
        log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    start_time = time.time()
    
    try:
        # Initialize scraper with configuration
        config = ScraperConfig()
        scraper = AmazonJobsScraper(config)
        
        # Run scraper
        result_df = scraper.run()
        
        # Log statistics
        execution_time = time.time() - start_time
        total_jobs = len(result_df)
        active_jobs = result_df['active'].sum() if not result_df.empty else 0
        
        log_scraper_stats(total_jobs, active_jobs, execution_time, logger)
        
        # Health check
        logger.info("Running health check...")
        health_ok = check_scraper_health()
        
        if health_ok:
            logger.info("✅ Scraping completed successfully")
            return 0
        else:
            logger.warning("⚠️  Scraping completed with warnings")
            return 1
        
    except Exception as e:
        logger.error(f"❌ Scraping failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 