#!/usr/bin/env python3
"""
Main execution script for Amazon Jobs Scraper
"""

import sys
import time
import logging
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

# flake8: noqa: E402
from scraper.amazon_scraper import AmazonJobsScraper  # type: ignore
from scraper.config import ScraperConfig  # type: ignore
from utils.logging_utils import setup_logging  # type: ignore
from utils.health_check import check_scraper_health  # type: ignore
from utils.monitoring import ScraperMetrics  # type: ignore


def main():
    """Main execution function."""

    # Setup logging
    logger = setup_logging(
        level="INFO",
        log_file="logs/scraper.log",
        log_format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Initialize monitoring
    metrics_tracker = ScraperMetrics()

    start_time = time.time()
    result_df = None
    success = False
    error_message = None

    try:
        # Initialize scraper with configuration
        config = ScraperConfig()
        scraper = AmazonJobsScraper(config)

        # Run scraper
        result_df = scraper.run()

        # Get statistics
        execution_time = time.time() - start_time
        total_jobs = len(result_df) if result_df is not None else 0
        active_jobs = (
            result_df["active"].sum()
            if result_df is not None and "active" in result_df.columns
            else 0
        )

        # Health check
        logger.info("Running health check...")
        health_ok = check_scraper_health()

        if health_ok:
            logger.info("✅ Scraping completed successfully")
            success = True
        else:
            logger.warning("⚠️  Scraping completed with warnings")
            success = True  # Consider it a success with warnings

    except Exception as e:
        execution_time = time.time() - start_time
        total_jobs = len(result_df) if result_df is not None else 0
        active_jobs = (
            result_df["active"].sum()
            if result_df is not None and "active" in result_df.columns
            else 0
        )
        error_message = str(e)
        logger.error(f"❌ Scraping failed: {error_message}")
        success = False

    finally:
        # Record metrics at the end of the run
        metrics_tracker.record_execution(
            # Convert total_jobs and active_jobs to standard Python integers
            total_jobs=int(total_jobs) if total_jobs is not None else 0,
            active_jobs=int(active_jobs) if active_jobs is not None else 0,
            execution_time=execution_time,
            success=success,
            error_message=error_message,
        )

    if not success:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
