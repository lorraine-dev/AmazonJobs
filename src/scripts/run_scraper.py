#!/usr/bin/env python3
"""
Main execution script for Job Scrapers
"""

import sys
import os
import time
import logging
import argparse
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# flake8: noqa: E402
from src.scraper.amazon_scraper import AmazonJobsScraper  # type: ignore
from src.scraper.theirstack_scraper import TheirStackScraper  # type: ignore
from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.logging_utils import setup_logging  # type: ignore
from src.utils.health_check import check_scraper_health  # type: ignore
from src.utils.monitoring import ScraperMetrics  # type: ignore
from src.utils.data_processor import process_latest_data  # type: ignore
from src.utils.combine_jobs import update_dashboard_data  # type: ignore


def run_amazon_scraper(config: ScraperConfig) -> Dict[str, Any]:
    """Run the Amazon jobs scraper."""
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting Amazon Jobs Scraper")

    start_time = time.time()
    metrics = {
        "success": False,
        "source": "amazon",
        "jobs_processed": 0,
        "execution_time": 0,
        "error": None,
    }

    try:
        scraper = AmazonJobsScraper(config)
        result_df = scraper.run()

        metrics.update(
            {
                "success": True,
                "jobs_processed": len(result_df) if result_df is not None else 0,
                "execution_time": time.time() - start_time,
            }
        )

        logger.info(
            f"✅ Amazon scraper completed successfully. Processed {metrics['jobs_processed']} jobs in {metrics['execution_time']:.2f} seconds"
        )

    except Exception as e:
        error_msg = f"❌ Amazon scraper failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        metrics["error"] = str(e)

    return metrics


def run_theirstack_scraper(config: ScraperConfig) -> Dict[str, Any]:
    """Run the TheirStack jobs scraper."""
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting TheirStack Jobs Scraper")

    start_time = time.time()
    metrics = {
        "success": False,
        "source": "theirstack",
        "jobs_processed": 0,
        "execution_time": 0,
        "error": None,
    }

    try:
        from src.utils.theirstack_state import TheirStackState

        # Initialize scraper with state management and YAML config
        state = TheirStackState()
        scraper = TheirStackScraper(config)

        # Get new jobs (the scraper will process and persist via processor)
        jobs = scraper.get_new_jobs()

        metrics.update(
            {
                "success": True,
                "jobs_processed": len(jobs) if jobs else 0,
                "execution_time": time.time() - start_time,
            }
        )

        logger.info(
            f"✅ TheirStack scraper completed successfully. Processed {metrics['jobs_processed']} jobs in {metrics['execution_time']:.2f} seconds"
        )

    except Exception as e:
        error_msg = f"❌ TheirStack scraper failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        metrics["error"] = str(e)

    return metrics


def main():
    """Main execution function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run job scrapers")
    parser.add_argument(
        "--source",
        type=str,
        choices=["all", "amazon", "theirstack"],
        default="all",
        help="Which scraper to run (default: all)",
    )
    parser.add_argument(
        "--skip-dashboard",
        action="store_true",
        help="Skip dashboard generation after scraping",
    )
    parser.add_argument(
        "--amazon-engine",
        type=str,
        choices=["selenium", "api"],
        default=None,
        help="Override Amazon scraper engine (selenium|api). Defaults to config or AMAZON_ENGINE env.",
    )
    args = parser.parse_args()

    # Load environment variables from .env for local runs
    load_dotenv()

    # Load config and setup logging from config
    config = ScraperConfig()
    logger = setup_logging(
        level=config.get("common.logging.level", "INFO"),
        log_file=config.get("common.logging.file", "logs/scraper.log"),
        log_format=config.get(
            "common.logging.format",
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        ),
    )

    # Apply CLI engine override if provided
    if args.amazon_engine:
        config.update("sources.amazon.engine", args.amazon_engine)
        logging.getLogger(__name__).info(
            f"Amazon engine overridden via CLI: {args.amazon_engine}"
        )

    # Initialize monitoring
    metrics_tracker = ScraperMetrics()
    results = []

    # Run the requested scrapers
    try:
        # Config already loaded above to configure logging; reuse instance
        # config = ScraperConfig()

        if args.source in ["all", "amazon"]:
            results.append(run_amazon_scraper(config))

        if args.source in ["all", "theirstack"]:
            ts_key = os.getenv("THEIR_STACK_API_KEY")
            if not ts_key:
                msg = "THEIR_STACK_API_KEY not set. Skipping TheirStack run (set it in a .env file or environment)."
                if args.source == "theirstack":
                    logger.error(msg)
                    return 1
                else:
                    logger.warning(msg)
            else:
                results.append(run_theirstack_scraper(config))

        # Combine latest raw files, then generate dashboard if not skipped
        if not args.skip_dashboard:
            logger.info("🔄 Combining latest job files...")
            combined_path = update_dashboard_data()
            if combined_path:
                logger.info(f"✅ Combined jobs updated: {combined_path}")
            else:
                logger.warning(
                    "⚠️ Combine step failed; proceeding if existing combined CSV is present"
                )

            logger.info("🔄 Generating dashboard...")
            dashboard_path = process_latest_data()
            if dashboard_path:
                logger.info(f"✅ Dashboard generated: {dashboard_path}")
            else:
                logger.warning("⚠️ Dashboard generation failed")

        # Log summary
        logger.info("\n📊 Scraping Summary:")
        for result in results:
            status = (
                "✅ Success"
                if result["success"]
                else f"❌ Failed: {result.get('error', 'Unknown error')}"
            )
            logger.info(
                f"- {result['source'].title()}: {status} ({result.get('jobs_processed', 0)} jobs, {result.get('execution_time', 0):.2f}s)"
            )

        # Check if all scrapers were successful
        all_success = all(r["success"] for r in results)
        return 0 if all_success else 1

    except Exception as e:
        logger.error(f"❌ Fatal error in main execution: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    main()
