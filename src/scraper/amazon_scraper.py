"""
Main Amazon Jobs Scraper class
"""

import os
import logging
import pandas as pd
from pathlib import Path
from typing import Optional, Any

from .config import ScraperConfig
from src.scraper.engines import get_amazon_scraper  # type: ignore
from src.utils.paths import (
    get_raw_dir,
    get_backup_dir,
    get_raw_path,
)  # type: ignore


class AmazonJobsScraper:
    """
    Thin delegator for Amazon job listings.

    Responsibilities:
    - Configuration management
    - Directory setup
    - Engine selection (API or Selenium) and delegation
    """

    def __init__(self, config: Optional[ScraperConfig] = None):
        """
        Initialize the Amazon Jobs Scraper.

        Args:
            config: Configuration object (optional)
        """
        self.config = config or ScraperConfig()
        self.logger = logging.getLogger(__name__)

        # Setup directories
        self._setup_directories()

    def _setup_directories(self):
        """Create necessary directories."""
        directories = [
            get_raw_dir(self.config),
            get_backup_dir(self.config),
            "logs",
        ]

        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Directories created: {directories}")

    def run(
        self,
        url: Optional[str] = None,
        out_csv: Optional[Path] = None,
        save_raw: Optional[bool] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Delegator: choose engine (api|selenium) and forward to the corresponding implementation.

        Args:
            url: Optional override for sources.amazon.base_url
            out_csv: Optional explicit output CSV path (defaults to amazon raw path)
            save_raw: Whether to save raw pages/records (engine-specific default if None)
            **kwargs: Passed through to the engine's run()
        """
        engine = (
            os.getenv("AMAZON_ENGINE")
            or self.config.get("sources.amazon.engine", "selenium")
        ).lower()
        self.logger.info(f"Using Amazon engine: {engine}")

        scraper = get_amazon_scraper(engine, self.config)

        if engine == "selenium":
            effective_url = (
                url
                or self.config.get("sources.amazon.html_base_url")
                or self.config.get("sources.amazon.base_url")
            )
        else:
            effective_url = url or self.config.get("sources.amazon.base_url")
        if not effective_url:
            raise ValueError("Missing sources.amazon.base_url for Amazon scraper")

        # Engine-specific default for save_raw when not provided
        if save_raw is None:
            save_raw = (
                bool(self.config.get("sources.amazon.api.save_page_json", True))
                if engine == "api"
                else False
            )

        final_out = out_csv or get_raw_path("amazon", self.config)

        return scraper.run(
            url=effective_url,
            out_csv=final_out,
            save_raw=save_raw,
            **kwargs,
        )

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit (no engine-specific cleanup)."""
        return False
