"""
Engine interface and factory for Amazon scrapers.

Defines a small Protocol to standardize the run signature and a factory
helper to instantiate the correct engine implementation based on a
string flag ("api"|"selenium").
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Protocol

import pandas as pd

from src.scraper.config import ScraperConfig  # type: ignore
from src.scraper.amazon_api_scraper import AmazonAPIScraper  # type: ignore


class IAmazonScraper(Protocol):
    def run(
        self,
        url: Optional[str] = None,
        out_csv: Optional[Path] = None,
        save_raw: bool = True,
        **kwargs,
    ) -> pd.DataFrame:  # pragma: no cover - Protocol signature only
        ...


def get_amazon_scraper(
    engine: str, config: Optional[ScraperConfig] = None
) -> IAmazonScraper:
    """Return the scraper implementation for the given engine name.

    Args:
        engine: "api" or "selenium" (case-insensitive)
        config: Optional ScraperConfig instance

    Raises:
        ValueError: if the engine name is not recognized
    """
    eng = (engine or "api").lower()
    if eng == "api":
        return AmazonAPIScraper(config)
    if eng == "selenium":
        # Lazy import so core installs don't require selenium deps.
        try:
            from src.scraper.amazon_selenium_scraper import (
                AmazonSeleniumScraper,  # type: ignore
            )
        except Exception as e:
            raise ImportError(
                "Selenium engine requested but selenium dependencies are missing.\n"
                "Install extras with: pip install '.[selenium]'"
            ) from e
        return AmazonSeleniumScraper(config)
    raise ValueError(f"Unknown Amazon engine: {engine}")
