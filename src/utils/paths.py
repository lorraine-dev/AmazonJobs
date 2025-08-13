"""
Centralized path helpers for scraper outputs derived from YAML config.
"""

from pathlib import Path
from typing import Optional

from src.scraper.config import ScraperConfig  # type: ignore


def _cfg(config: Optional[ScraperConfig]) -> ScraperConfig:
    return config or ScraperConfig()


def get_raw_dir(config: Optional[ScraperConfig] = None) -> Path:
    c = _cfg(config)
    # Prefer new schema
    raw_dir = c.get("common.paths.raw_dir")
    if raw_dir:
        return Path(raw_dir)
    # Fallback to legacy
    return Path(c.get("output.data_dir", "data/raw"))


def get_backup_dir(config: Optional[ScraperConfig] = None) -> Path:
    c = _cfg(config)
    backup_dir = c.get("common.paths.backup_dir")
    if backup_dir:
        return Path(backup_dir)
    return Path(c.get("output.backup_dir", "data/backups"))


def get_combined_file(config: Optional[ScraperConfig] = None) -> Path:
    c = _cfg(config)
    combined = c.get("common.paths.combined_file")
    if combined:
        return Path(combined)
    return Path(c.get("output.combined_file", "data/processed/combined_jobs.csv"))


def get_raw_filename(source: str, config: Optional[ScraperConfig] = None) -> str:
    c = _cfg(config)
    src_key = f"sources.{source.lower()}.raw_filename"
    # New schema
    filename = c.get(src_key)
    if filename:
        return filename
    # Backward compatibility map
    raw_map = c.get("output.raw_filenames", {}) or {}
    if source.lower() == "amazon":
        return raw_map.get("amazon") or c.get("output.filename", "amazon_jobs.csv")
    return raw_map.get(source.lower(), f"{source.lower()}_jobs.csv")


def get_raw_path(source: str, config: Optional[ScraperConfig] = None) -> Path:
    return get_raw_dir(config) / get_raw_filename(source, config)
