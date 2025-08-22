"""
Centralized path helpers for scraper outputs derived from YAML config.
"""

from pathlib import Path
from typing import Optional

from src.scraper.config import ScraperConfig  # type: ignore


def _cfg(config: Optional[ScraperConfig]) -> ScraperConfig:
    return config or ScraperConfig()


def _resolve_rel(c: ScraperConfig, p: str) -> Path:
    """Resolve a possibly-relative path against the config file directory.

    If "p" is absolute, return it as-is. If it's relative, interpret it
    relative to the directory containing the active YAML config file.
    """
    path = Path(p)
    if path.is_absolute():
        return path
    base = Path(".")
    if getattr(c, "config_path", None):
        cfg_dir = Path(c.config_path).parent
        # If config file is under a 'config/' directory, treat its parent as project root
        base = cfg_dir.parent if cfg_dir.name == "config" else cfg_dir
    return base / path


def get_raw_dir(config: Optional[ScraperConfig] = None) -> Path:
    c = _cfg(config)
    # If legacy paths explicitly present in YAML, prefer them
    cfg_map = getattr(c, "_config", {}) or {}
    legacy_out = cfg_map.get("output", {}) if isinstance(cfg_map, dict) else {}
    if isinstance(legacy_out, dict) and "data_dir" in legacy_out:
        return _resolve_rel(c, legacy_out.get("data_dir", "data/raw"))
    # Otherwise use new schema, falling back to default
    raw_dir = c.get("common.paths.raw_dir")
    if raw_dir:
        return _resolve_rel(c, raw_dir)
    return _resolve_rel(c, "data/raw")


def get_backup_dir(config: Optional[ScraperConfig] = None) -> Path:
    c = _cfg(config)
    cfg_map = getattr(c, "_config", {}) or {}
    legacy_out = cfg_map.get("output", {}) if isinstance(cfg_map, dict) else {}
    if isinstance(legacy_out, dict) and "backup_dir" in legacy_out:
        return _resolve_rel(c, legacy_out.get("backup_dir", "data/backups"))
    backup_dir = c.get("common.paths.backup_dir")
    if backup_dir:
        return _resolve_rel(c, backup_dir)
    return _resolve_rel(c, "data/backups")


def get_combined_file(config: Optional[ScraperConfig] = None) -> Path:
    c = _cfg(config)
    cfg_map = getattr(c, "_config", {}) or {}
    legacy_out = cfg_map.get("output", {}) if isinstance(cfg_map, dict) else {}
    if isinstance(legacy_out, dict) and "combined_file" in legacy_out:
        return _resolve_rel(
            c, legacy_out.get("combined_file", "data/processed/combined_jobs.csv")
        )
    combined = c.get("common.paths.combined_file")
    if combined:
        return _resolve_rel(c, combined)
    return _resolve_rel(c, "data/processed/combined_jobs.csv")


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
