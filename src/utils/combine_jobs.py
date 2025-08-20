"""
Combine job listings from multiple sources into a single CSV file.

This module provides functionality to combine job listings from different sources
(Amazon, TheirStack, etc.) into a single CSV file that can be used by the dashboard.
"""

import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict
import logging
from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.paths import (
    get_raw_path,
    get_combined_file,
    get_raw_dir,
)  # type: ignore

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_latest_job_files(data_dir: Optional[Path] = None) -> List[Path]:
    """
    Get the job file from each configured source.

    Uses YAML-configured raw_filenames to locate current files. Since we
    write to fixed filenames per source, the "latest" is the configured file.
    """
    cfg = ScraperConfig()

    # Collect files for each configured source
    job_files: List[Path] = []

    # Prefer new schema: enumerate sources.* entries (respect enabled flag)
    sources_cfg: Dict[str, Dict] = cfg.get("sources", {}) or {}
    source_names: List[str] = []

    if sources_cfg:
        for name, sconf in sources_cfg.items():
            if isinstance(sconf, dict) and sconf.get("enabled", True):
                source_names.append(name)

    # Fallback to legacy mapping if present
    if not source_names:
        raw_map: Dict[str, str] = cfg.get("output.raw_filenames", {}) or {}
        source_names = sorted(list(raw_map.keys()))

    for source in sorted(source_names):
        path = get_raw_path(source, cfg)
        if path.exists():
            job_files.append(path)
        else:
            logger.warning(f"No raw file found for source '{source}' at {path}")

    # Backward-compat: if nothing collected, fall back to listing by glob
    if not job_files:
        data_path = Path(data_dir) if data_dir is not None else get_raw_dir(cfg)
        if data_path.exists():
            for file in data_path.glob("*_jobs.csv"):
                job_files.append(file)
        else:
            logger.error(f"Data directory not found: {data_path}")

    return job_files


def combine_job_files(
    files: List[Path], output_dir: str = "data/processed"
) -> Optional[Path]:
    """
    Combine multiple job files into a single CSV.

    Args:
        files: List of Path objects to job CSV files
        output_dir: Directory to save the combined CSV

    Returns:
        Path to the combined CSV file, or None if there was an error
    """
    if not files:
        logger.warning("No job files to combine")
        return None

    all_jobs = []

    for file in files:
        try:
            df = pd.read_csv(file)

            # Normalize known legacy/mismatched columns
            if "url" not in df.columns and "job_url" in df.columns:
                df["url"] = df["job_url"]
            if "job_category" not in df.columns and "category" in df.columns:
                df = df.rename(columns={"category": "job_category"})

            # Ensure 'active' exists and is boolean; default to True if missing/empty
            if "active" not in df.columns:
                df["active"] = True
            else:
                # Coerce common truthy/falsey representations; treat NaN/empty as True (currently scraped jobs)
                coerced = (
                    df["active"]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .map(
                        {
                            "true": True,
                            "t": True,
                            "1": True,
                            "yes": True,
                            "y": True,
                            "false": False,
                            "f": False,
                            "0": False,
                            "no": False,
                            "n": False,
                        }
                    )
                )
                df["active"] = coerced.fillna(True)

            # Ensure required columns exist
            if "source" not in df.columns:
                # Infer source from filename as last resort
                stem = file.stem.lower()
                if stem.startswith("amazon"):
                    df["source"] = "Amazon"
                elif stem.startswith("theirstack"):
                    df["source"] = "TheirStack"
                else:
                    df["source"] = "unknown"

            all_jobs.append(df)
            logger.info(f"Loaded {len(df)} jobs from {file.name}")

        except Exception as e:
            logger.error(f"Error reading {file}: {e}")

    if not all_jobs:
        logger.error("No valid job data found")
        return None

    # Combine all DataFrames
    combined_df = pd.concat(all_jobs, ignore_index=True)

    # Ensure consistent column order
    columns = [
        "id",
        "title",
        "company",
        "location",
        "posting_date",
        "url",
        "description",
        "basic_qual",
        "pref_qual",
        "skills",
        "active",
        "job_category",  # Renamed from 'category'
        "team",
        "role",
        "source",
    ]

    # Add any missing columns with empty values
    for col in columns:
        if col not in combined_df.columns:
            combined_df[col] = True if col == "active" else ""

    # Reorder columns
    combined_df = combined_df[columns]

    # Final safety: ensure 'active' is boolean dtype
    if "active" in combined_df.columns:
        combined_df["active"] = (
            combined_df["active"]
            .astype(str)
            .str.strip()
            .str.lower()
            .map(
                {
                    "true": True,
                    "t": True,
                    "1": True,
                    "yes": True,
                    "y": True,
                    "false": False,
                    "f": False,
                    "0": False,
                    "no": False,
                    "n": False,
                }
            )
            .fillna(True)
            .astype(bool)
        )

    # Determine configured combined file path
    cfg = ScraperConfig()
    output_file = get_combined_file(cfg)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(output_file, index=False)

    logger.info(
        f"Combined {len(combined_df)} jobs from {len(files)} sources into {output_file}"
    )
    return output_file


def update_dashboard_data() -> Optional[Path]:
    """
    Update the dashboard data by combining the latest job files.

    Returns:
        Path to the combined CSV file, or None if there was an error
    """
    logger.info("Updating dashboard data...")

    # Get the latest job files
    job_files = get_latest_job_files()

    # If no files found at all, log error and return
    if not job_files:
        logger.error("No job files found to process")
        return None

    # Combine the job files
    combined_file = combine_job_files(job_files)

    if combined_file:
        logger.info(f"Dashboard data updated: {combined_file}")
    else:
        logger.error("Failed to update dashboard data")

    return combined_file


if __name__ == "__main__":
    # When run directly, update the dashboard data
    result = update_dashboard_data()
    if not result:
        exit(1)
