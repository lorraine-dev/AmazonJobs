"""
Unified raw storage writer using centralized path helpers.
"""

from typing import List, Dict, Optional
import pandas as pd

from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.paths import get_raw_path  # type: ignore

REQUIRED_COLUMNS = [
    "id",
    "title",
    "company",
    "location",
    "posting_date",
    "url",
    "description",
    "skills",
    "active",
    "job_category",
    "team",
    "role",
    "source",
]


def _normalize_records(records: List[Dict]) -> List[Dict]:
    norm = []
    for r in records:
        rr = dict(r)
        # Normalize url field name
        if "url" not in rr and "job_url" in rr:
            rr["url"] = rr.get("job_url", "")
        # Normalize category
        if "job_category" not in rr and "category" in rr:
            rr["job_category"] = rr.get("category", "")
        norm.append(rr)
    return norm


def save_raw_jobs(
    source: str, jobs: List[Dict], config: Optional[ScraperConfig] = None
):
    if not jobs:
        return None

    src = source.title() if source else "Unknown"
    records = _normalize_records(jobs)

    df = pd.DataFrame(records)

    # Ensure required columns exist
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            if col == "source":
                df[col] = src
            elif col == "company" and src == "Amazon":
                df[col] = "Amazon"
            else:
                df[col] = ""

    # Stable types and dedupe
    if "id" in df.columns:
        df["id"] = df["id"].astype(str)
        df.drop_duplicates(subset=["id"], keep="last", inplace=True)

    path = get_raw_path(source, config)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Append/dedupe with existing file
    if path.exists():
        try:
            existing = pd.read_csv(path, dtype={"id": str})
        except Exception:
            existing = pd.DataFrame(columns=df.columns)
        combined = pd.concat([existing, df], ignore_index=True)
        if "id" in combined.columns:
            combined.drop_duplicates(subset=["id"], keep="last", inplace=True)
        combined.to_csv(path, index=False)
    else:
        df.to_csv(path, index=False)

    return path
