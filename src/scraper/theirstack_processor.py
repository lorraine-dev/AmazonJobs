import pandas as pd
from typing import List, Dict
import logging
from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.raw_storage import save_raw_jobs  # type: ignore
from src.utils.category_mapper import infer_job_category  # type: ignore
from src.utils.description_parser import parse_job_description  # type: ignore

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Category inference is centralized in src/utils/category_mapper.py


# Define the mapping between TheirStack fields and our format
def _map_theirstack_to_our_format(job: Dict) -> Dict:
    """Map TheirStack job data to our standard format"""
    title = job.get("job_title", "")
    # Prefer final_url, fall back to url, then source_url
    url = job.get("final_url") or job.get("url") or job.get("source_url") or ""
    company = job.get("company") or (job.get("company_object", {}) or {}).get(
        "name", ""
    )
    # Derive fields to match our dashboard expectations
    role = title  # Dashboard displays 'role' as the primary title text
    team = company or "External"
    job_category = infer_job_category(job)
    description = job.get("description", "")
    parsed = parse_job_description(description)

    return {
        "id": str(job.get("id", "")),
        "title": title,
        "company": company,
        "location": job.get("location", ""),
        "posting_date": job.get("date_posted", ""),
        "url": url,
        "description": description,
        "skills": ", ".join(job.get("technology_slugs", [])),
        "active": True,
        "job_category": job_category,
        "team": team,
        "role": role,
        "source": "TheirStack",
        # Parsed fields for richer dashboard analysis
        "about": parsed.get("about", ""),
        "responsibilities": "\n".join(parsed.get("responsibilities", []) or []),
        "basic_qualifications": "\n".join(parsed.get("basic_qualifications", []) or []),
        "preferred_qualifications": "\n".join(
            parsed.get("preferred_qualifications", []) or []
        ),
        "benefits": "\n".join(parsed.get("benefits", []) or []),
    }


def process_theirstack_jobs(jobs: List[Dict]) -> pd.DataFrame:
    """
    Process TheirStack jobs and save to CSV in our format

    Args:
        jobs: List of TheirStack job dictionaries

    Returns:
        DataFrame containing processed jobs
    """
    if not jobs:
        logger.info("No TheirStack jobs to process")
        return pd.DataFrame()

    # Convert jobs to our format
    processed_jobs = [_map_theirstack_to_our_format(job) for job in jobs]

    # Create DataFrame and ensure consistent column order
    columns = [
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
        # New structured fields parsed from description
        "about",
        "responsibilities",
        "basic_qualifications",
        "preferred_qualifications",
        "benefits",
    ]
    df = pd.DataFrame(processed_jobs)

    # Ensure all expected columns exist (fill with empty strings if missing)
    for col in columns:
        if col not in df.columns:
            df[col] = ""

    # Reorder columns
    df = df[columns]

    # Ensure ID is string for stable dedupe
    if "id" in df.columns:
        df["id"] = df["id"].astype(str)

    # Save via centralized raw storage
    cfg = ScraperConfig()
    saved = save_raw_jobs("theirstack", df.to_dict("records"), cfg)
    if saved:
        logger.info("✅ TheirStack raw CSV updated at %s", saved)

    return df


if __name__ == "__main__":
    # Example usage (best run via package context)
    try:
        from .theirstack_scraper import TheirStackScraper  # type: ignore[import-not-found]
    except ImportError as e:  # pragma: no cover - for direct script runs
        logger.debug(f"Could not import TheirStackScraper: {e}")

    scraper = TheirStackScraper()
    jobs = scraper.get_new_jobs()
    process_theirstack_jobs(jobs)
