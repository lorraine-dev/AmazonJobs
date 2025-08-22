"""
API-based Amazon Jobs scraper using the undocumented search.json endpoint.

- Reads URL and headers (Cookie/Referer/User-Agent/etc) from a plain-text headers file
  like `docs/reference/Amazon/request_headers.txt`.
- Paginates using offset/result_limit until all hits are retrieved.
- Saves raw JSON pages under `data/raw/amazon_api_raw/`.
- Writes a flattened CSV to `data/raw/amazon_api_jobs.csv` (via utils.paths helpers).

This module is intended for quick testing and validation before integrating into the
main scraper flow (replacing Selenium if results are satisfactory).
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import time
import random

from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.paths import (
    get_raw_dir,
    get_raw_path,
)  # type: ignore


LOGGER = logging.getLogger(__name__)


def create_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a requests Session with retry/backoff configured.

    Retries on common transient errors and 429 rate limiting with exponential backoff.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


@dataclass
class RequestSpec:
    url: str
    headers: Dict[str, str]


def merge_with_active_flags(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    seen_ids: Set[str],
    logger: logging.Logger,
) -> pd.DataFrame:
    """Merge existing and new API results and set 'active' based on seen_ids.

    Mirrors Selenium scraper behavior where any job id present in the current crawl
    is marked active=True; jobs missing from the current crawl are marked active=False.
    """
    # Normalize id to string for consistent comparisons
    if not new_df.empty:
        new_df = new_df.copy()
        new_df["id"] = new_df["id"].astype(str)

    if existing_df is None or existing_df.empty:
        # First run or no prior data: mark only currently-seen ids as active
        if not new_df.empty:
            new_df["active"] = new_df["id"].astype(str).isin(seen_ids)
            new_df["active"] = new_df["active"].astype(bool)
        logger.info("Merged data (no existing): %d total jobs", len(new_df))
        return new_df

    # Ensure existing ids are strings
    existing_df = existing_df.copy()
    if "id" in existing_df.columns:
        existing_df["id"] = existing_df["id"].astype(str)

    combined = pd.concat([existing_df, new_df], ignore_index=True)
    # Keep the most recent row per id
    combined = combined.drop_duplicates(subset=["id"], keep="last").reset_index(
        drop=True
    )

    # Set active=True only for ids seen in this crawl
    combined["active"] = combined["id"].astype(str).isin(seen_ids)
    combined["active"] = combined["active"].astype(bool)

    active_count = int(combined["active"].sum()) if not combined.empty else 0
    logger.info(
        "Updated active status: active=%d inactive=%d total=%d",
        active_count,
        len(combined) - active_count,
        len(combined),
    )
    return combined


def parse_headers_file(path: Path) -> RequestSpec:
    """Parse a plain-text headers dump file to extract URL and headers.

    Expected lines include for example:
      URL: https://www.amazon.jobs/en/search.json?...  (one line)
      Referer: ...
      Cookie: ...
      User-Agent: ...

    All `key: value` pairs are collected as headers except `URL`, which is used as target URL.

    Args:
        path: Path to the headers file.

    Returns:
        RequestSpec with url and headers.
    """
    text = path.read_text(encoding="utf-8", errors="ignore")
    url: Optional[str] = None
    headers: Dict[str, str] = {}

    # Normalize Windows line endings if present
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        # Some lines could be like `Summary` or other non header lines; skip
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if key.lower() == "url":
            url = value
        else:
            # Keep only a curated subset of headers that matter; others are harmless but not required
            if key.lower() in {
                "user-agent",
                "accept",
                "accept-language",
                "accept-encoding",
                "cookie",
                "referer",
                "origin",
                "sec-fetch-mode",
                "sec-fetch-site",
                "sec-fetch-dest",
                "x-requested-with",
            }:
                headers[key] = value

    if not url:
        raise ValueError(
            f"Did not find a URL in headers file: {path}. Ensure a 'URL: ...' line exists."
        )

    return RequestSpec(url=url, headers=headers)


def ensure_json_endpoint(url: str) -> str:
    """Ensure we call the JSON endpoint, not the HTML search.

    Converts .../search? to .../search.json? when needed.
    """
    # Replace only the first occurrence of '/search?' with '/search.json?'
    return re.sub(r"/search\?", "/search.json?", url, count=1)


def update_query_param(url: str, key: str, value: str) -> str:
    """Update or append a query parameter in a URL string.

    This is a simple regex-based approach to avoid adding a runtime dependency.
    """
    # Replace existing param
    pattern = re.compile(rf"([?&]){re.escape(key)}=[^&]*")
    if pattern.search(url):
        return pattern.sub(rf"\1{key}={value}", url)
    # Append
    sep = "&" if ("?" in url) else "?"
    return f"{url}{sep}{key}={value}"


def extract_int_query_param(url: str, key: str, default: int) -> int:
    m = re.search(rf"[?&]{re.escape(key)}=([^&]+)", url)
    if not m:
        return default
    try:
        return int(m.group(1))
    except Exception:
        return default


def sanitize_url_query(url: str) -> str:
    """Remove empty query params and prefer normalized_country_code[] over country/country[].

    - Drops keys with only empty values (e.g., latitude=, city=)
    - If normalized_country_code[] is present, removes country and country[] to avoid conflicts
    """
    try:
        parsed = urlparse(url)
        q = parse_qs(parsed.query, keep_blank_values=True)

        if "normalized_country_code[]" in q:
            q.pop("country[]", None)
            q.pop("country", None)

        # Drop keys whose all values are empty strings
        to_delete = [k for k, vals in q.items() if all((v == "" for v in vals))]
        for k in to_delete:
            del q[k]

        # Rebuild query preserving repeated keys
        query = urlencode([(k, v) for k, vals in q.items() for v in vals], doseq=True)
        new = parsed._replace(query=query)
        return urlunparse(new)
    except Exception:
        return url


def fetch_page(
    spec: RequestSpec,
    offset: int,
    timeout: int = 30,
    session: Optional[requests.Session] = None,
) -> Dict[str, Any]:
    url = update_query_param(spec.url, "offset", str(offset))
    try:
        # Sanitize headers for logging (avoid dumping raw Cookie)
        sanitized_headers = {
            k: (f"<len={len(v)}>" if k.lower() == "cookie" else v)
            for k, v in spec.headers.items()
        }
        LOGGER.debug("GET %s headers=%s", url, sanitized_headers)
        http = session or requests
        resp = http.get(url, headers=spec.headers, timeout=timeout)
        LOGGER.debug(
            "Response status: %s content-type=%s",
            resp.status_code,
            resp.headers.get("Content-Type"),
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            LOGGER.debug(
                "Page summary: keys=%s jobs=%s hits=%s",
                list(data.keys())[:10],
                len(list(data.get("jobs") or [])),
                data.get("hits"),
            )
        return data
    except requests.RequestException as e:
        LOGGER.error("Request failed at offset=%s: %s", offset, e)
        try:
            LOGGER.debug("Error body (truncated): %s", resp.text[:500])  # type: ignore[name-defined]
        except Exception:
            pass
        raise
    except ValueError as e:
        LOGGER.error("Failed to decode JSON at offset=%s: %s", offset, e)
        try:
            LOGGER.debug("Response snippet: %s", resp.text[:500])  # type: ignore[name-defined]
        except Exception:
            pass
        raise


def flatten_jobs(jobs: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for j in jobs:
        api_uuid = j.get("id")
        job_path = j.get("job_path")
        # Prefer numeric id from id_icims when present; else parse from job_path; else use API UUID
        id_icims = j.get("id_icims")
        numeric_from_path: Optional[str] = None
        if isinstance(job_path, str):
            m = re.search(r"/jobs/(\d+)", job_path)
            if m:
                numeric_from_path = m.group(1)

        # Extract optional nested fields
        team_label: Optional[str] = None
        team_val = j.get("team")
        if isinstance(team_val, dict):
            team_label = team_val.get("label")

        # Compose row with rich fields; preserve HTML from API for descriptions/quals
        row: Dict[str, Any] = {
            "id": id_icims
            or numeric_from_path
            or (str(api_uuid) if api_uuid is not None else None),
            "api_id": api_uuid,
            "title": j.get("title"),
            "role": j.get("title"),
            "company": j.get("company_name"),
            "city": j.get("city"),
            "country_code": j.get("country_code"),
            "normalized_location": j.get("normalized_location"),
            "location": j.get("location"),
            "job_category": j.get("job_category"),
            "job_schedule_type": j.get("job_schedule_type"),
            "is_manager": j.get("is_manager"),
            "is_intern": j.get("is_intern"),
            "posted_date": j.get("posted_date"),
            "posting_date": j.get(
                "posted_date"
            ),  # duplicate for downstream compatibility
            "description": j.get("description"),
            "description_short": j.get("description_short"),
            "basic_qual": j.get("basic_qualifications"),
            "pref_qual": j.get("preferred_qualifications"),
            "team": team_label,
            "job_path": job_path,
            "job_url": (f"https://amazon.jobs{job_path}" if job_path else None),
            "apply_url": j.get("url_next_step"),
            "source": "AmazonAPI",
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    # Ensure consistent types
    if not df.empty:
        df["id"] = df["id"].astype(str)
        if "api_id" in df.columns:
            df["api_id"] = df["api_id"].astype(str)
        df["source"] = "AmazonAPI"
    return df


class AmazonAPIScraper:
    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or ScraperConfig()
        self.logger = LOGGER
        # Align module logger level with YAML config so DEBUG logs can be enabled centrally
        try:
            level_name = str(self.config.get("common.logging.level") or "INFO").upper()
            level = getattr(logging, level_name, logging.INFO)
            LOGGER.setLevel(level)
            self.logger.debug("Logger level set to %s from config", level_name)
        except Exception:
            pass
        # Ensure base raw dir; per-page JSON dir will be created lazily only if saving
        get_raw_dir(self.config).mkdir(parents=True, exist_ok=True)

    def run(
        self,
        headers_file: Path = Path("docs/reference/Amazon/request_headers.txt"),
        url: Optional[str] = None,
        out_csv: Optional[Path] = None,
        max_pages: Optional[int] = None,
        save_raw: bool = True,
        timeout: int = 30,
        no_cookie: bool = False,
        write_output: bool = True,
    ) -> pd.DataFrame:
        """Run the API scraper.

        Args:
            headers_file: path to the headers dump file to construct the request
            out_csv: optional path for the combined CSV; defaults to get_raw_path('amazon_api')
            max_pages: limit pages for smoke testing
            save_raw: save each page JSON under data/raw/amazon_api_raw/
            timeout: request timeout seconds
            write_output: when False, skip writing the final CSV (useful for hybrid mode)
        """
        # Establish URL and headers
        spec: Optional[RequestSpec] = None
        if url:
            target_url = sanitize_url_query(ensure_json_endpoint(url))
            self.logger.info("Using URL from --url (cookies not required)")
            # Minimal, safe default headers
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.8",
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
                "Referer": re.sub(r"/search\.json\?", "/search?", target_url),
            }
            spec = RequestSpec(url=target_url, headers=headers)
        else:
            self.logger.info(f"Reading headers and URL from: {headers_file}")
            spec = parse_headers_file(headers_file)
            spec.url = sanitize_url_query(ensure_json_endpoint(spec.url))

            # Fill minimal defaults if missing
            spec.headers.setdefault("Accept", "application/json, text/plain, */*")
            spec.headers.setdefault("Accept-Language", "en-US,en;q=0.8")
            spec.headers.setdefault(
                "User-Agent",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
            )
            spec.headers.setdefault(
                "Referer", re.sub(r"/search\.json\?", "/search?", spec.url)
            )

        if no_cookie and "Cookie" in spec.headers:
            del spec.headers["Cookie"]
            self.logger.info("Cookie header stripped (--no-cookie)")

        result_limit = extract_int_query_param(spec.url, "result_limit", default=10)
        self.logger.info(f"result_limit inferred from URL: {result_limit}")

        # HTTP session with retries/backoff (configurable via common.http_retries/backoff)
        try:
            retries = int(self.config.get("common.http_retries") or 3)
        except Exception:
            retries = 3
        try:
            backoff = float(self.config.get("common.http_backoff") or 0.5)
        except Exception:
            backoff = 0.5
        session = create_session(retries=retries, backoff_factor=backoff)

        # Fetch first page to get hits
        self.logger.info("Fetching first page (offset=0)...")
        first = fetch_page(spec, offset=0, timeout=timeout, session=session)
        hits = int(first.get("hits") or 0)
        self.logger.info(f"Total hits reported: {hits}")

        all_jobs = list(first.get("jobs") or [])

        # Track duplicates across pages using numeric job id from job_path; fallback to API UUID
        def _job_key(j: Dict[str, Any]) -> Optional[str]:
            id_icims = j.get("id_icims")
            if id_icims is not None:
                return str(id_icims)
            jp = j.get("job_path")
            if isinstance(jp, str):
                m = re.search(r"/jobs/(\d+)", jp)
                if m:
                    return m.group(1)
            api_uuid = j.get("id")
            return str(api_uuid) if api_uuid is not None else None

        seen_ids: Set[str] = set(k for k in (_job_key(j) for j in all_jobs) if k)
        self.logger.info(
            "First page jobs=%d unique_job_ids=%d",
            len(all_jobs),
            len(seen_ids),
        )

        # Save raw if requested
        raw_dir = get_raw_dir(self.config) / "amazon_api_raw"
        if save_raw:
            raw_dir.mkdir(parents=True, exist_ok=True)
            (raw_dir / "page_0.json").write_text(
                json.dumps(first, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        # Determine number of pages
        total_pages = (hits + result_limit - 1) // result_limit if hits else 1
        pages = total_pages
        if max_pages is not None:
            pages = min(total_pages, max_pages)
        self.logger.info(
            f"Planned pages to fetch: {pages} (total available: {total_pages})"
        )

        # Optional rate limiting between requests
        try:
            min_interval = float(
                self.config.get("common.http_min_interval_seconds") or 0.0
            )
        except Exception:
            min_interval = 0.0
        try:
            jitter = float(self.config.get("common.http_jitter_seconds") or 0.0)
        except Exception:
            jitter = 0.0

        # Fetch remaining pages
        offset = result_limit
        page_idx = 1
        while page_idx < pages:
            self.logger.info(f"Fetching page {page_idx+1}/{pages} (offset={offset})...")
            data = fetch_page(spec, offset=offset, timeout=timeout, session=session)
            jobs = list(data.get("jobs") or [])
            all_jobs.extend(jobs)

            if save_raw:
                (raw_dir / f"page_{page_idx}.json").write_text(
                    json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
                )

            if not jobs:
                self.logger.info("No jobs returned; stopping early.")
                break

            # Sleep between requests if configured
            if min_interval > 0:
                delay = min_interval + (
                    random.uniform(0, jitter) if jitter > 0 else 0.0
                )
                self.logger.debug(f"Sleeping {delay:.2f}s before next page request")
                time.sleep(delay)

            offset += result_limit
            page_idx += 1

            # Per-page metrics and duplicate tracking (by numeric job id when available)
            new_ids = [_job_key(j) for j in jobs]
            dup_in_page = sum(1 for _id in new_ids if _id in seen_ids)
            for _id in new_ids:
                if _id:
                    seen_ids.add(_id)
            self.logger.info(
                "Page %d: jobs=%d dups=%d cumulative_unique_job_ids=%d",
                page_idx + 1,
                len(jobs),
                dup_in_page,
                len(seen_ids),
            )

            offset += result_limit
            page_idx += 1

        self.logger.info(f"Total jobs collected (rows): {len(all_jobs)}")
        self.logger.info(f"Total unique job ids collected: {len(seen_ids)}")
        if hits and len(seen_ids) != hits:
            if max_pages is not None and pages < total_pages:
                self.logger.info(
                    "Partial fetch limited by --max-pages=%s: API hits=%d, collected unique job ids=%d (total rows=%d)",
                    max_pages,
                    hits,
                    len(seen_ids),
                    len(all_jobs),
                )
            else:
                self.logger.warning(
                    "Hits mismatch: API hits=%d but collected unique job ids=%d (total rows=%d)",
                    hits,
                    len(seen_ids),
                    len(all_jobs),
                )
        df_new = flatten_jobs(all_jobs)
        # Deduplicate by job id to ensure unique listings among new rows
        if not df_new.empty:
            before = len(df_new)
            df_new = df_new.drop_duplicates(subset=["id"], keep="first").reset_index(
                drop=True
            )
            after = len(df_new)
            if after < before:
                self.logger.info(
                    "Dropped %d duplicate rows by id (kept %d unique)",
                    before - after,
                    after,
                )

        # Merge with existing raw CSV (if present) and set active based on seen_ids
        out_path = out_csv or get_raw_path("amazon_api", self.config)
        existing_df: pd.DataFrame = pd.DataFrame()
        try:
            if out_path.exists():
                existing_df = pd.read_csv(out_path)
                self.logger.info(
                    "Loaded existing raw CSV for merge: %s (%d rows)",
                    out_path,
                    len(existing_df),
                )
        except Exception as e:
            self.logger.warning("Failed to load existing raw CSV %s: %s", out_path, e)

        final_df = merge_with_active_flags(existing_df, df_new, seen_ids, self.logger)

        # Write CSV only if requested
        if write_output:
            final_df.to_csv(out_path, index=False)
            self.logger.info(f"Wrote CSV: {out_path} ({len(final_df)} rows)")
        else:
            self.logger.debug("write_output=False; skipping CSV write to %s", out_path)
        return final_df


def _build_arg_parser():
    import argparse

    p = argparse.ArgumentParser(description="Amazon Jobs API Scraper (experimental)")
    p.add_argument(
        "--headers-file",
        type=Path,
        default=Path("docs/reference/Amazon/request_headers.txt"),
        help="Path to headers file containing URL and headers",
    )
    p.add_argument(
        "--url",
        type=str,
        default=None,
        help="Optional direct URL to search.json (overrides --headers-file)",
    )
    p.add_argument(
        "--out-csv",
        type=Path,
        default=None,
        help="Optional output CSV path (defaults to data/raw/amazon_api_jobs.csv)",
    )
    p.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Limit pages for testing (optional)",
    )
    p.add_argument(
        "--no-save-raw",
        action="store_true",
        help="Do not save raw JSON pages",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds",
    )
    p.add_argument(
        "--no-cookie",
        action="store_true",
        help="Strip Cookie header before making requests (CI-safe)",
    )
    return p


if __name__ == "__main__":
    # Basic console logging so info-level messages appear when run as a script
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = _build_arg_parser().parse_args()
    scraper = AmazonAPIScraper()
    scraper.run(
        headers_file=args.headers_file,
        url=args.url,
        out_csv=args.out_csv,
        max_pages=args.max_pages,
        save_raw=(not args.no_save_raw),
        timeout=args.timeout,
        no_cookie=args.no_cookie,
    )
