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
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.paths import (
    get_raw_dir,
    get_raw_path,
)  # type: ignore


LOGGER = logging.getLogger(__name__)


@dataclass
class RequestSpec:
    url: str
    headers: Dict[str, str]


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


def fetch_page(spec: RequestSpec, offset: int, timeout: int = 30) -> Dict[str, Any]:
    url = update_query_param(spec.url, "offset", str(offset))
    try:
        # Sanitize headers for logging (avoid dumping raw Cookie)
        sanitized_headers = {
            k: (f"<len={len(v)}>" if k.lower() == "cookie" else v)
            for k, v in spec.headers.items()
        }
        LOGGER.debug("GET %s headers=%s", url, sanitized_headers)
        resp = requests.get(url, headers=spec.headers, timeout=timeout)
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
        rows.append(
            {
                # core
                "id": j.get("id"),
                "title": j.get("title"),
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
                "job_path": j.get("job_path"),
                "job_url": (
                    f"https://amazon.jobs{j.get('job_path')}"
                    if j.get("job_path")
                    else None
                ),
                "source": "AmazonAPI",
            }
        )
    df = pd.DataFrame(rows)
    # Ensure consistent types
    if not df.empty:
        df["id"] = df["id"].astype(str)
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
    ) -> pd.DataFrame:
        """Run the API scraper.

        Args:
            headers_file: path to the headers dump file to construct the request
            out_csv: optional path for the combined CSV; defaults to get_raw_path('amazon_api')
            max_pages: limit pages for smoke testing
            save_raw: save each page JSON under data/raw/amazon_api_raw/
            timeout: request timeout seconds
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

        # Fetch first page to get hits
        self.logger.info("Fetching first page (offset=0)...")
        first = fetch_page(spec, offset=0, timeout=timeout)
        hits = int(first.get("hits") or 0)
        self.logger.info(f"Total hits reported: {hits}")

        all_jobs = list(first.get("jobs") or [])
        # Track duplicates across pages
        seen_ids: Set[str] = set(str(j.get("id")) for j in all_jobs)
        self.logger.info(
            "First page jobs=%d unique_ids=%d",
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
        pages = (hits + result_limit - 1) // result_limit if hits else 1
        if max_pages is not None:
            pages = min(pages, max_pages)
        self.logger.info(f"Planned pages to fetch: {pages}")

        # Fetch remaining pages
        offset = result_limit
        page_idx = 1
        while page_idx < pages:
            self.logger.info(f"Fetching page {page_idx+1}/{pages} (offset={offset})...")
            data = fetch_page(spec, offset=offset, timeout=timeout)
            jobs = list(data.get("jobs") or [])
            all_jobs.extend(jobs)

            if save_raw:
                (raw_dir / f"page_{page_idx}.json").write_text(
                    json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
                )

            if not jobs:
                self.logger.info("No jobs returned; stopping early.")
                break

            # Per-page metrics and duplicate tracking
            new_ids = [str(j.get("id")) for j in jobs]
            dup_in_page = sum(1 for _id in new_ids if _id in seen_ids)
            for _id in new_ids:
                seen_ids.add(_id)
            self.logger.info(
                "Page %d: jobs=%d dups=%d cumulative_unique=%d",
                page_idx + 1,
                len(jobs),
                dup_in_page,
                len(seen_ids),
            )

            offset += result_limit
            page_idx += 1

        self.logger.info(f"Total jobs collected (rows): {len(all_jobs)}")
        self.logger.info(f"Total unique IDs collected: {len(seen_ids)}")
        if hits and len(seen_ids) != hits:
            self.logger.warning(
                "Hits mismatch: API hits=%d but collected unique ids=%d (total rows=%d)",
                hits,
                len(seen_ids),
                len(all_jobs),
            )
        df = flatten_jobs(all_jobs)

        # Write CSV
        out_path = out_csv or get_raw_path("amazon_api", self.config)
        df.to_csv(out_path, index=False)
        self.logger.info(f"Wrote CSV: {out_path} ({len(df)} rows)")
        return df


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
