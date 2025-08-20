"""
Amazon Selenium-based scraper, extracted from the legacy `amazon_scraper.py`.
Implements a clean engine class that can be delegated to by the thin
`AmazonJobsScraper` wrapper.
"""

from __future__ import annotations

import logging
import os
import random
import time
import re
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from .config import ScraperConfig
from src.utils.paths import get_raw_dir, get_backup_dir, get_raw_path  # type: ignore
from src.utils.raw_storage import save_raw_jobs  # type: ignore


class AmazonSeleniumScraper:
    """
    Engine: Selenium. Scrapes amazon.jobs HTML pages.

    Public method:
      - run(url, out_csv, save_raw, ...) -> pd.DataFrame
    """

    def __init__(self, config: Optional[ScraperConfig] = None):
        self.config = config or ScraperConfig()
        self.logger = logging.getLogger(__name__)
        self.driver: Optional[webdriver.Chrome] = None
        self.all_job_links: set = set()
        self.seen_job_ids: set = set()
        self._setup_directories()

    def _setup_directories(self) -> None:
        directories = [get_raw_dir(self.config), get_backup_dir(self.config), "logs"]
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Directories created: {directories}")

    def setup_driver_fast(self) -> webdriver.Chrome:
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) "
            "Gecko/20100101 Firefox/89.0",
        ]

        chrome_options = Options()
        headless = bool(self.config.get("sources.amazon.headless", True))
        if headless:
            # Prefer the new headless when available
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--disable-css")
        chrome_options.add_argument("--window-size=1920,1080")
        # Stability/stealth flags
        chrome_options.add_argument(
            "--disable-features=NetworkService,NetworkServiceInProcess"
        )
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--lang=en-US")
        chrome_options.add_argument(
            f"--user-agent={random.choice(user_agents)}"
        )  # nosec B311

        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver

    def check_for_blocking(self, driver: webdriver.Chrome) -> bool:
        page_source = driver.page_source.lower()
        blocking_indicators = [
            "access denied",
            "blocked",
            "captcha",
            "robot",
            "bot detection",
            "rate limit",
            "too many requests",
            "suspicious activity",
        ]
        return any(ind in page_source for ind in blocking_indicators)

    def parse_posting_date(self, date_text: str) -> Optional[date]:
        if not date_text:
            return None
        try:
            if date_text.startswith("Posted "):
                date_text = date_text[7:]
            for fmt in ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"]:
                try:
                    parsed_date = datetime.strptime(date_text, fmt)
                    return parsed_date.date()
                except ValueError:
                    continue
            self.logger.warning(f"Could not parse date: {date_text}")
            return None
        except Exception as e:
            self.logger.error(f"Error parsing date '{date_text}': {e}")
            return None

    def extract_role_and_team(self, title: str) -> Tuple[str, str]:
        if not title:
            return "", ""
        patterns = [
            (
                r"^([^,]+),\s*([^,]+)$",
                lambda m: (m.group(1).strip(), m.group(2).strip()),
            ),
            (
                r"^([^-]+)\s*-\s*([^-]+)$",
                lambda m: (m.group(1).strip(), m.group(2).strip()),
            ),
            (
                r"^(.+?)\s+position\s+(.+)$",
                lambda m: (m.group(1).strip(), m.group(2).strip()),
            ),
        ]
        for pattern, extractor in patterns:
            match = re.match(pattern, title)
            if match:
                return extractor(match)
        if "," in title:
            parts = title.split(",", 1)
            return parts[0].strip(), parts[1].strip()
        if " - " in title:
            parts = title.split(" - ", 1)
            return parts[0].strip(), parts[1].strip()
        return title.strip(), ""

    def scrape_job_details_selenium_improved(
        self, driver: webdriver.Chrome, job_url: str
    ) -> Dict[str, Any]:
        job_details: Dict[str, Any] = {
            "job_url": job_url,
            "description": "",
            "basic_qual": "",
            "pref_qual": "",
            "job_category": "",
            "active": True,
        }
        try:
            time.sleep(random.uniform(1, 3))  # nosec B311
            driver.get(job_url)
            time.sleep(4)
            page_source = driver.page_source.lower()
            if not page_source:
                job_details["active"] = False
                self.logger.debug(f"Job appears to be inactive: {job_url}")
                return job_details
            wait = WebDriverWait(driver, 10)
            try:
                desc_elem = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "//h2[contains(text(), 'DESCRIPTION')]/following-sibling::p",
                        )
                    )
                )
                job_details["description"] = desc_elem.text.strip()
            except Exception:
                self.logger.debug("Could not find description section")
            try:
                basic_qual_elem = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "//h2[contains(text(), 'BASIC QUALIFICATIONS')]/following-sibling::p",
                        )
                    )
                )
                job_details["basic_qual"] = basic_qual_elem.text.strip()
            except Exception:
                self.logger.debug("Could not find basic qualifications section")
            try:
                pref_qual_elem = wait.until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "//h2[contains(text(), 'PREFERRED QUALIFICATIONS')]/following-sibling::p",
                        )
                    )
                )
                job_details["pref_qual"] = pref_qual_elem.text.strip()
            except Exception:
                self.logger.debug("Could not find preferred qualifications section")
            try:
                category_elem = wait.until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "div.association.job-category-icon a")
                    )
                )
                job_details["job_category"] = category_elem.text.strip()
            except Exception:
                self.logger.debug("Could not find job category section")
        except Exception as e:
            self.logger.error(f"Error scraping job details from {job_url}: {str(e)}")
        return job_details

    def load_existing_jobs(self) -> pd.DataFrame:
        csv_path = str(get_raw_path("amazon", self.config))
        if os.path.exists(csv_path):
            try:
                existing_jobs_df = pd.read_csv(csv_path)
                if "company" not in existing_jobs_df.columns:
                    existing_jobs_df["company"] = "Amazon"
                if "source" not in existing_jobs_df.columns:
                    existing_jobs_df["source"] = "Amazon"
                self.logger.info(
                    f"Loaded {len(existing_jobs_df)} existing jobs from {csv_path}"
                )
                return existing_jobs_df
            except Exception as e:
                self.logger.error(f"Error loading existing jobs: {e}")
                return pd.DataFrame()
        self.logger.info(f"No existing file found at {csv_path}")
        return pd.DataFrame()

    def merge_job_data_with_seen_ids(
        self, existing_df: pd.DataFrame, new_df: pd.DataFrame, seen_job_ids: set
    ) -> pd.DataFrame:
        if existing_df.empty:
            return new_df
        if new_df.empty:
            for idx, row in existing_df.iterrows():
                job_id = str(row["id"])
                existing_df.at[idx, "active"] = job_id in seen_job_ids
            active_count = existing_df["active"].sum()
            self.logger.info(
                f"Updated active status: {active_count} active, {len(existing_df) - active_count} inactive"
            )
            return existing_df
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["id"], keep="last")
        for idx, row in combined_df.iterrows():
            job_id = str(row["id"])
            combined_df.at[idx, "active"] = job_id in seen_job_ids
        self.logger.info(f"Merged data: {len(combined_df)} total jobs")
        return combined_df

    def scrape_job_details_parallel(
        self, job_links_list: List[Dict], max_workers: int = 3
    ) -> List[Dict]:
        if not job_links_list:
            return []
        self.logger.info(
            f"Starting parallel scraping of {len(job_links_list)} jobs with {max_workers} workers"
        )
        self.logger.info(f"Creating driver pool with {max_workers} drivers...")
        driver_pool = [self.setup_driver_fast() for _ in range(max_workers)]

        def scrape_job_worker(args):
            job_info, driver = args
            try:
                time.sleep(random.uniform(0, 2))  # nosec B311
                job_details = self.scrape_job_details_selenium_improved(
                    driver, job_info["job_url"]
                )
                role, team = self.extract_role_and_team(job_info["title"])  # guard
                job_data = {
                    "id": job_info["job_id"],
                    "title": job_info["title"],
                    "company": "Amazon",
                    "role": role,
                    "team": team,
                    "job_url": job_info["job_url"],
                    "posting_date": job_info["posting_date"],
                    "description": job_details["description"],
                    "basic_qual": job_details["basic_qual"],
                    "pref_qual": job_details["pref_qual"],
                    "job_category": job_details["job_category"],
                    "active": job_details["active"],
                    "source": "Amazon",
                }
                return job_data
            except Exception as e:
                self.logger.error(
                    f"Error in worker for job {job_info.get('job_id', 'unknown')}: {e}"
                )
                return None

        import concurrent.futures

        batch_size = self.config.get("sources.amazon.batch_size", 10)
        all_results: List[Dict[str, Any]] = []
        for i in range(0, len(job_links_list), batch_size):
            batch = job_links_list[i : i + batch_size]
            self.logger.info(
                f"Processing batch {i//batch_size + 1}/{(len(job_links_list) + batch_size - 1)//batch_size}"
            )
            job_driver_pairs = []
            for j, job_info in enumerate(batch):
                driver_index = j % max_workers
                job_driver_pairs.append((job_info, driver_pool[driver_index]))
            try:
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    batch_results = list(
                        executor.map(scrape_job_worker, job_driver_pairs)
                    )
                batch_results = [r for r in batch_results if r is not None]
                all_results.extend(batch_results)
                if i + batch_size < len(job_links_list):
                    time.sleep(random.uniform(2, 5))  # nosec B311
            except Exception as e:
                self.logger.error(f"Error in parallel execution: {e}")
            finally:
                pass

        self.logger.info("Cleaning up driver pool...")
        for driver in driver_pool:
            try:
                driver.quit()
            except Exception as e:
                self.logger.debug(f"Error quitting driver: {e}")

        self.logger.info(f"Successfully scraped {len(all_results)} jobs")
        return all_results

    def create_backup(self) -> None:
        source_path = get_raw_path("amazon", self.config)
        if source_path.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = (
                get_backup_dir(self.config) / f"amazon_jobs_backup_{timestamp}.csv"
            )
            try:
                import shutil

                shutil.copy2(source_path, backup_file)
                self.logger.info(f"Backup created: {backup_file}")
            except Exception as e:
                self.logger.error(f"Error creating backup: {e}")

    def run(
        self,
        url: Optional[str] = None,
        out_csv: Optional[Path] = None,
        save_raw: bool = False,
        no_cookie: bool = False,  # ignored in selenium engine
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Run Selenium scraping flow.
        """
        start_time = time.time()
        self.create_backup()

        existing_df = self.load_existing_jobs()
        existing_job_ids = (
            set(existing_df["id"].astype(str)) if not existing_df.empty else set()
        )
        self.logger.info(f"Found {len(existing_job_ids)} existing job IDs")

        self.driver = None
        self.all_job_links = set()
        self.seen_job_ids = set()
        refresh_existing: bool = bool(
            self.config.get("sources.amazon.refresh_existing", False)
        )
        # Cumulative counters for concise logging
        total_new = 0
        total_refreshed = 0
        total_skipped = 0
        total_extract_errors = 0

        # Prefer an explicit HTML search page URL for Selenium (separate from API JSON URL)
        selected_url = (
            url
            or self.config.get("sources.amazon.html_base_url")
            or self.config.get("sources.amazon.base_url")
        )
        if not selected_url:
            raise ValueError("Missing sources.amazon.base_url for Selenium engine")

        try:
            self.driver = self.setup_driver_fast()
            # If the configured URL is the JSON API endpoint, convert to the HTML search page
            html_url = (
                selected_url.replace("search.json", "search")
                if "search.json" in selected_url
                else selected_url
            )
            self.logger.info("Loading initial page...")
            self.driver.get(html_url)
            time.sleep(random.uniform(2, 4))  # nosec B311
            try:
                self.logger.info(f"Current URL after load: {self.driver.current_url}")
            except Exception:
                pass

            # Attempt to accept cookie consent if present (OneTrust common ID)
            try:
                consent_btn = WebDriverWait(self.driver, 3).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                consent_btn.click()
                time.sleep(0.5)
            except Exception:
                pass

            if self.check_for_blocking(self.driver):
                self.logger.error("Blocked on initial page load. Stopping scraper.")
                return existing_df

            wait = WebDriverWait(self.driver, 15)  # type: ignore
            page = 1
            prev_seen_count = 0

            # URL-driven pagination setup: parse initial URL and preserve all filters
            try:
                current_url = self.driver.current_url
            except Exception:
                current_url = html_url
            parsed = urlparse(current_url)
            params = parse_qs(parsed.query)

            # Determine pagination parameters
            def _get_int_param(p: Dict[str, List[str]], key: str, default: int) -> int:
                try:
                    return int((p.get(key) or [str(default)])[0] or default)
                except Exception:
                    return default

            result_limit = _get_int_param(params, "result_limit", 10)
            offset = _get_int_param(params, "offset", 0)

            # Guard: required country filter must be present (LUX)
            def _has_required_filter(p: Dict[str, List[str]]) -> bool:
                """
                On the HTML search UI, the active country checkbox writes to country[] not
                normalized_country_code[]. Requiring country[] ensures the UI actually has
                the Luxembourg filter applied.
                """
                return "LUX" in (p.get("country[]") or [])

            # Optional pagination limits
            max_pages = self.config.get("sources.amazon.limits.max_pages")
            max_jobs = self.config.get("sources.amazon.limits.max_jobs")

            # If required filter is missing, attempt to auto-correct the URL once
            if not _has_required_filter(params):
                self.logger.warning(
                    "Required country filter (LUX) missing from initial URL; attempting to correct and reload. URL=%s",
                    current_url,
                )
                corrected_params = dict(params)
                # Prefer the UI-backed key country[] (HTML uses this for the checkbox filter)
                desired_country = (
                    corrected_params.get("country[]")
                    or corrected_params.get("normalized_country_code[]")
                    or ["LUX"]
                )[0]
                corrected_params["country[]"] = [desired_country]
                # Remove conflicting/legacy/empty location params and normalized_country_code[]
                for k in [
                    "normalized_country_code[]",
                    "latitude",
                    "longitude",
                    "radius",
                    "distanceType",
                    "loc_group_id",
                    "loc_query",
                    "base_query",
                    "city",
                    "country",
                    "region",
                    "county",
                    "query_options",
                ]:
                    corrected_params.pop(k, None)
                corrected_query = urlencode(corrected_params, doseq=True)
                corrected_url = urlunparse(parsed._replace(query=corrected_query))
                if corrected_url != current_url:
                    self.logger.info(
                        "Reloading corrected initial URL: %s", corrected_url
                    )
                    try:
                        self.driver.get(corrected_url)
                        time.sleep(random.uniform(1, 2))  # nosec B311
                    except Exception as e:
                        self.logger.error("Error loading corrected URL: %s", e)
                        return existing_df
                    # Re-parse after reload
                    try:
                        current_url = self.driver.current_url
                    except Exception:
                        current_url = corrected_url
                    parsed = urlparse(current_url)
                    params = parse_qs(parsed.query)

            # Final guard: if still missing, stop
            if not _has_required_filter(params):
                self.logger.warning(
                    "Required country filter (LUX) still missing after correction; stopping. URL=%s",
                    current_url,
                )
                return existing_df

            while True:
                try:
                    self.logger.info(
                        f"Collecting job links from page {page} | URL: {self.driver.current_url}"
                    )
                except Exception:
                    self.logger.info(f"Collecting job links from page {page}")
                # Hardened tile detection with retries and alternative selectors
                selectors = [
                    "div.job-tile",
                    "li.job-tile",
                    "div[data-job-id]",
                ]
                job_tiles = []
                last_error: Optional[Exception] = None
                for attempt in range(3):
                    for sel in selectors:
                        try:
                            job_tiles = wait.until(
                                EC.presence_of_all_elements_located(
                                    (By.CSS_SELECTOR, sel)
                                )
                            )
                            if job_tiles:
                                break
                        except Exception as e:
                            last_error = e
                            job_tiles = []
                    if job_tiles:
                        break
                    time.sleep(0.8)  # nosec B311
                if not job_tiles:
                    if last_error:
                        self.logger.error(
                            f"Error finding job tiles on page {page}: {last_error}"
                        )
                    else:
                        self.logger.error(f"No job tiles found on page {page}")
                    break

                self.logger.info(f"Found {len(job_tiles)} job tiles on page {page}")
                if len(job_tiles) == 0:
                    break
                # Stop after this page if it's a short page (likely the last page)
                short_page = len(job_tiles) < result_limit

                page_job_links: List[Dict[str, Any]] = []
                # Per-page counters
                page_new = 0
                page_refreshed = 0
                page_skipped = 0
                page_errors = 0
                for i, tile in enumerate(job_tiles):
                    try:
                        job_id = tile.get_attribute("data-job-id")
                        if not job_id:
                            try:
                                job_id_elem = tile.find_element(
                                    By.CSS_SELECTOR, "[data-job-id]"
                                )
                                job_id = job_id_elem.get_attribute("data-job-id")
                            except Exception as e:
                                self.logger.debug(f"Error extracting job ID: {e}")
                                continue

                        self.seen_job_ids.add(job_id)
                        if job_id in existing_job_ids and not refresh_existing:
                            self.logger.debug(f"Skipping existing job ID: {job_id}")
                            page_skipped += 1
                            continue
                        elif job_id in existing_job_ids and refresh_existing:
                            self.logger.debug(f"Refreshing existing job ID: {job_id}")
                            page_refreshed += 1
                        else:
                            page_new += 1

                        try:
                            title_elem = tile.find_element(By.CSS_SELECTOR, "h3, h2, a")
                            title = title_elem.text.strip()
                        except Exception:
                            title = tile.text.strip()[:100]

                        try:
                            job_link = tile.find_element(
                                By.CSS_SELECTOR, "a[href*='/jobs/']"
                            )
                            job_url = job_link.get_attribute("href") or ""
                        except Exception:
                            job_url = f"https://amazon.jobs/en/jobs/{job_id}"

                        posting_date: Optional[date] = None
                        try:
                            posting_date_elem = tile.find_element(
                                By.CSS_SELECTOR, "h2.posting-date"
                            )
                            posting_date_text = posting_date_elem.text.strip()
                            posting_date = self.parse_posting_date(posting_date_text)
                        except Exception as e:
                            self.logger.debug(f"Error extracting posting date: {e}")

                        role, team = self.extract_role_and_team(title)
                        page_job_links.append(
                            {
                                "job_id": job_id,
                                "title": title,
                                "role": role,
                                "team": team,
                                "job_url": job_url,
                                "posting_date": posting_date,
                            }
                        )
                        self.logger.debug(f"Found job {i+1}: {title}")
                    except Exception as e:
                        self.logger.error(f"Error extracting job {i+1}: {e}")
                        page_errors += 1
                        continue

                before_links = len(self.all_job_links)
                for job_info in page_job_links:
                    self.all_job_links.add(
                        (
                            job_info["job_id"],
                            job_info["title"],
                            job_info["role"],
                            job_info["team"],
                            job_info["job_url"],
                            job_info["posting_date"],
                        )
                    )
                page_new_links = len(self.all_job_links) - before_links
                # Update cumulative counters
                total_new += page_new
                total_refreshed += page_refreshed
                total_skipped += page_skipped
                total_extract_errors += page_errors
                # Concise per-page summary
                self.logger.info(
                    "Page %d summary: new=%d refresh=%d skipped=%d errors=%d new_links_collected=%d total_unique_links=%d",
                    page,
                    page_new,
                    page_refreshed,
                    page_skipped,
                    page_errors,
                    page_new_links,
                    len(self.all_job_links),
                )

                # Cumulative progress logging and loop guard
                curr_seen_count = len(self.seen_job_ids)
                self.logger.info(
                    f"Cumulative unique job IDs seen: {curr_seen_count} | unique links collected: {len(self.all_job_links)}"
                )
                if curr_seen_count == prev_seen_count:
                    self.logger.warning(
                        "No new job IDs found on this page; stopping pagination to avoid duplicates"
                    )
                    break
                prev_seen_count = curr_seen_count

                # Short page guard
                if short_page:
                    self.logger.info(
                        f"Short page detected (tiles={len(job_tiles)} < result_limit={result_limit}); stopping pagination"
                    )
                    break

                # Guard: stop if we have reached the max_jobs limit
                if max_jobs and isinstance(max_jobs, int) and max_jobs > 0:
                    if len(self.all_job_links) >= max_jobs:
                        self.logger.info(
                            f"Reached max_jobs={max_jobs}; stopping pagination after collecting {len(self.all_job_links)} jobs"
                        )
                        break

                # Max runtime guard
                max_runtime = int(
                    self.config.get("sources.amazon.limits.max_runtime_seconds", 0) or 0
                )
                if max_runtime > 0 and (time.time() - start_time) >= max_runtime:
                    self.logger.info(
                        f"Reached max_runtime_seconds={max_runtime}; stopping pagination gracefully"
                    )
                    break

                # URL-driven pagination: increment offset and navigate directly
                prev_url = ""
                try:
                    prev_url = self.driver.current_url
                except Exception:
                    prev_url = urlunparse(parsed)

                next_offset = offset + result_limit
                next_params = dict(params)
                next_params["offset"] = [str(next_offset)]
                next_query = urlencode(next_params, doseq=True)
                next_url = urlunparse(parsed._replace(query=next_query))
                if prev_url == next_url:
                    self.logger.warning(
                        "Next URL is identical to current; stopping pagination"
                    )
                    break

                # Guard: stop before navigating if we have reached the max_pages limit
                if (
                    max_pages
                    and isinstance(max_pages, int)
                    and max_pages > 0
                    and page >= max_pages
                ):
                    self.logger.info(
                        f"Reached max_pages={max_pages}; stopping before navigating to page {page+1}"
                    )
                    break

                self.logger.info(f"Navigating via URL to page {page+1}: {next_url}")
                try:
                    self.driver.get(next_url)
                    time.sleep(random.uniform(1, 2))  # nosec B311
                except Exception as e:
                    self.logger.error(f"Navigation error on page {page}: {e}")
                    break

                # Update parsed components and params from the actually loaded URL
                try:
                    loaded_url = self.driver.current_url
                except Exception:
                    loaded_url = next_url
                parsed = urlparse(loaded_url)
                params = parse_qs(parsed.query)

                # Verify required filter persists
                if not _has_required_filter(params):
                    self.logger.warning(
                        "Required country filter (LUX) disappeared after navigation; stopping at URL=%s",
                        loaded_url,
                    )
                    break

                # Update offset using what the site reports (fallback to our next_offset)
                new_offset = _get_int_param(params, "offset", next_offset)
                if new_offset == offset:
                    self.logger.warning(
                        "Offset did not advance (still %d); stopping.", offset
                    )
                    break
                offset = new_offset
                page += 1

            self.logger.info(
                f"Finished collecting {len(self.all_job_links)} unique jobs from {page} pages"
            )
            self.logger.info(f"Total jobs seen on website: {len(self.seen_job_ids)}")
            # Final concise session summary
            self.logger.info(
                "Session summary: pages_crawled=%d unique_links=%d jobs_seen=%d new=%d refresh=%d skipped=%d errors=%d",
                page,
                len(self.all_job_links),
                len(self.seen_job_ids),
                total_new,
                total_refreshed,
                total_skipped,
                total_extract_errors,
            )

            if len(self.all_job_links) == 0:
                # If we didn't even see any job ids on the site, avoid flipping everything inactive.
                if len(self.seen_job_ids) == 0:
                    self.logger.warning(
                        "No job tiles found on the site; leaving existing 'active' flags unchanged"
                    )
                    output_path = get_raw_path("amazon", self.config)
                    existing_df.to_csv(output_path, index=False)
                    self.logger.debug(f"Saved existing data unchanged to {output_path}")
                    return existing_df

                self.logger.info(
                    "No new Amazon jobs; updating active flags (seen_on_site=%d)",
                    len(self.seen_job_ids),
                )
                if not existing_df.empty:
                    for idx, row in existing_df.iterrows():
                        job_id = str(row["id"])
                        existing_df.at[idx, "active"] = job_id in self.seen_job_ids
                    active_count = existing_df["active"].sum()
                    self.logger.info(
                        "Active summary: active=%d inactive=%d total=%d",
                        active_count,
                        len(existing_df) - active_count,
                        len(existing_df),
                    )
                output_path = get_raw_path("amazon", self.config)
                existing_df.to_csv(output_path, index=False)
                self.logger.debug(f"Updated data saved to {output_path}")
                return existing_df

            job_links_list: List[Dict[str, Any]] = []
            for job_tuple in self.all_job_links:
                job_links_list.append(
                    {
                        "job_id": job_tuple[0],
                        "title": job_tuple[1],
                        "role": job_tuple[2],
                        "team": job_tuple[3],
                        "job_url": job_tuple[4],
                        "posting_date": job_tuple[5],
                    }
                )

            new_jobs_data = self.scrape_job_details_parallel(
                job_links_list,
                max_workers=self.config.get("sources.amazon.max_workers", 3),
            )
            new_df = pd.DataFrame(new_jobs_data)

            if not existing_df.empty:
                final_df = self.merge_job_data_with_seen_ids(
                    existing_df, new_df, self.seen_job_ids
                )
            else:
                final_df = new_df

            records = final_df.to_dict("records")
            saved_path = save_raw_jobs("amazon", records, self.config)
            if saved_path:
                self.logger.info(f"✅ Saved raw Amazon data to {saved_path}")

            execution_time = time.time() - start_time
            self.logger.info("=== SCRAPING COMPLETE ===")
            self.logger.info(f"Total jobs: {len(final_df)}")
            self.logger.info(f"Active jobs: {final_df['active'].sum()}")
            self.logger.info(
                f"Inactive jobs: {len(final_df) - final_df['active'].sum()}"
            )
            self.logger.info(f"Execution time: {execution_time:.2f} seconds")
            self.logger.info("✅ Raw save completed")
            return final_df
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                except Exception as e:
                    self.logger.debug(f"Error quitting driver: {e}")
