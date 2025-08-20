"""
Main Amazon Jobs Scraper class
"""

import os
import logging
import time
import random
import pandas as pd
from datetime import datetime, date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import concurrent.futures
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple, Any

from .config import ScraperConfig
from src.utils.paths import (
    get_raw_dir,
    get_backup_dir,
    get_raw_path,
)  # type: ignore


class AmazonJobsScraper:
    """
    Main scraper class for Amazon job listings.

    Handles the complete scraping workflow including:
    - Configuration management
    - Web scraping with Selenium
    - Data processing and storage
    - Error handling and logging
    """

    def __init__(self, config: Optional[ScraperConfig] = None):
        """
        Initialize the Amazon Jobs Scraper.

        Args:
            config: Configuration object (optional)
        """
        self.config = config or ScraperConfig()
        self.logger = logging.getLogger(__name__)

        # Initialize state
        self.driver: Optional[webdriver.Chrome] = None
        self.all_job_links: set = set()
        self.seen_job_ids: set = set()

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

    def setup_driver_fast(self):
        """
        Configure and return Chrome WebDriver with production optimizations.

        Returns:
            Configured Chrome WebDriver instance
        """
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
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        # Disable JavaScript to avoid false positives
        # chrome_options.add_argument("--disable-javascript")
        chrome_options.add_argument("--disable-css")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            f"--user-agent={random.choice(user_agents)}"  # nosec B311
        )

        # Performance preferences
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
        }
        chrome_options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        return driver

    def check_for_blocking(self, driver):
        """
        Check if the scraper is being blocked by the website.

        Args:
            driver: Selenium WebDriver instance

        Returns:
            True if blocking detected, False otherwise
        """
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

        for indicator in blocking_indicators:
            if indicator in page_source:
                self.logger.warning(f"Potential blocking detected: {indicator}")
                return True

        return False

    def parse_posting_date(self, date_text: str) -> Optional[date]:
        """
        Parse posting date string into datetime.date object.

        Args:
            date_text: Date string to parse

        Returns:
            Parsed date or None if parsing fails
        """
        if not date_text:
            return None

        try:
            # Remove "Posted " prefix if present
            if date_text.startswith("Posted "):
                date_text = date_text[7:]

            # Parse common date formats
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
        """
        Extract role and team from job title.

        Args:
            title: Job title string

        Returns:
            Tuple of (role, team)
        """
        if not title:
            return "", ""

        # Common patterns
        patterns = [
            # "Role, Team" pattern
            (
                r"^([^,]+),\s*([^,]+)$",
                lambda m: (m.group(1).strip(), m.group(2).strip()),
            ),
            # "Role - Team" pattern
            (
                r"^([^-]+)\s*-\s*([^-]+)$",
                lambda m: (m.group(1).strip(), m.group(2).strip()),
            ),
            # "Role position Team" pattern
            (
                r"^(.+?)\s+position\s+(.+)$",
                lambda m: (m.group(1).strip(), m.group(2).strip()),
            ),
        ]

        for pattern, extractor in patterns:
            match = re.match(pattern, title)
            if match:
                return extractor(match)

        # Default: split on first comma or dash
        if "," in title:
            parts = title.split(",", 1)
            return parts[0].strip(), parts[1].strip()
        elif " - " in title:
            parts = title.split(" - ", 1)
            return parts[0].strip(), parts[1].strip()

        return title.strip(), ""

    def scrape_job_details_selenium_improved(
        self, driver, job_url: str
    ) -> Dict[str, Any]:
        """
        Scrape detailed information from a single job posting.

        Args:
            driver: Selenium WebDriver instance
            job_url: URL of the job posting

        Returns:
            Dictionary containing job details
        """
        job_details = {
            "job_url": job_url,
            "description": "",
            "basic_qual": "",
            "pref_qual": "",
            "job_category": "",
            "active": True,  # Default to active
        }

        try:
            # Add random delay before each request (1-3 seconds)
            time.sleep(random.uniform(1, 3))  # nosec B311

            # Navigate to job detail page
            driver.get(job_url)
            time.sleep(4)

            # Check if job page loads successfully (indicates job is still active)
            page_source = driver.page_source.lower()
            if not page_source:
                job_details["active"] = False
                self.logger.debug(f"Job appears to be inactive: {job_url}")
                return job_details

            # Wait for content to load
            wait = WebDriverWait(driver, 10)

            # Extract job category
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

            # Extract basic qualifications
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

            # Extract preferred qualifications
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

            # Extract job category
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
        """
        Load existing job data from CSV file.

        Returns:
            DataFrame containing existing jobs
        """
        csv_path = str(get_raw_path("amazon", self.config))

        if os.path.exists(csv_path):
            try:
                existing_jobs_df = pd.read_csv(csv_path)
                # Backward compatibility: add new columns if missing
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
        else:
            self.logger.info(f"No existing file found at {csv_path}")
            return pd.DataFrame()

    def merge_job_data_with_seen_ids(
        self, existing_df: pd.DataFrame, new_df: pd.DataFrame, seen_job_ids: set
    ) -> pd.DataFrame:
        """
        Merge new job data with existing data using seen_job_ids for active status.

        Args:
            existing_df: Existing job data
            new_df: New job data
            seen_job_ids: Set of job IDs seen on website

        Returns:
            Merged DataFrame
        """
        if existing_df.empty:
            return new_df

        if new_df.empty:
            # Update active status based on what we saw
            for idx, row in existing_df.iterrows():
                job_id = str(row["id"])
                existing_df.at[idx, "active"] = job_id in seen_job_ids

            active_count = existing_df["active"].sum()
            self.logger.info(
                f"Updated active status: {active_count} active, {len(existing_df) - active_count} inactive"
            )
            return existing_df

        # Combine dataframes
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # Remove duplicates, keeping most recent
        combined_df = combined_df.drop_duplicates(subset=["id"], keep="last")

        # Update active status based on seen_job_ids
        for idx, row in combined_df.iterrows():
            job_id = str(row["id"])
            combined_df.at[idx, "active"] = job_id in seen_job_ids

        self.logger.info(f"Merged data: {len(combined_df)} total jobs")
        return combined_df

    def scrape_job_details_parallel(
        self, job_links_list: List[Dict], max_workers: int = 3
    ) -> List[Dict]:
        """
        Scrape job details in parallel using a driver pool for better performance.

        Args:
            job_links_list: List of job information dictionaries
            max_workers: Number of parallel workers

        Returns:
            List of scraped job details
        """
        if not job_links_list:
            return []

        self.logger.info(
            f"Starting parallel scraping of {len(job_links_list)} jobs with {max_workers} workers"
        )

        # Create driver pool once
        self.logger.info(f"Creating driver pool with {max_workers} drivers...")
        driver_pool = [self.setup_driver_fast() for _ in range(max_workers)]

        def scrape_job_worker(args):
            """Worker function for parallel job scraping using driver pool."""
            job_info, driver = args
            try:
                # Random delay between workers
                time.sleep(random.uniform(0, 2))  # nosec B311

                job_details = self.scrape_job_details_selenium_improved(
                    driver, job_info["job_url"]
                )

                # Combine basic info with detailed info
                job_data = {
                    "id": job_info["job_id"],
                    "title": job_info["title"],
                    "company": "Amazon",  # Fixed company name
                    "role": job_info["role"],
                    "team": job_info["team"],
                    "job_url": job_info["job_url"],
                    "posting_date": job_info["posting_date"],
                    "description": job_details["description"],
                    "basic_qual": job_details["basic_qual"],
                    "pref_qual": job_details["pref_qual"],
                    "job_category": job_details["job_category"],
                    "active": job_details["active"],
                    "source": "Amazon",  # Add source column
                }
                return job_data
            except Exception as e:
                self.logger.error(
                    f"Error in worker for job {job_info.get('job_id', 'unknown')}: {e}"
                )
                return None

        # Process jobs in batches to avoid overwhelming the server
        batch_size = self.config.get("scraper.batch_size", 10)
        all_results = []

        for i in range(0, len(job_links_list), batch_size):
            batch = job_links_list[i : i + batch_size]
            self.logger.info(
                f"Processing batch {i//batch_size + 1}/{(len(job_links_list) + batch_size - 1)//batch_size}"
            )

            # Distribute jobs across drivers in the pool
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

                # Filter out None results
                batch_results = [r for r in batch_results if r is not None]
                all_results.extend(batch_results)

                # Delay between batches
                if i + batch_size < len(job_links_list):
                    time.sleep(random.uniform(2, 5))  # nosec B311

            except Exception as e:
                self.logger.error(f"Error in parallel execution: {e}")

            finally:
                # Cleanup resources if needed
                pass

        # Clean up driver pool
        self.logger.info("Cleaning up driver pool...")
        for driver in driver_pool:
            try:
                driver.quit()
            except Exception as e:
                self.logger.debug(f"Error quitting driver: {e}")

        self.logger.info(f"Successfully scraped {len(all_results)} jobs")
        return all_results

    def create_backup(self):
        """Create a backup of the current data file."""
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

    def run(self) -> pd.DataFrame:
        """
        Run the complete scraping workflow.

        Returns:
            DataFrame containing scraped job data
        """
        start_time = time.time()

        try:
            # Setup
            self.create_backup()

            # Load existing data
            existing_df = self.load_existing_jobs()
            existing_job_ids = (
                set(existing_df["id"].astype(str)) if not existing_df.empty else set()
            )
            self.logger.info(f"Found {len(existing_job_ids)} existing job IDs")

            # Initialize scraping variables
            self.driver = None
            self.all_job_links = set()
            self.seen_job_ids = set()

            # Engine selection: allow API mode to bypass Selenium entirely
            engine = (
                os.getenv("AMAZON_ENGINE")
                or self.config.get("sources.amazon.engine", "selenium")
            ).lower()
            if engine == "api":
                self.logger.info(
                    "Using Amazon API engine (delegating to AmazonAPIScraper)"
                )
                try:
                    # Prefer absolute import path used elsewhere in repo
                    from src.scraper.amazon_api_scraper import AmazonAPIScraper  # type: ignore
                except Exception:  # pragma: no cover - fallback for module resolution
                    from .amazon_api_scraper import AmazonAPIScraper  # type: ignore

                out_csv_path = get_raw_path("amazon", self.config)
                api = AmazonAPIScraper(self.config)
                save_page_json = bool(
                    self.config.get("sources.amazon.api.save_page_json", True)
                )
                # Use headers file by default; do not pass URL so API scraper
                # reads docs/reference/Amazon/request_headers.txt and sanitizes it
                df = api.run(
                    out_csv=out_csv_path,
                    no_cookie=True,
                    save_raw=save_page_json,
                )
                return df

            try:
                self.driver = self.setup_driver_fast()

                # Navigate to initial page
                self.logger.info("Loading initial page...")
                self.driver.get(self.config.get("scraper.base_url"))
                time.sleep(random.uniform(2, 4))  # nosec B311

                # Check for blocking
                if self.check_for_blocking(self.driver):
                    self.logger.error("Blocked on initial page load. Stopping scraper.")
                    return existing_df

                wait = WebDriverWait(self.driver, 8)  # type: ignore
                page = 1

                # Collect all job links
                while True:
                    self.logger.info(f"Collecting job links from page {page}")

                    try:
                        job_tiles = wait.until(
                            EC.presence_of_all_elements_located(
                                (By.CLASS_NAME, "job-tile")
                            )
                        )
                    except Exception as e:
                        self.logger.error(
                            f"Error finding job tiles on page {page}: {e}"
                        )
                        break

                    self.logger.info(f"Found {len(job_tiles)} job tiles on page {page}")

                    if len(job_tiles) == 0:
                        break

                    page_job_links = []

                    for i, tile in enumerate(job_tiles):
                        try:
                            # Extract job ID
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

                            # Track seen jobs
                            self.seen_job_ids.add(job_id)

                            # Skip existing jobs
                            if job_id in existing_job_ids:
                                self.logger.debug(f"Skipping existing job ID: {job_id}")
                                continue

                            # Extract job information
                            title = ""
                            try:
                                title_elem = tile.find_element(
                                    By.CSS_SELECTOR, "h3, h2, a"
                                )
                                title = title_elem.text.strip()
                            except Exception:
                                title = tile.text.strip()[:100]

                            job_url = ""
                            try:
                                job_link = tile.find_element(
                                    By.CSS_SELECTOR, "a[href*='/jobs/']"
                                )
                                job_url = job_link.get_attribute("href") or ""
                            except Exception:
                                job_url = f"https://amazon.jobs/en/jobs/{job_id}"

                            posting_date = None
                            try:
                                posting_date_elem = tile.find_element(
                                    By.CSS_SELECTOR, "h2.posting-date"
                                )
                                posting_date_text = posting_date_elem.text.strip()
                                posting_date = self.parse_posting_date(
                                    posting_date_text
                                )
                            except Exception as e:
                                self.logger.debug(f"Error extracting posting date: {e}")
                                pass

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
                            continue

                    # Add to main collection
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

                    self.logger.debug(
                        f"Added {len(page_job_links)} job links from page {page}"
                    )

                    # Navigate to next page
                    try:
                        next_button = self.driver.find_element(  # type: ignore
                            By.XPATH, "//button[@aria-label='Next page']"
                        )

                        if "disabled" in (next_button.get_attribute("class") or ""):
                            break

                        self.driver.execute_script(  # type: ignore
                            "arguments[0].scrollIntoView();", next_button
                        )
                        time.sleep(0.5)
                        next_button.click()
                        time.sleep(2)
                        page += 1

                    except Exception as e:
                        self.logger.error(f"Error on page {page}: {e}")
                        break

                self.logger.info(
                    f"Finished collecting {len(self.all_job_links)} unique jobs from {page} pages"
                )
                self.logger.info(
                    f"Total jobs seen on website: {len(self.seen_job_ids)}"
                )

                # Handle case where no new jobs were found
                if len(self.all_job_links) == 0:
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

                    # Save updated data
                    output_path = get_raw_path("amazon", self.config)
                    existing_df.to_csv(output_path, index=False)
                    self.logger.debug(f"Updated data saved to {output_path}")
                    return existing_df

                # Convert to list for parallel processing
                job_links_list = []
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

                # Parallel scraping of job details
                new_jobs_data = self.scrape_job_details_parallel(
                    job_links_list,
                    max_workers=self.config.get("scraper.max_workers", 3),
                )
                new_df = pd.DataFrame(new_jobs_data)

                # Merge with existing data
                if not existing_df.empty:
                    final_df = self.merge_job_data_with_seen_ids(
                        existing_df, new_df, self.seen_job_ids
                    )
                else:
                    final_df = new_df

                # Save results to raw CSV via centralized storage
                from src.utils.raw_storage import save_raw_jobs

                records = final_df.to_dict("records")
                saved_path = save_raw_jobs("amazon", records, self.config)
                if saved_path:
                    self.logger.info(f"✅ Saved raw Amazon data to {saved_path}")

                # Log summary
                execution_time = time.time() - start_time
                self.logger.info("=== SCRAPING COMPLETE ===")
                self.logger.info(f"Total jobs: {len(final_df)}")
                self.logger.info(f"Active jobs: {final_df['active'].sum()}")
                self.logger.info(
                    f"Inactive jobs: {len(final_df) - final_df['active'].sum()}"
                )
                self.logger.info(f"Execution time: {execution_time:.2f} seconds")

                # Raw save completed
                self.logger.info("✅ Raw save completed")

                return final_df

            finally:
                if self.driver:
                    try:
                        self.driver.quit()
                    except Exception as e:
                        self.logger.debug(f"Error quitting driver: {e}")

        except Exception as e:
            self.logger.error(f"Fatal error in main execution: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                self.logger.debug(f"Error quitting driver: {e}")
