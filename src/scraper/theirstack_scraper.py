import os
from typing import Dict, List, Optional
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
import logging
from pathlib import Path
import json

from src.utils.theirstack_state import TheirStackState  # type: ignore
from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.paths import get_backup_dir  # type: ignore

load_dotenv()

API_KEY = os.getenv("THEIR_STACK_API_KEY")


class TheirStackScraper:
    def __init__(self, config: Optional[ScraperConfig] = None):
        # Validate API key early for better DX (local runs) and clearer errors
        if not API_KEY:
            raise EnvironmentError(
                "THEIR_STACK_API_KEY is not set. For local runs, create a .env at repo root "
                "with 'THEIR_STACK_API_KEY=...' or set it in your environment. "
                "In CI, define it as a GitHub Actions secret."
            )

        self.state = TheirStackState()
        self.headers = {"Authorization": f"Bearer {API_KEY}"}
        self.logger = logging.getLogger(__name__)
        # Load YAML config (with defaults) if not provided
        self.config = config or ScraperConfig()
        # API endpoint from YAML only (no code fallback)
        self.api_url = self.config.get("theirstack.api_url")
        if not self.api_url:
            raise ValueError(
                "Missing 'theirstack.api_url' in config/scraper_config.yaml. Please set it under the 'theirstack' section."
            )

        # Backups directory
        self.backup_dir: Path = get_backup_dir(self.config)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    def _load_external_titles(self) -> List[str]:
        """Load additional job titles from config/theirstack_titles.json if present.

        The JSON is expected to be an object whose values are arrays of strings
        (grouped by category). We flatten values and return a simple list.
        """
        try:
            titles_path = Path("config/theirstack_titles.json")
            if titles_path.exists():
                with open(titles_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    merged: List[str] = []
                    for v in data.values():
                        if isinstance(v, list):
                            merged.extend([str(x) for x in v])
                    return merged
                if isinstance(data, list):  # fallback if a flat list is provided
                    return [str(x) for x in data]
        except Exception as e:  # pragma: no cover - defensive only
            self.logger.warning("Failed to load external titles: %s", e)
        return []

    @staticmethod
    def _merge_titles(base: List[str], extra: List[str]) -> List[str]:
        """Merge two title lists, deduping case-insensitively while preserving order.

        Returns a new list.
        """
        seen = set()
        out: List[str] = []
        for title in list(base) + list(extra):
            key = title.strip().lower()
            if key and key not in seen:
                seen.add(key)
                out.append(title)
        return out

    def _save_response_backup(
        self,
        kind: str,
        response_json: Dict,
        request_payload: Dict,
        page: Optional[int] = None,
    ) -> None:
        """Persist the full API response (and request payload) under data/backups.

        Args:
            kind: 'precheck' or 'page'
            response_json: Parsed JSON response from TheirStack
            request_payload: Payload sent in the request
            page: Page number for paginated calls (optional)
        """
        try:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
            page_sfx = f"_p{page}" if page is not None else ""
            fname = f"theirstack_response_{kind}_{ts}{page_sfx}.json"
            out_path = self.backup_dir / fname
            payload = {
                "timestamp_utc": ts,
                "kind": kind,
                "page": page,
                "request_payload": request_payload,
                "response": response_json,
            }
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Saved TheirStack {kind} response backup: {out_path}")
        except Exception as e:  # pragma: no cover - guardrails only
            self.logger.warning(
                f"Failed to save TheirStack {kind} response backup: {e}"
            )

    def get_new_jobs(self) -> List[Dict]:
        """
        Fetches new jobs from the TheirStack API since the last run.
        """
        # Configurable pagination and daily cap (YAML-driven)
        page_size = int(self.config.get("theirstack.page_size"))
        max_jobs_per_run = int(self.config.get("theirstack.max_jobs_per_run"))
        max_excluded_ids = int(self.config.get("theirstack.max_excluded_ids"))
        # Log current state snapshot
        self.logger.info(
            "State snapshot before request: last_run_date=%s, seen_ids=%d, page_size=%d, max_jobs_per_run=%d",
            self.state.get_last_run_date(),
            len(self.state.scraped_job_ids),
            page_size,
            max_jobs_per_run,
        )
        # Calculate the date for the last run
        last_run_date = self.state.get_last_run_date()
        if last_run_date:
            # Use the last_run_date string directly (it is in YYYY-MM-DD format)
            posted_at_gte = last_run_date
        else:
            # First run: get jobs from the last 24 hours
            posted_at_gte = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Prepare exclusion list of already seen job IDs (normalized as strings)
        seen_ids = list(self.state.scraped_job_ids)
        if max_excluded_ids and len(seen_ids) > max_excluded_ids:
            seen_ids = seen_ids[:max_excluded_ids]

        # Build title filters and max age window from YAML, then merge external list if present
        title_filters = self.config.get("theirstack.job_title_or") or []
        try:
            external_titles = self._load_external_titles()
            if external_titles:
                title_filters = self._merge_titles(title_filters, external_titles)
        except Exception as e:  # pragma: no cover - defensive only
            self.logger.warning("Title merge failed: %s", e)
        max_age_days = int(self.config.get("theirstack.posted_at_max_age_days"))
        country_codes = self.config.get(
            "theirstack.job_country_code_or", ["LU"]
        )  # default LU

        # First, make a free request to check if there are any unseen jobs
        check_payload = {
            "posted_at_gte": posted_at_gte,
            "job_country_code_or": country_codes,
            "posted_at_max_age_days": max_age_days,
            "job_title_or": title_filters,
            "limit": 1,
            "blur_company_data": True,
            "include_total_results": True,
            "job_id_not": seen_ids,
        }
        try:
            # Log a concise snapshot of the pre-check payload (avoid overly verbose logs)
            seen_preview = ", ".join(seen_ids[:10]) if seen_ids else ""
            titles_preview = ", ".join(title_filters[:5]) if title_filters else ""
            self.logger.info(
                "Sending pre-check: posted_at_gte=%s countries=%s titles=%d[%s] max_age_days=%d exclude_ids=%d[%s]",
                posted_at_gte,
                ",".join(country_codes),
                len(title_filters),
                titles_preview,
                max_age_days,
                len(seen_ids),
                seen_preview,
            )
            response = requests.post(
                self.api_url,
                json=check_payload,
                headers=self.headers,
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            # Persist pre-check response for offline analysis
            self._save_response_backup("precheck", data, check_payload)
            meta_obj = data.get("metadata") or data.get("meta") or {}
            total_jobs = int(
                (meta_obj.get("total_results") or meta_obj.get("total") or 0) or 0
            )
            self.logger.info("Pre-check total_results=%d", total_jobs)
            if total_jobs == 0:
                self.logger.info(
                    "No new jobs via pre-check; skipping full request. (posted_at_gte=%s, max_age_days=%d, exclude_ids=%d)",
                    posted_at_gte,
                    max_age_days,
                    len(seen_ids),
                )
                # Debug: run a wide pre-check over the full max_age_days window to verify if older jobs exist
                try:
                    wide_gte = (
                        datetime.utcnow() - timedelta(days=max_age_days)
                    ).strftime("%Y-%m-%d")
                    wide_payload = dict(check_payload)
                    wide_payload.update({"posted_at_gte": wide_gte})
                    wresp = requests.post(
                        self.api_url,
                        json=wide_payload,
                        headers=self.headers,
                        timeout=10,
                    )
                    wresp.raise_for_status()
                    wdata = wresp.json()
                    self._save_response_backup("precheck_wide", wdata, wide_payload)
                    wmeta = wdata.get("metadata") or wdata.get("meta") or {}
                    wtotal = int(
                        (wmeta.get("total_results") or wmeta.get("total") or 0) or 0
                    )
                    self.logger.info(
                        "Wide pre-check total_results=%d (posted_at_gte=%s).",
                        wtotal,
                        wide_gte,
                    )

                    # Option A: If wide pre-check finds results, do a limited paid fetch from the wide window
                    wide_cap = self.config.get("theirstack.wide_fetch_limit")
                    try:
                        wide_fetch_limit = int(wide_cap) if wide_cap is not None else 10
                    except Exception:
                        wide_fetch_limit = 10

                    if wtotal > 0 and wide_fetch_limit > 0:
                        target_to_fetch = min(wtotal, wide_fetch_limit)
                        self.logger.info(
                            "Proceeding with limited wide fetch: cap=%d from posted_at_gte=%s",
                            target_to_fetch,
                            wide_gte,
                        )

                        collected_jobs: List[Dict] = []
                        page = 1
                        while len(collected_jobs) < target_to_fetch:
                            remaining = target_to_fetch - len(collected_jobs)
                            current_limit = min(page_size, remaining)
                            payload = {
                                "posted_at_gte": wide_gte,
                                "job_country_code_or": country_codes,
                                "posted_at_max_age_days": max_age_days,
                                "job_title_or": title_filters,
                                "limit": current_limit,
                                "page": page,
                                "job_id_not": seen_ids,
                            }
                            try:
                                self.logger.info(
                                    "[wide] Sending request page=%d limit=%d posted_at_gte=%s titles=%d exclude_ids=%d",
                                    page,
                                    current_limit,
                                    wide_gte,
                                    len(title_filters),
                                    len(seen_ids),
                                )
                                response = requests.post(
                                    self.api_url,
                                    json=payload,
                                    headers=self.headers,
                                    timeout=15,
                                )
                                response.raise_for_status()
                                data = response.json()
                                self._save_response_backup(
                                    "page_wide", data, payload, page=page
                                )

                                jobs = data.get("data", [])
                                if not jobs:
                                    self.logger.info(
                                        "[wide] No more jobs returned; stopping pagination at page %d.",
                                        page,
                                    )
                                    break

                                page_new_jobs = [
                                    job
                                    for job in jobs
                                    if self.state.is_job_new(str(job["id"]))
                                ]
                                try:
                                    id_preview = ", ".join(
                                        str(j["id"]) for j in jobs[:5]
                                    )
                                except Exception:
                                    id_preview = ""
                                self.logger.info(
                                    "[wide] Page %d results=%d new=%d dup=%d ids=[%s]",
                                    page,
                                    len(jobs),
                                    len(page_new_jobs),
                                    len(jobs) - len(page_new_jobs),
                                    id_preview,
                                )

                                collected_jobs.extend(page_new_jobs)
                                if len(jobs) < current_limit:
                                    self.logger.info(
                                        "[wide] Last page reached at page %d (returned=%d < limit=%d).",
                                        page,
                                        len(jobs),
                                        current_limit,
                                    )
                                    break
                                page += 1
                            except Exception as fe:
                                self.logger.error("[wide] Error fetching jobs: %s", fe)
                                break

                        # Persist state and process
                        for job in collected_jobs:
                            self.state.add_job_id(str(job["id"]))
                        self.state.update_last_run()
                        self.state.save_state()

                        from src.scraper.theirstack_processor import (
                            process_theirstack_jobs,
                        )

                        process_theirstack_jobs(collected_jobs)
                        return collected_jobs
                except Exception as we:
                    self.logger.warning("Wide pre-check failed: %s", we)
                return []
        except Exception as e:
            self.logger.error(f"Pre-check request failed: {e}")
            return []

        # Determine how many to fetch this run (respect daily cap)
        target_to_fetch = min(total_jobs, max_jobs_per_run)

        # Now make the paid, paginated requests
        collected_jobs: List[Dict] = []
        page = 1
        while len(collected_jobs) < target_to_fetch:
            remaining = target_to_fetch - len(collected_jobs)
            current_limit = min(page_size, remaining)
            payload = {
                "posted_at_gte": posted_at_gte,
                "job_country_code_or": country_codes,
                "posted_at_max_age_days": max_age_days,
                "job_title_or": title_filters,
                "limit": current_limit,
                "page": page,
                # Reduce paid duplicates as an extra safeguard
                "job_id_not": seen_ids,
            }
            try:
                self.logger.info(
                    "Sending request page=%d limit=%d posted_at_gte=%s titles=%d exclude_ids=%d",
                    page,
                    current_limit,
                    posted_at_gte,
                    len(title_filters),
                    len(seen_ids),
                )
                response = requests.post(
                    self.api_url,
                    json=payload,
                    headers=self.headers,
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
                # Persist paginated response for offline analysis
                self._save_response_backup("page", data, payload, page=page)

                # Extract page results
                jobs = data.get("data", [])
                if not jobs:
                    self.logger.info(
                        "No more jobs returned; stopping pagination at page %d.", page
                    )
                    break

                # Filter out any that may still be considered seen
                page_new_jobs = [
                    job for job in jobs if self.state.is_job_new(str(job["id"]))
                ]
                try:
                    id_preview = ", ".join(str(j["id"]) for j in jobs[:5])
                except Exception:
                    id_preview = ""
                self.logger.info(
                    "Page %d results=%d new=%d dup=%d ids=[%s]",
                    page,
                    len(jobs),
                    len(page_new_jobs),
                    len(jobs) - len(page_new_jobs),
                    id_preview,
                )

                collected_jobs.extend(page_new_jobs)

                # If fewer than requested returned, likely end of results
                if len(jobs) < current_limit:
                    self.logger.info(
                        "Last page reached at page %d (returned=%d < limit=%d).",
                        page,
                        len(jobs),
                        current_limit,
                    )
                    break

                page += 1
            except Exception as e:
                self.logger.error(f"Error fetching jobs: {e}")
                break

        # Update state and persist when we have collected something (or even if zero to advance last run)
        for job in collected_jobs:
            self.state.add_job_id(str(job["id"]))

        # Update last run date and persist
        self.state.update_last_run()
        self.state.save_state()

        # Process and save jobs
        from src.scraper.theirstack_processor import process_theirstack_jobs

        process_theirstack_jobs(collected_jobs)

        return collected_jobs


def main():
    # Check if already ran today
    state = TheirStackState()
    today = datetime.now().strftime("%Y-%m-%d")
    if state.get_last_run_date() == today:
        print(f"TheirStack scraper already ran today ({today}). Skipping.")
        return

    try:
        print("🚀 Starting TheirStack scraper...")
        scraper = TheirStackScraper()

        print("🔍 Checking for new jobs...")
        new_jobs = scraper.get_new_jobs()

        if new_jobs:
            print(f"✅ Found {len(new_jobs)} new jobs")

            # Process and save jobs to CSV
            from src.scraper.theirstack_processor import process_theirstack_jobs

            df = process_theirstack_jobs(new_jobs)

            print("\n📊 Job Summary:")
            print(f"- Total jobs processed: {len(df)}")
            print("- Source: TheirStack")
            print(
                f"- Date range: {df['posting_date'].min()} to {df['posting_date'].max()}"
            )

            print("\n🎉 Scraping completed successfully!")
        else:
            print("ℹ️ No new jobs found since last run")

    except Exception as e:
        print(f"\n❌ An error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    main()
