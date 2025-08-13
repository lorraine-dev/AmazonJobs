import json
import os
from datetime import datetime
from typing import Set, Optional


class TheirStackState:
    def __init__(self, state_file: str = "theirstack_state.json"):
        self.state_file = state_file
        # Initialize attributes with types once
        self.last_run_date: Optional[str] = None
        self.scraped_job_ids: Set[str] = set()
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                state = json.load(f)
                self.last_run_date = state.get("last_run_date", None)
                # Normalize all IDs to strings
                loaded_ids = state.get("scraped_job_ids", [])
                self.scraped_job_ids = set(str(x) for x in loaded_ids)
        else:
            # Keep defaults when no state file exists
            self.last_run_date = None
            self.scraped_job_ids = set()

    def _load_state(self):
        """Load state from file or initialize defaults"""
        if os.path.exists(self.state_file):
            with open(self.state_file, "r") as f:
                state = json.load(f)
                self.last_run_date = state.get("last_run_date", None)
                loaded_ids = state.get("scraped_job_ids", [])
                self.scraped_job_ids = set(str(x) for x in loaded_ids)
        else:
            self.last_run_date = None
            self.scraped_job_ids = set()

    def save(self):
        """Save current state to file"""
        with open(self.state_file, "w") as f:
            json.dump(
                {
                    "last_run_date": self.last_run_date,
                    "scraped_job_ids": list(self.scraped_job_ids),
                },
                f,
            )

    def save_state(self):
        """Save current state to file"""
        with open(self.state_file, "w") as f:
            json.dump(
                {
                    "last_run_date": self.last_run_date,
                    "scraped_job_ids": list(self.scraped_job_ids),
                },
                f,
                indent=2,
            )

    def update_last_run(self):
        """Update last run date to current time"""
        self.last_run_date = datetime.now().strftime("%Y-%m-%d")

    def add_job_id(self, job_id: str):
        """Add a job ID to tracked list"""
        self.scraped_job_ids.add(job_id)

    def is_job_new(self, job_id: str) -> bool:
        """Check if job ID has been seen before"""
        return job_id not in self.scraped_job_ids

    def get_last_run_date(self) -> Optional[str]:
        """Return last run date (YYYY-MM-DD) or None if never run"""
        return self.last_run_date
