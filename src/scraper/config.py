"""
Configuration management for Amazon Jobs Scraper
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


class ScraperConfig:
    """
    Configuration management for the Amazon Jobs Scraper.

    Handles loading configuration from YAML files and environment variables.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration.

        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path or "config/scraper_config.yaml"
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file and environment variables."""

        # Default configuration using the new logical schema
        default_config = {
            "common": {
                "paths": {
                    "raw_dir": "data/raw",
                    "backup_dir": "data/backups",
                    "combined_file": "data/processed/combined_jobs.csv",
                },
                "logging": {
                    "level": "INFO",
                    "file": "logs/scraper.log",
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "sources": {
                "amazon": {
                    # Engine is chosen from YAML/env; defaults here are minimal to enforce central control via YAML
                    "engine": "api",
                    # Keep URLs empty in defaults; require YAML to provide base_url and/or html_base_url
                    "base_url": "",
                    "html_base_url": "",
                    # Execution controls
                    "max_workers": 3,
                    "batch_size": 10,
                    "headless": True,
                    "refresh_existing": False,
                    "delays": {"min": 1, "max": 3},
                    "raw_filename": "amazon_jobs.csv",
                    "api": {"save_page_json": False},
                    "limits": {
                        "max_pages": 0,
                        "max_jobs": 0,
                        "max_runtime_seconds": 0,
                    },
                },
                "theirstack": {
                    "api_url": "https://api.theirstack.com/v1/jobs/search",
                    "filters": {
                        "job_title_or": [
                            "Data scientist",
                            "Data Architect",
                            "Data Engineer",
                            "Data Steward",
                            "Machine learning",
                            "Software",
                            "Python",
                            "LLM",
                            "GenAI",
                            "Generative AI",
                            "ML",
                        ],
                        "job_country_code_or": ["LU"],
                        "posted_at_max_age_days": 1,
                    },
                    "limits": {
                        "page_size": 25,
                        "max_jobs_per_run": 50,
                        "max_excluded_ids": 200,
                    },
                    "raw_filename": "theirstack_jobs.csv",
                },
            },
        }

        # Load from YAML file if it exists
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r") as f:
                    yaml_config = yaml.safe_load(f)
                    if yaml_config:
                        default_config.update(yaml_config)
            except Exception as e:
                print(f"Warning: Could not load config from {self.config_path}: {e}")

        # Override with environment variables
        self._override_from_env(default_config)

        return default_config

    def _override_from_env(self, config: Dict[str, Any]):
        """Override configuration with environment variables."""

        env_mappings = {
            "AMAZON_SCRAPER_BASE_URL": ("sources", "amazon", "base_url"),
            "AMAZON_SCRAPER_HTML_BASE_URL": ("sources", "amazon", "html_base_url"),
            "AMAZON_SCRAPER_MAX_WORKERS": ("sources", "amazon", "max_workers"),
            "AMAZON_SCRAPER_BATCH_SIZE": ("sources", "amazon", "batch_size"),
            "AMAZON_SCRAPER_HEADLESS": ("sources", "amazon", "headless"),
            "AMAZON_SCRAPER_REFRESH_EXISTING": (
                "sources",
                "amazon",
                "refresh_existing",
            ),
            "AMAZON_SCRAPER_MAX_RUNTIME_SECONDS": (
                "sources",
                "amazon",
                "limits",
                "max_runtime_seconds",
            ),
            "AMAZON_ENGINE": ("sources", "amazon", "engine"),
            # Paths are YAML-driven; do not override via env to avoid drift
            "AMAZON_SCRAPER_LOG_LEVEL": ("common", "logging", "level"),
            "AMAZON_SCRAPER_LOG_FILE": ("common", "logging", "file"),
        }

        for env_var, config_path in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Navigate to the nested config location
                current = config
                for key in config_path[:-1]:
                    if key not in current:
                        current[key] = {}
                    current = current[key]

                # Set the value, converting types as needed
                key = config_path[-1]
                if key in ["max_workers", "batch_size", "max_runtime_seconds"]:
                    current[key] = int(env_value)
                elif key in ["delays.min", "delays.max"]:
                    delay_key = key.split(".")[-1]
                    if "delays" not in current:
                        current["delays"] = {}
                    current["delays"][delay_key] = int(env_value)
                elif key in ["headless", "refresh_existing"]:
                    current[key] = str(env_value).lower() in ["1", "true", "yes", "on"]
                else:
                    current[key] = env_value

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.

        Args:
            key: Configuration key (e.g., 'scraper.max_workers')
            default: Default value if key not found

        Returns:
            Configuration value
        """
        # Primary lookup
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                # Backward-compatibility aliases from old schema to new schema
                alias_map = {
                    # Paths
                    "output.data_dir": "common.paths.raw_dir",
                    "output.backup_dir": "common.paths.backup_dir",
                    "output.combined_file": "common.paths.combined_file",
                    "output.filename": "sources.amazon.raw_filename",
                    # Logging
                    "logging.level": "common.logging.level",
                    "logging.file": "common.logging.file",
                    "logging.format": "common.logging.format",
                    # Amazon scraper
                    "scraper.base_url": "sources.amazon.base_url",
                    "scraper.max_workers": "sources.amazon.max_workers",
                    "scraper.batch_size": "sources.amazon.batch_size",
                    "scraper.delays.min": "sources.amazon.delays.min",
                    "scraper.delays.max": "sources.amazon.delays.max",
                    # TheirStack
                    "theirstack.api_url": "sources.theirstack.api_url",
                    "theirstack.job_title_or": "sources.theirstack.filters.job_title_or",
                    "theirstack.job_country_code_or": "sources.theirstack.filters.job_country_code_or",
                    "theirstack.posted_at_max_age_days": "sources.theirstack.filters.posted_at_max_age_days",
                    "theirstack.page_size": "sources.theirstack.limits.page_size",
                    "theirstack.max_jobs_per_run": "sources.theirstack.limits.max_jobs_per_run",
                    "theirstack.max_excluded_ids": "sources.theirstack.limits.max_excluded_ids",
                }
                alias_key = alias_map.get(key)
                if alias_key and alias_key != key:
                    return self.get(alias_key, default)
                return default
        return value

    def get_scraper_config(self) -> Dict[str, Any]:
        """Get scraper-specific configuration."""
        return self._config.get("scraper", {})

    def get_output_config(self) -> Dict[str, Any]:
        """Get output-specific configuration."""
        return self._config.get("output", {})

    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging-specific configuration."""
        return self._config.get("logging", {})

    def update(self, key: str, value: Any):
        """
        Update configuration value.

        Args:
            key: Configuration key (e.g., 'scraper.max_workers')
            value: New value
        """
        keys = key.split(".")
        current = self._config

        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]

        current[keys[-1]] = value

    def save(self, path: Optional[str] = None):
        """
        Save configuration to YAML file.

        Args:
            path: Path to save configuration (uses config_path if None)
        """
        save_path = path or self.config_path

        # Ensure directory exists
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, "w") as f:
            yaml.dump(self._config, f, default_flow_style=False, indent=2)

    def __str__(self) -> str:
        """String representation of configuration."""
        return f"ScraperConfig(config_path='{self.config_path}')"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"ScraperConfig(config_path='{self.config_path}', config={self._config})"
