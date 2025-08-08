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

        # Default configuration
        default_config = {
            "scraper": {
                "base_url": (
                    "https://amazon.jobs/en/search?"
                    "offset=0&result_limit=10&sort=relevant"
                    "&category%5B%5D=business-intelligence"
                    "&category%5B%5D=software-development"
                    "&category%5B%5D=project-program-product-management-technical"
                    "&category%5B%5D=machine-learning-science"
                    "&category%5B%5D=data-science"
                    "&category%5B%5D=operations-it-support-engineering"
                    "&category%5B%5D=research-science"
                    "&category%5B%5D=solutions-architect"
                    "&country%5B%5D=LUX"
                    "&distanceType=Mi&radius=24km"
                    "&industry_experience=four_to_six_years"
                    "&job_level%5B%5D=5"
                    "&job_level%5B%5D=6"
                    "&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options="
                ),
                "max_workers": 3,
                "batch_size": 10,
                "delays": {"min": 1, "max": 3},
            },
            "output": {
                "data_dir": "data/raw",
                "backup_dir": "data/backups",
                "filename": "amazon_luxembourg_jobs.csv",
            },
            "logging": {
                "level": "INFO",
                "file": "logs/scraper.log",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
            "AMAZON_SCRAPER_BASE_URL": ("scraper", "base_url"),
            "AMAZON_SCRAPER_MAX_WORKERS": ("scraper", "max_workers"),
            "AMAZON_SCRAPER_BATCH_SIZE": ("scraper", "batch_size"),
            "AMAZON_SCRAPER_DATA_DIR": ("output", "data_dir"),
            "AMAZON_SCRAPER_BACKUP_DIR": ("output", "backup_dir"),
            "AMAZON_SCRAPER_FILENAME": ("output", "filename"),
            "AMAZON_SCRAPER_LOG_LEVEL": ("logging", "level"),
            "AMAZON_SCRAPER_LOG_FILE": ("logging", "file"),
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
                if key in ["max_workers", "batch_size"]:
                    current[key] = int(env_value)
                elif key in ["delays.min", "delays.max"]:
                    delay_key = key.split(".")[-1]
                    if "delays" not in current:
                        current["delays"] = {}
                    current["delays"][delay_key] = int(env_value)
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
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
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
