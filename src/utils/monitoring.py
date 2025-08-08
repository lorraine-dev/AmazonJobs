"""
Monitoring utilities for Amazon Jobs Scraper
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import json
from pathlib import Path


class ScraperMetrics:
    """Track and store scraper performance metrics."""

    def __init__(self, metrics_file: str = "logs/metrics.json"):
        self.metrics_file = Path(metrics_file)
        self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)

    def record_execution(
        self,
        total_jobs: int,
        active_jobs: int,
        execution_time: float,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> None:
        """Record execution metrics."""

        metrics = {
            "timestamp": datetime.now().isoformat(),
            "total_jobs": total_jobs,
            "active_jobs": active_jobs,
            "execution_time_seconds": execution_time,
            "jobs_per_second": total_jobs / execution_time if execution_time > 0 else 0,
            "success": success,
            "error_message": error_message,
        }

        # Load existing metrics
        existing_metrics = self._load_metrics()
        existing_metrics.append(metrics)

        # Keep only last 30 days of metrics
        cutoff_date = datetime.now() - timedelta(days=30)
        existing_metrics = [
            m
            for m in existing_metrics
            if datetime.fromisoformat(m["timestamp"]) > cutoff_date
        ]

        # Save metrics
        self._save_metrics(existing_metrics)

        # Log summary
        self.logger.info(
            f"Metrics recorded: {total_jobs} jobs, {execution_time:.2f}s, success={success}"
        )

    def get_performance_summary(self, days: int = 7) -> Dict[str, Any]:
        """Get performance summary for the last N days."""
        metrics = self._load_metrics()

        if not metrics:
            return {"error": "No metrics available"}

        # Filter by date
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_metrics = [
            m for m in metrics if datetime.fromisoformat(m["timestamp"]) > cutoff_date
        ]

        if not recent_metrics:
            return {"error": f"No metrics available for last {days} days"}

        # Calculate statistics
        execution_times = [m["execution_time_seconds"] for m in recent_metrics]
        total_jobs_list = [m["total_jobs"] for m in recent_metrics]
        success_rate = sum(1 for m in recent_metrics if m["success"]) / len(
            recent_metrics
        )

        return {
            "period_days": days,
            "total_executions": len(recent_metrics),
            "avg_execution_time": sum(execution_times) / len(execution_times),
            "min_execution_time": min(execution_times),
            "max_execution_time": max(execution_times),
            "avg_jobs_per_execution": sum(total_jobs_list) / len(total_jobs_list),
            "success_rate": success_rate,
            "last_execution": (
                recent_metrics[-1]["timestamp"] if recent_metrics else None
            ),
        }

    def _load_metrics(self) -> list:
        """Load metrics from file."""
        if not self.metrics_file.exists():
            return []

        try:
            with open(self.metrics_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    def _save_metrics(self, metrics: list) -> None:
        """Save metrics to file."""
        with open(self.metrics_file, "w") as f:
            json.dump(metrics, f, indent=2)


def setup_monitoring() -> ScraperMetrics:
    """Set up monitoring for the scraper."""
    return ScraperMetrics()
