#!/usr/bin/env python3
"""
Health check for Amazon Jobs Scraper
Monitors data freshness, file integrity, and scraper status
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import sys
from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.paths import get_raw_path  # type: ignore


def check_scraper_health():
    """Check if the scraper is running properly."""

    print("🔍 Amazon Jobs Scraper Health Check")
    print("=" * 50)

    # Check if data file exists and is recent (centralized via config)
    cfg = ScraperConfig()
    data_file = str(get_raw_path("amazon", cfg))
    if not os.path.exists(data_file):
        print("❌ Data file not found")
        print(f"   Expected: {data_file}")
        return False

    # Check file modification time
    mtime = os.path.getmtime(data_file)
    last_modified = datetime.fromtimestamp(mtime)
    hours_ago = datetime.now() - last_modified

    print(f"📅 Last updated: {last_modified.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ Age: {hours_ago.total_seconds()/3600:.1f} hours ago")

    if hours_ago > timedelta(hours=8):
        print("⚠️  WARNING: Data file is old (>8 hours)")
        print("   Consider running the scraper manually")

    # Check data quality
    try:
        df = pd.read_csv(data_file)

        if len(df) == 0:
            print("❌ Data file is empty")
            return False

        # Basic statistics
        total_jobs = len(df)
        active_jobs = df["active"].sum() if "active" in df.columns else 0
        inactive_jobs = total_jobs - active_jobs

        print("📊 Data Statistics:")
        print(f"   Total jobs: {total_jobs}")
        print(f"   Active jobs: {active_jobs}")
        print(f"   Inactive jobs: {inactive_jobs}")

        # Check for required columns
        required_columns = ["id", "title", "job_url", "active"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            return False

        # Check for recent activity (jobs posted in last 30 days)
        if "posting_date" in df.columns:
            try:
                # Convert posting_date to datetime if it's not already
                if df["posting_date"].dtype == "object":
                    df["posting_date"] = pd.to_datetime(
                        df["posting_date"], errors="coerce"
                    )

                recent_jobs = df[
                    df["posting_date"] >= (datetime.now() - timedelta(days=30))
                ]
                print(f"   Recent jobs (30 days): {len(recent_jobs)}")

            except Exception as e:
                print(f"⚠️  Could not analyze posting dates: {e}")

        # Check backup directory
        backup_dir = "backups"
        if os.path.exists(backup_dir):
            backup_files = [f for f in os.listdir(backup_dir) if f.endswith(".csv")]
            print(f"📦 Backups available: {len(backup_files)}")
        else:
            print("⚠️  Backup directory not found")

        # Check log directory
        log_dir = "logs"
        if os.path.exists(log_dir):
            log_files = [f for f in os.listdir(log_dir) if f.endswith(".log")]
            print(f"📝 Log files available: {len(log_files)}")

            # Check recent log activity
            scraper_log = os.path.join(log_dir, "amazon_jobs_scraper.log")
            if os.path.exists(scraper_log):
                log_mtime = os.path.getmtime(scraper_log)
                log_last_modified = datetime.fromtimestamp(log_mtime)
                log_hours_ago = datetime.now() - log_last_modified
                print(
                    f"   Last log update: {log_hours_ago.total_seconds()/3600:.1f} hours ago"
                )
        else:
            print("⚠️  Log directory not found")

        # Overall health assessment
        if hours_ago <= timedelta(hours=8) and total_jobs > 0:
            print("\n✅ Scraper appears healthy!")
            return True
        elif total_jobs > 0:
            print("\n⚠️  Scraper may need attention (data is old)")
            return False
        else:
            print("\n❌ Scraper needs immediate attention")
            return False

    except Exception as e:
        print(f"❌ Error reading data file: {e}")
        return False


def check_dependencies():
    """Check if required dependencies are available."""

    print("\n🔧 Dependency Check")
    print("-" * 30)

    required_packages = [
        "pandas",
        "requests",
        "beautifulsoup4",
        "selenium",
        "webdriver-manager",
    ]

    missing_packages = []

    for package in required_packages:
        try:
            __import__(package.replace("-", "_"))
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package}")
            missing_packages.append(package)

    if missing_packages:
        print(f"\n⚠️  Missing packages: {', '.join(missing_packages)}")
        print("   Run: pip install -r requirements.txt")
        return False
    else:
        print("\n✅ All dependencies available")
        return True


def check_directories():
    """Check if required directories exist."""

    print("\n📁 Directory Check")
    print("-" * 30)

    required_dirs = ["data", "backups", "logs"]

    for directory in required_dirs:
        if os.path.exists(directory):
            print(f"✅ {directory}/")
        else:
            print(f"❌ {directory}/ (will be created automatically)")

    return True


def main():
    """Main health check function."""

    print("Amazon Jobs Scraper - Health Check")
    print("=" * 50)

    # Check dependencies
    deps_ok = check_dependencies()

    # Check directories
    dirs_ok = check_directories()

    # Check scraper health
    scraper_ok = check_scraper_health()

    # Summary
    print("\n" + "=" * 50)
    print("📋 Health Check Summary")
    print("=" * 50)

    if deps_ok and dirs_ok and scraper_ok:
        print("✅ All systems operational")
        sys.exit(0)
    elif scraper_ok:
        print("⚠️  Minor issues detected (see details above)")
        sys.exit(1)
    else:
        print("❌ Critical issues detected")
        print("\n💡 Recommendations:")
        print("   1. Run: python amazon_jobs_scraper.py")
        print("   2. Check logs: tail -f logs/amazon_jobs_scraper.log")
        print("   3. Verify internet connectivity")
        sys.exit(1)


if __name__ == "__main__":
    main()
