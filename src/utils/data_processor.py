"""
Data processor for Jobs Dashboard
Converts CSV data to HTML dashboard
"""

import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional
from html import escape
from urllib.parse import urlparse

# Import the new template function
from .dashboard_template import generate_dashboard_html_template
from .data_analytics import get_skills_by_category
from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.paths import get_combined_file  # type: ignore


def _coerce_active_column(series: pd.Series) -> pd.Series:
    """Coerce assorted truthy/falsey/empty values to boolean. Defaults to True when ambiguous/missing."""
    if series is None:
        return pd.Series([True])  # fallback, should not happen
    mapped = (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map(
            {
                "true": True,
                "t": True,
                "1": True,
                "yes": True,
                "y": True,
                "false": False,
                "f": False,
                "0": False,
                "no": False,
                "n": False,
            }
        )
    )
    return mapped.fillna(True).astype(bool)


def _escape_html_text(text: str) -> str:
    """Escape text for safe HTML rendering, preserving basic newlines as <br>."""
    if text is None:
        return ""
    # escape quotes only where needed in attributes; for text nodes quote=False is fine
    return escape(str(text), quote=False).replace("\n", "<br>")


def _safe_http_url(url: str) -> str:
    """Return a safe http(s) URL for href, or '#' if invalid."""
    if not url:
        return "#"
    try:
        parsed = urlparse(str(url))
        if parsed.scheme in ("http", "https"):
            # Basic attribute escaping; quotes should be escaped in attributes
            return escape(url, quote=True)
        return "#"
    except Exception:
        return "#"


def _generate_table_rows(df: pd.DataFrame) -> str:
    """
    Generates the HTML table rows from the DataFrame.

    Args:
        df: DataFrame with job data.

    Returns:
        A string of HTML <tr> tags for the table body.
    """
    html_rows = []
    for index, row in df.iterrows():
        # Format posting date
        posting_date = row.get("posting_date", "")
        if posting_date and pd.notna(posting_date):
            try:
                # Convert to datetime object first, then format
                # pd.to_datetime can handle various input types (str, datetime, etc.)
                posting_date = pd.to_datetime(posting_date).strftime("%d %b, %Y")
            except Exception as e:
                logging.warning(f"Could not parse posting date '{posting_date}': {e}")
                # Fallback to string representation if parsing fails
                posting_date = str(posting_date)
        else:
            posting_date = "N/A"

        # Format active status (df['active'] should already be coerced to boolean)
        active_status = row.get("active", True)
        status_class = "active" if active_status else "inactive"
        status_text = "Active" if active_status else "Inactive"

        # Create job URL (fallback to 'url' if 'job_url' missing) and validate scheme
        raw_job_url = row.get("job_url") or row.get("url") or ""
        job_url = _safe_http_url(raw_job_url)

        # Display the job TITLE (not role/seniority) for all sources
        link_text = row.get("title") or row.get("role", "N/A")
        safe_link_text = _escape_html_text(link_text)
        role_link = (
            f'<a href="{job_url}" target="_blank" rel="noopener" class="job-url">{safe_link_text}</a>'
            if job_url and job_url != "#"
            else safe_link_text
        )

        # Extract description and qualifications, handling potential NaN
        description = (
            _escape_html_text(row.get("description", "N/A"))
            if pd.notna(row.get("description"))
            else _escape_html_text("N/A")
        )
        basic_qual = (
            _escape_html_text(row.get("basic_qual", "N/A"))
            if pd.notna(row.get("basic_qual"))
            else _escape_html_text("N/A")
        )
        pref_qual = (
            _escape_html_text(row.get("pref_qual", "N/A"))
            if pd.notna(row.get("pref_qual"))
            else _escape_html_text("N/A")
        )

        # We will use a single row for both summary and details.
        html_rows.append(
            f"""
            <tr class="job-row" data-job-id="{index}">
                <td class="toggle-cell">
                    <div class="summary-content">
                        {role_link}
                    </div>
                    <div class="details-container hidden">
                        <h4>Description</h4>
                        <p>{description}</p>
                        <h4>Basic Qualifications</h4>
                        <p>{basic_qual}</p>
                        <h4>Preferred Qualifications</h4>
                        <p>{pref_qual}</p>
                    </div>
                </td>
                <td>{_escape_html_text(row.get('company', 'N/A'))}</td>
                <td>{_escape_html_text(row.get('job_category', 'N/A'))}</td>
                <td>{_escape_html_text(posting_date)}</td>
                <td class="{status_class}">{status_text}</td>
            </tr>
        """
        )

    return "".join(html_rows)


def create_dashboard_html(df: pd.DataFrame) -> str:
    """
    Creates the complete HTML dashboard by combining the template and table rows.

    Args:
        df: DataFrame with job data.

    Returns:
        Complete HTML string for the dashboard.
    """
    # Normalize active column for accurate counts and rendering
    if "active" in df.columns:
        df["active"] = _coerce_active_column(df["active"])
    else:
        df["active"] = True

    total_jobs = len(df)
    active_jobs = int(df["active"].astype(bool).sum())
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get unique values for filters from the DataFrame
    job_categories = sorted(df["job_category"].dropna().unique().tolist())
    roles = sorted(df["role"].dropna().unique().tolist())
    teams = sorted(df["team"].dropna().unique().tolist())

    # Prepare the skills data for all categories
    skills_data = {}

    # Create a dictionary to store job counts for each category
    category_job_counts = {"All Categories": len(df)}

    # Get skills for ALL jobs
    all_jobs_skills = get_skills_by_category(df, job_category="ALL")
    skills_data["All Categories"] = all_jobs_skills

    # Get skills for each specific category
    for category in job_categories:
        category_df = df[df["job_category"] == category]
        skills_data[category] = get_skills_by_category(
            category_df, job_category=category
        )
        category_job_counts[category] = len(category_df)

    table_rows = _generate_table_rows(df)

    # Prepare lightweight jobs dataset for client-side Sankey rendering
    jobs_data = []
    for _, r in df.iterrows():
        jobs_data.append(
            {
                "job_category": str(r.get("job_category", ""))
                if pd.notna(r.get("job_category"))
                else "",
                "team": str(r.get("team", "")) if pd.notna(r.get("team")) else "",
                "role": str(r.get("role", "")) if pd.notna(r.get("role")) else "",
                "company": str(r.get("company", ""))
                if pd.notna(r.get("company"))
                else "",
                "title": str(r.get("title", "")) if pd.notna(r.get("title")) else "",
                "active": bool(r.get("active", True)),
            }
        )

    return generate_dashboard_html_template(
        total_jobs,
        active_jobs,
        last_updated,
        table_rows,
        job_categories,
        roles,
        teams,
        skills_data,
        category_job_counts,
        jobs_data,
    )


def csv_to_html_table(csv_path: str, output_path: Optional[str] = None) -> str:
    """
    Convert CSV data to HTML table for dashboard.

    Args:
        csv_path: Path to CSV file
        output_path: Path to save HTML file (optional)

    Returns:
        HTML string for dashboard
    """

    # Read CSV data
    try:
        df = pd.read_csv(csv_path)
        print(f"✅ Loaded {len(df)} jobs from {csv_path}")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return create_error_html("Could not load job data")

    # Coerce 'active' after load to ensure correct dtypes
    if "active" in df.columns:
        df["active"] = _coerce_active_column(df["active"])
    else:
        df["active"] = True

    # Create HTML table
    html_content = create_dashboard_html(df)

    # Save to file if output path provided
    if output_path:
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            print(f"✅ Dashboard saved to {output_path}")
        except Exception as e:
            print(f"❌ Error saving HTML: {e}")

    return html_content


def create_error_html(message: str) -> str:
    """Create error HTML when data loading fails."""

    safe_message = _escape_html_text(message)
    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Jobs Dashboard - Error</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
        }}
        .error-container {{
            background: white;
            padding: 40px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .error-icon {{
            font-size: 48px;
            color: #dc3545;
            margin-bottom: 20px;
        }}
        .error-message {{
            color: #6c757d;
            font-size: 16px;
        }}
    </style>
</head>
<body>
    <div class="error-container">
        <div class="error-icon">⚠️</div>
        <h2>Dashboard Error</h2>
        <p class="error-message">{safe_message}</p>
        <p>Please check the scraper logs for more information.</p>
    </div>
</body>
</html>
"""


def process_latest_data():
    """
    Process the latest combined jobs data and generate dashboard.
    This is the main function to be called by the scraper.
    """
    # Path to the unified jobs file (YAML-configured)
    cfg = ScraperConfig()
    combined_jobs_file = get_combined_file(cfg)

    if not combined_jobs_file.exists():
        error_msg = f"❌ Combined jobs file not found: {combined_jobs_file}"
        print(error_msg)

        # Create an error dashboard
        docs_dir = Path("docs")
        docs_dir.mkdir(exist_ok=True)
        error_html = create_error_html(error_msg)

        with open(docs_dir / "index.html", "w", encoding="utf-8") as f:
            f.write(error_html)

        return None

    print(f"📊 Processing combined jobs data: {combined_jobs_file}")

    try:
        # Generate dashboard from the combined jobs file
        html_content = csv_to_html_table(str(combined_jobs_file))

        # Save to docs directory for GitHub Pages
        docs_dir = Path("docs")
        docs_dir.mkdir(exist_ok=True)

        output_path = docs_dir / "index.html"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"✅ Dashboard generated: {output_path}")
        return str(output_path)

    except Exception as e:
        error_msg = f"❌ Error generating dashboard: {str(e)}"
        print(error_msg)

        # Create an error dashboard
        docs_dir = Path("docs")
        error_html = create_error_html(error_msg)

        with open(docs_dir / "index.html", "w", encoding="utf-8") as f:
            f.write(error_html)

        return None


if __name__ == "__main__":
    # Test the data processor
    process_latest_data()
