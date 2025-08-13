"""
Data processor for Jobs Dashboard
Converts CSV data to HTML dashboard
"""

import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional

# Import the new template function
from .dashboard_template import generate_dashboard_html_template
from .dashboard_visuals import create_sankey_diagram
from .data_analytics import get_skills_by_category
from src.scraper.config import ScraperConfig  # type: ignore
from src.utils.paths import get_combined_file  # type: ignore


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

        # Format active status
        active_status = row.get("active", True)
        status_class = "active" if active_status else "inactive"
        status_text = "Active" if active_status else "Inactive"

        # Create job URL (fallback to 'url' if 'job_url' missing)
        job_url = row.get("job_url") or row.get("url") or ""

        # Display the job TITLE (not role/seniority) for all sources
        link_text = row.get("title") or row.get("role", "N/A")
        role_link = (
            f'<a href="{job_url}" target="_blank" class="job-url">{link_text}</a>'
            if job_url
            else link_text
        )

        # Extract description and qualifications, handling potential NaN
        description = (
            str(row.get("description", "N/A"))
            if pd.notna(row.get("description"))
            else "N/A"
        )
        basic_qual = (
            str(row.get("basic_qual", "N/A"))
            if pd.notna(row.get("basic_qual"))
            else "N/A"
        )
        pref_qual = (
            str(row.get("pref_qual", "N/A"))
            if pd.notna(row.get("pref_qual"))
            else "N/A"
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
                <td>{row.get('company', 'N/A')}</td>
                <td>{row.get('job_category', 'N/A')}</td>
                <td>{posting_date}</td>
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
    total_jobs = len(df)
    active_jobs = df["active"].sum() if "active" in df.columns else 0
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

    sankey_chart_html = create_sankey_diagram(df, as_html=True)

    return generate_dashboard_html_template(
        total_jobs,
        active_jobs,
        last_updated,
        table_rows,
        sankey_chart_html,
        job_categories,
        roles,
        teams,
        skills_data,
        category_job_counts,
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

    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Amazon Jobs Dashboard - Error</title>
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
        <p class="error-message">{message}</p>
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
