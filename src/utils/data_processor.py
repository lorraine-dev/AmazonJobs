"""
Data processor for Amazon Jobs Scraper
Converts CSV data to HTML dashboard
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import os

def csv_to_html_table(csv_path: str, output_path: str = None) -> str:
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
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"✅ Dashboard saved to {output_path}")
        except Exception as e:
            print(f"❌ Error saving HTML: {e}")
    
    return html_content

def create_dashboard_html(df: pd.DataFrame) -> str:
    """
    Create HTML dashboard from DataFrame.
    
    Args:
        df: DataFrame with job data
        
    Returns:
        HTML string
    """
    
    # Get basic stats
    total_jobs = len(df)
    active_jobs = df['active'].sum() if 'active' in df.columns else 0
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Amazon Jobs Dashboard</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: #232f3e;
            color: white;
            padding: 20px;
            text-align: center;
        }}
        .stats {{
            display: flex;
            justify-content: space-around;
            padding: 20px;
            background: #f8f9fa;
            border-bottom: 1px solid #dee2e6;
        }}
        .stat {{
            text-align: center;
        }}
        .stat-number {{
            font-size: 24px;
            font-weight: bold;
            color: #232f3e;
        }}
        .stat-label {{
            font-size: 14px;
            color: #6c757d;
        }}
        .table-container {{
            overflow-x: auto;
            padding: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #dee2e6;
        }}
        th {{
            background-color: #f8f9fa;
            font-weight: 600;
            color: #495057;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .active {{
            color: #28a745;
            font-weight: bold;
        }}
        .inactive {{
            color: #dc3545;
        }}
        .footer {{
            padding: 20px;
            text-align: center;
            color: #6c757d;
            font-size: 12px;
            border-top: 1px solid #dee2e6;
        }}
        .job-title {{
            font-weight: 600;
            color: #232f3e;
        }}
        .job-url {{
            color: #007bff;
            text-decoration: none;
        }}
        .job-url:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Amazon Jobs Dashboard</h1>
            <p>Personal job monitoring dashboard</p>
        </div>
        
        <div class="stats">
            <div class="stat">
                <div class="stat-number">{total_jobs}</div>
                <div class="stat-label">Total Jobs</div>
            </div>
            <div class="stat">
                <div class="stat-number">{active_jobs}</div>
                <div class="stat-label">Active Jobs</div>
            </div>
            <div class="stat">
                <div class="stat-number">{total_jobs - active_jobs}</div>
                <div class="stat-label">Inactive Jobs</div>
            </div>
        </div>
        
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Title</th>
                        <th>Role</th>
                        <th>Team</th>
                        <th>Category</th>
                        <th>Posted</th>
                        <th>Status</th>
                        <th>Link</th>
                    </tr>
                </thead>
                <tbody>
"""
    
    # Add table rows
    for _, row in df.iterrows():
        # Format posting date
        posting_date = row.get('posting_date', '')
        if posting_date and pd.notna(posting_date):
            try:
                if isinstance(posting_date, str):
                    posting_date = pd.to_datetime(posting_date).strftime("%Y-%m-%d")
                else:
                    posting_date = posting_date.strftime("%Y-%m-%d")
            except:
                posting_date = str(posting_date)
        else:
            posting_date = "N/A"
        
        # Format active status
        active_status = row.get('active', True)
        status_class = "active" if active_status else "inactive"
        status_text = "Active" if active_status else "Inactive"
        
        # Create job URL
        job_url = row.get('job_url', '')
        job_link = f'<a href="{job_url}" target="_blank" class="job-url">View Job</a>' if job_url else "N/A"
        
        # Add row to HTML
        html += f"""
                    <tr>
                        <td class="job-title">{row.get('title', 'N/A')}</td>
                        <td>{row.get('role', 'N/A')}</td>
                        <td>{row.get('team', 'N/A')}</td>
                        <td>{row.get('job_category', 'N/A')}</td>
                        <td>{posting_date}</td>
                        <td class="{status_class}">{status_text}</td>
                        <td>{job_link}</td>
                    </tr>
"""
    
    # Close HTML
    html += f"""
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>Last updated: {last_updated}</p>
            <p>Data source: Amazon Jobs Luxembourg</p>
        </div>
    </div>
</body>
</html>
"""
    
    return html

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
    Process the latest CSV data and generate dashboard.
    This is the main function to be called by the scraper.
    """
    
    # Find the latest CSV file
    data_dir = Path("data/raw")
    csv_files = list(data_dir.glob("*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in data/raw/")
        return None
    
    # Get the most recent CSV file
    latest_csv = max(csv_files, key=lambda x: x.stat().st_mtime)
    print(f"📊 Processing latest data: {latest_csv}")
    
    # Generate dashboard
    html_content = csv_to_html_table(str(latest_csv))
    
    # Save to docs directory for GitHub Pages
    docs_dir = Path("docs")
    docs_dir.mkdir(exist_ok=True)
    
    output_path = docs_dir / "index.html"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Dashboard generated: {output_path}")
    return str(output_path)

if __name__ == "__main__":
    # Test the data processor
    process_latest_data() 