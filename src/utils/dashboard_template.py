"""
HTML template for the Amazon Jobs dashboard.
Separated from data_processor.py for better modularity.
"""

def generate_dashboard_html_template(total_jobs: int, active_jobs: int, last_updated: str, table_rows: str, job_categories: list, roles: list, teams: list) -> str:
    """
    Generates the static HTML dashboard template with dynamic data injected.
    
    Args:
        total_jobs: Total number of jobs.
        active_jobs: Number of active jobs.
        last_updated: Timestamp of the last update.
        table_rows: HTML string containing all the <tr> table rows.
        job_categories: A list of unique job categories for the filter dropdown.
        roles: A list of unique roles for the filter dropdown.
        teams: A list of unique teams for the filter dropdown.
        
    Returns:
        A complete HTML string for the dashboard.
    """
    category_options = "".join([f'<option value="{cat}">{cat}</option>' for cat in job_categories])
    role_options = "".join([f'<option value="{r}">{r}</option>' for r in roles])
    team_options = "".join([f'<option value="{t}">{t}</option>' for t in teams])

    return f"""
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
            cursor: pointer;
        }}
        th.sorted-asc::after {{
            content: " ▲";
        }}
        th.sorted-desc::after {{
            content: " ▼";
        }}
        tr.hidden {{
            display: none;
        }}
        .filter-controls {{
            display: flex;
            gap: 10px;
            padding: 20px;
            background: #f8f9fa;
            border-bottom: 1px solid #dee2e6;
            flex-wrap: wrap;
        }}
        .filter-controls label {{
            font-weight: 600;
            color: #495057;
        }}
        .filter-controls input, .filter-controls select {{
            padding: 8px;
            border-radius: 4px;
            border: 1px solid #dee2e6;
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
        
        <div class="filter-controls">
            <label for="search-input">Search Role/Team:</label>
            <input type="text" id="search-input" placeholder="e.g., Software, AFT">
            
            <label for="category-filter">Category:</label>
            <select id="category-filter">
                <option value="">All Categories</option>
                {category_options}
            </select>
            
            <label for="status-filter">Status:</label>
            <select id="status-filter">
                <option value="">All Statuses</option>
                <option value="Active">Active</option>
                <option value="Inactive">Inactive</option>
            </select>
        </div>

        <div class="table-container">
            <table id="job-table">
                <thead>
                    <tr>
                        <th>Title</th>
                        <th>Role</th>
                        <th>Team</th>
                        <th>Category</th>
                        <th id="posting-date-header" class="sortable">Posted</th>
                        <th>Status</th>
                        <th>Link</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>Last updated: {last_updated}</p>
            <p>Data source: Amazon Jobs Luxembourg</p>
        </div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', () => {{
            const table = document.getElementById('job-table');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const searchInput = document.getElementById('search-input');
            const categoryFilter = document.getElementById('category-filter');
            const statusFilter = document.getElementById('status-filter');
            const postingDateHeader = document.getElementById('posting-date-header');
            
            let sortDirection = 'desc';

            function applyFilters() {{
                const searchTerm = searchInput.value.toLowerCase();
                const selectedCategory = categoryFilter.value;
                const selectedStatus = statusFilter.value;

                rows.forEach(row => {{
                    const roleText = row.children[1].textContent.toLowerCase();
                    const teamText = row.children[2].textContent.toLowerCase();
                    const categoryText = row.children[3].textContent;
                    const statusText = row.children[5].textContent;

                    const matchesSearch = searchTerm === '' || roleText.includes(searchTerm) || teamText.includes(searchTerm);
                    const matchesCategory = selectedCategory === '' || categoryText === selectedCategory;
                    const matchesStatus = selectedStatus === '' || statusText === selectedStatus;

                    if (matchesSearch && matchesCategory && matchesStatus) {{
                        row.classList.remove('hidden');
                    }} else {{
                        row.classList.add('hidden');
                    }}
                }});
            }}

            function sortTable() {{
                const header = postingDateHeader;
                const columnIndex = Array.from(header.parentNode.children).indexOf(header);

                rows.sort((a, b) => {{
                    const aDate = new Date(a.children[columnIndex].textContent);
                    const bDate = new Date(b.children[columnIndex].textContent);

                    if (sortDirection === 'asc') {{
                        return aDate - bDate;
                    }} else {{
                        return bDate - aDate;
                    }}
                }});

                // Remove existing rows and re-append sorted rows
                rows.forEach(row => tbody.appendChild(row));
                
                // Toggle sort direction and update header class
                header.classList.remove('sorted-asc', 'sorted-desc');
                if (sortDirection === 'asc') {{
                    sortDirection = 'desc';
                    header.classList.add('sorted-asc');
                }} else {{
                    sortDirection = 'asc';
                    header.classList.add('sorted-desc');
                }}
            }}
            
            // Initial sort on page load (newest jobs first)
            sortTable();

            // Event listeners
            searchInput.addEventListener('input', applyFilters);
            categoryFilter.addEventListener('change', applyFilters);
            statusFilter.addEventListener('change', applyFilters);
            postingDateHeader.addEventListener('click', sortTable);
        }});
    </script>
</body>
</html>
"""