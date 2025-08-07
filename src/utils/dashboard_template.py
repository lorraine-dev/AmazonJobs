"""
HTML template for the Amazon Jobs dashboard.
Separated from data_processor.py for better modularity.
"""

from typing import List

def generate_dashboard_html_template(total_jobs: int, active_jobs: int, last_updated: str, table_rows: str, sankey_chart_html: str, job_categories: list, roles: list, teams: list, skills_data: dict, category_job_counts: dict) -> str:
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
        skills_data: A dictionary mapping job categories to a list of their top skills.
        category_job_counts: A dictionary with the total job count for each category.
        
    Returns:
        A complete HTML string for the dashboard.
    """
    category_options = "".join([f'<option value="{cat}">{cat}</option>' for cat in job_categories])
    role_options = "".join([f'<option value="{r}">{r}</option>' for r in roles])
    team_options = "".join([f'<option value="{t}">{t}</option>' for t in teams])

    # Convert skills_data and category_job_counts to JSON for JavaScript
    import json
    skills_data_json = json.dumps(skills_data)
    category_job_counts_json = json.dumps(category_job_counts)
    
    # Generate the initial HTML for the skills list (for all categories)
    total_jobs_for_all = category_job_counts.get('All Categories', 0)
    initial_skills_html = _generate_skills_html(skills_data.get('All Categories', []), total_jobs_for_all)

    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Amazon Jobs Dashboard</title>
    <link rel="stylesheet" href="style.css">
    <link rel="stylesheet" href="skills.css">
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
                        <th colspan="2">Role / Team</th>
                        <th>Category</th>
                        <th id="posting-date-header" class="sortable">Posted</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>

        <div id="skills-container" class="skills-container">
            <h3 id="skills-header">Top Skills for All Categories</h3>
            <div id="skills-list">
                {initial_skills_html}
            </div>
        </div>
        
        <div id="sankey-chart-container" class="sankey-container">
            <h3>Job Distribution Flow</h3>
            {sankey_chart_html}
        </div>
        <div class="footer">
            <p>Last updated: {last_updated}</p>
            <p>Data source: Amazon Jobs Luxembourg</p>
        </div>
    </div>

    <script>
        const skillsData = {skills_data_json};
        const categoryJobCounts = {category_job_counts_json};

        function generateSkillsHtml(skills, totalJobs) {{
            if (!skills || skills.length === 0 || totalJobs === 0) {{
                return '<p>No specific skills found for this category.</p>';
            }}
            let html = '<div class="skills-grid">';
            skills.slice(0, 10).forEach(skill => {{
                const name = skill[0];
                const counts = skill[1];
                const total = counts.basic_count + counts.preferred_count;
                const percentage = (total / totalJobs) * 100;
                
                html += `
                    <div class="skill-item">
                        <span class="skill-name">${{name}}</span>
                        <div class="skill-bar-container">
                            <div class="skill-bar" style="width: ${{percentage.toFixed(1)}}%;"></div>
                        </div>
                        <span class="skill-percentage">${{percentage.toFixed(1)}}%</span>
                    </div>
                `;
            }});
            html += '</div>';
            return html;
        }}

        document.addEventListener('DOMContentLoaded', () => {{
            const table = document.getElementById('job-table');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr.job-row'));
            const searchInput = document.getElementById('search-input');
            const categoryFilter = document.getElementById('category-filter');
            const statusFilter = document.getElementById('status-filter');
            const postingDateHeader = document.getElementById('posting-date-header');
            const skillsHeader = document.getElementById('skills-header');
            const skillsListContainer = document.getElementById('skills-list');
            
            let sortDirection = 'desc';

            function applyFilters() {{
                const searchTerm = searchInput.value.toLowerCase();
                const selectedCategory = categoryFilter.value;
                const selectedStatus = statusFilter.value;

                rows.forEach(row => {{
                    const roleText = row.children[0].querySelector('.summary-content').textContent.toLowerCase();
                    const teamText = row.children[1].textContent.toLowerCase();
                    const categoryText = row.children[2].textContent;
                    const statusText = row.children[4].textContent;

                    const matchesSearch = searchTerm === '' || roleText.includes(searchTerm) || teamText.includes(searchTerm);
                    const matchesCategory = selectedCategory === '' || categoryText === selectedCategory;
                    const matchesStatus = selectedStatus === '' || statusText === selectedStatus;

                    if (matchesSearch && matchesCategory && matchesStatus) {{
                        row.classList.remove('hidden');
                    }} else {{
                        row.classList.add('hidden');
                    }}
                }});

                // Update skills list based on the selected category
                const categoryKey = selectedCategory || 'All Categories';
                const skillsForCategory = skillsData[categoryKey] || [];
                const totalJobsForCategory = categoryJobCounts[categoryKey] || 0;
                
                skillsHeader.textContent = `Top Skills for ${{categoryKey}} (${{totalJobsForCategory}} jobs)`;
                skillsListContainer.innerHTML = generateSkillsHtml(skillsForCategory, totalJobsForCategory);
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

            // Add click event for expanding rows
            tbody.addEventListener('click', (event) => {{
                let targetRow = event.target.closest('tr.job-row');
                if (targetRow) {{
                    const detailsContainer = targetRow.querySelector('.details-container');
                    if (detailsContainer) {{
                        detailsContainer.classList.toggle('hidden');
                    }}
                }}
            }});
        }});
    </script>
</body>
</html>
"""

def _generate_skills_html(skills_list: List, total_jobs_in_category: int) -> str:
    """
    Helper function to generate the HTML for the skills list,
    displaying each skill's prevalence as a percentage.
    """
    if not skills_list or total_jobs_in_category == 0:
        return '<p>No specific skills found for this category.</p>'

    html = '<div class="skills-grid">'
    # We will only show the top 10 skills
    for skill_item in skills_list[:10]:
        skill, counts, total = skill_item
        percentage = (total / total_jobs_in_category) * 100
        html += f"""
            <div class="skill-item">
                <span class="skill-name">{skill}</span>
                <div class="skill-bar-container">
                    <div class="skill-bar" style="width: {percentage:.1f}%;"></div>
                </div>
                <span class="skill-percentage">{percentage:.1f}%</span>
            </div>
        """
    html += '</div>'
    return html