"""
HTML template for the Amazon Jobs dashboard.
Separated from data_processor.py for better modularity.
"""

from typing import List


def generate_dashboard_html_template(
    total_jobs: int,
    active_jobs: int,
    last_updated: str,
    table_rows: str,
    sankey_chart_html: str,
    job_categories: list,
    roles: list,
    teams: list,
    skills_data: dict,
    category_job_counts: dict,
) -> str:
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
    category_options = "".join(
        [f'<option value="{cat}">{cat}</option>' for cat in job_categories]
    )
    # role_options = "".join([f'<option value="{r}">{r}</option>' for r in roles])
    # team_options = "".join([f'<option value="{t}">{t}</option>' for t in teams])

    # Convert skills_data and category_job_counts to JSON for JavaScript
    import json

    skills_data_json = json.dumps(skills_data)
    category_job_counts_json = json.dumps(category_job_counts)

    # Generate the initial HTML for the skills list (for all categories)
    total_jobs_for_all = category_job_counts.get("All Categories", 0)
    initial_skills_html = _generate_skills_html(
        skills_data.get("All Categories", []), total_jobs_for_all
    )

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
            <div id="skills-pagination" class="skills-pagination hidden">
                <button id="prev-page" disabled>&lt; Prev</button>
                <span id="page-info">Page 1 of 1</span>
                <button id="next-page" disabled>Next &gt;</button>
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
        const skillsPerPage = 10;
        let currentPage = 1;

        function generateSkillsHtml(skills, totalJobs, page) {{
            if (!skills || skills.length === 0 || totalJobs === 0) {{
                return '<p>No specific skills found for this category.</p>';
            }}

            const start = (page - 1) * skillsPerPage;
            const end = start + skillsPerPage;
            const skillsToDisplay = skills.slice(start, end);

            let html = '<div class="skills-grid">';
            skillsToDisplay.forEach(skill => {{
                const name = skill[0];
                const counts = skill[1];
                const total = counts.basic_count + counts.preferred_count;
                const basicPercentage = (counts.basic_count / totalJobs) * 100;
                const preferredPercentage = (counts.preferred_count / totalJobs) * 100;
                const totalPercentage = basicPercentage + preferredPercentage;

                html += `
                    <div class="skill-item">
                        <span class="skill-name">${{name}}</span>
                        <div class="skill-bar-container">
                            <div class="skill-bar-basic" style="width: ${{basicPercentage.toFixed(1)}}%;"></div>
                            <div class="skill-bar-preferred" style="width: ${{preferredPercentage.toFixed(1)}}%;"></div>
                        </div>
                        <span class="skill-percentage">${{totalPercentage.toFixed(1)}}%</span>
                    </div>
                `;
            }});
            html += '</div>';
            return html;
        }}

        function updateSkillsPagination(skills) {{
            const totalSkills = skills.length;
            const totalPages = Math.ceil(totalSkills / skillsPerPage);
            const paginationContainer = document.getElementById('skills-pagination');
            const prevButton = document.getElementById('prev-page');
            const nextButton = document.getElementById('next-page');
            const pageInfo = document.getElementById('page-info');

            if (totalSkills > skillsPerPage) {{
                paginationContainer.classList.remove('hidden');
                prevButton.disabled = currentPage === 1;
                nextButton.disabled = currentPage === totalPages;
                pageInfo.textContent = `Page ${{currentPage}} of ${{totalPages}}`;
            }} else {{
                paginationContainer.classList.add('hidden');
            }}
        }}
    </script>
    <script src="dashboard_interactions.js"></script>
</body>
</html>
"""


def _generate_skills_html(skills_list: List, total_jobs_in_category: int) -> str:
    """
    Helper function to generate the HTML for the skills list.
    This version returns the full list, and the client-side JS handles pagination.
    """
    if not skills_list or total_jobs_in_category == 0:
        return "<p>No specific skills found for this category.</p>"

    html = '<div class="skills-grid">'
    for skill_item in skills_list:
        skill, counts, total = skill_item
        basic_percentage = (counts["basic_count"] / total_jobs_in_category) * 100
        preferred_percentage = (
            counts["preferred_count"] / total_jobs_in_category
        ) * 100
        total_percentage = basic_percentage + preferred_percentage
        html += f"""
            <div class="skill-item">
                <span class="skill-name">{skill}</span>
                <div class="skill-bar-container">
                    <div class="skill-bar-basic" style="width: {basic_percentage:.1f}%;"></div>
                    <div class="skill-bar-preferred" style="width: {preferred_percentage:.1f}%;"></div>
                </div>
                <span class="skill-percentage">{total_percentage:.1f}%</span>
            </div>
        """
    html += "</div>"
    return html
