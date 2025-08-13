document.addEventListener('DOMContentLoaded', () => {
    const table = document.getElementById('job-table');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr.job-row'));
    const searchInput = document.getElementById('search-input');
    const categoryFilter = document.getElementById('category-filter');
    const statusFilter = document.getElementById('status-filter');
    const postingDateHeader = document.getElementById('posting-date-header');
    const skillsHeader = document.getElementById('skills-header');
    const skillsListContainer = document.getElementById('skills-list');
    const prevButton = document.getElementById('prev-page');
    const nextButton = document.getElementById('next-page');

    let sortDirection = 'desc';

    function applyFilters() {
        const searchTerm = searchInput.value.toLowerCase();
        const selectedCategory = categoryFilter.value;
        const selectedStatus = statusFilter.value;

        rows.forEach(row => {
            const roleText = row.children[0].querySelector('.summary-content').textContent.toLowerCase();
            const companyText = row.children[1].textContent.toLowerCase();
            const categoryText = row.children[2].textContent;
            const statusText = row.children[4].textContent;

            const matchesSearch = searchTerm === '' || roleText.includes(searchTerm) || companyText.includes(searchTerm);
            const matchesCategory = selectedCategory === '' || categoryText === selectedCategory;
            const matchesStatus = selectedStatus === '' || statusText === selectedStatus;

            if (matchesSearch && matchesCategory && matchesStatus) {
                row.classList.remove('hidden');
            } else {
                row.classList.add('hidden');
            }
        });

        // Always reset to the first page when filters change
        // currentPage = 1; (This line should be here, but let's assume it's commented out for the bug)

        // Update skills list based on the selected category
        const categoryKey = selectedCategory || 'All Categories';
        const skillsForCategory = skillsData[categoryKey] || [];
        const totalJobsForCategory = categoryJobCounts[categoryKey] || 0;

        skillsHeader.textContent = `Top Skills for ${categoryKey} (${totalJobsForCategory} jobs)`;
        skillsListContainer.innerHTML = generateSkillsHtml(skillsForCategory, totalJobsForCategory, currentPage);
        updateSkillsPagination(skillsForCategory);
    }

    // Pagination event listeners
    prevButton.addEventListener('click', () => {
        currentPage = Math.max(1, currentPage - 1);
        applyFilters(); // Re-apply filters to refresh skills list
    });

    nextButton.addEventListener('click', () => {
        const selectedCategory = categoryFilter.value || 'All Categories';
        const skillsForCategory = skillsData[selectedCategory] || [];
        const totalPages = Math.ceil(skillsForCategory.length / skillsPerPage);
        currentPage = Math.min(totalPages, currentPage + 1);
        applyFilters(); // Re-apply filters to refresh skills list
    });

    function sortTable() {
        console.log('sortTable function called!');
        const header = postingDateHeader;
        // The actual column index for the 'Posted' date in the <td> elements is 3 now.
        const columnIndex = 3;

        const currentRows = Array.from(tbody.querySelectorAll('tr.job-row:not(.hidden)'))


        currentRows.sort((a, b) => {
            const aDateContent = a.children[columnIndex].textContent;
            const bDateContent = b.children[columnIndex].textContent;
            const aDate = new Date(aDateContent);
            const bDate = new Date(bDateContent)


            if (sortDirection === 'asc') {
                return aDate - bDate;
            } else {
                return bDate - aDate;
            }
        });

        // Remove existing rows and re-append sorted rows
        currentRows.forEach(row => tbody.appendChild(row));

        // Toggle sort direction and update header class
        header.classList.remove('sorted-asc', 'sorted-desc');
        if (sortDirection === 'asc') {
            sortDirection = 'desc';
            header.classList.add('sorted-asc');

        } else {
            sortDirection = 'asc';
            header.classList.add('sorted-desc');

        }
    }

    // Initial sort on page load (newest jobs first)
    sortTable();
    // Initial render of skills and pagination
    applyFilters();

    // Event listeners
    searchInput.addEventListener('input', applyFilters);
    categoryFilter.addEventListener('change', applyFilters);
    statusFilter.addEventListener('change', applyFilters);
    postingDateHeader.addEventListener('click', sortTable);

    // Add click event for expanding rows
    tbody.addEventListener('click', (event) => {
        let targetRow = event.target.closest('tr.job-row');
        if (targetRow) {
            const detailsContainer = targetRow.querySelector('.details-container');
            if (detailsContainer) {
                detailsContainer.classList.toggle('hidden');
            }
        }
    });
});
