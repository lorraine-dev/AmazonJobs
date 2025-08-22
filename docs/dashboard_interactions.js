document.addEventListener('DOMContentLoaded', () => {
    const table = document.getElementById('job-table');
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr.job-row'));
    const searchInput = document.getElementById('search-input');
    const categoryFilter = document.getElementById('category-filter');
    const statusFilter = document.getElementById('status-filter');
    const companyFilter = document.getElementById('company-filter'); // wrapper div
    const companyToggle = companyFilter ? companyFilter.querySelector('.multi-select-toggle') : null;
    const companyMenu = companyFilter ? companyFilter.querySelector('.multi-select-menu') : null;
    const companyOptionsContainer = companyFilter ? companyFilter.querySelector('.multi-select-options') : null;
    const postingDateHeader = document.getElementById('posting-date-header');
    const skillsHeader = document.getElementById('skills-header');
    const skillsListContainer = document.getElementById('skills-list');
    const prevButton = document.getElementById('prev-page');
    const nextButton = document.getElementById('next-page');

    let sortDirection = 'desc';
    let lastCategoryKey = null; // track last category to guard resets
    let currentSkills = []; // skills computed from currently visible rows
    let currentSkillsTotalJobs = 0; // denominator for percentages

    function getSelectedCompanies() {
        if (!companyOptionsContainer) return [];
        return Array.from(companyOptionsContainer.querySelectorAll('input[type="checkbox"]:checked')).map(cb => cb.value);
    }

    // Split HTML bullet-like content into individual skill lines
    function splitHtmlBullets(html) {
        if (!html) return [];
        let text = String(html);
        // Decode HTML entities so &lt;br/&gt; etc. become real tags
        const decodeHtml = (s) => {
            const el = document.createElement('textarea');
            el.innerHTML = s;
            return el.value;
        };
        // Decode twice defensively in case of double-encoding
        text = decodeHtml(decodeHtml(text));

        // Normalize tag-based line breaks and strip list wrappers/tags
        text = text.replace(/<br\s*\/?/gi, '\n')
                   .replace(/<\/(?:li|p)\s*>/gi, '\n')
                   .replace(/<(?:li|p)\s*>/gi, '')
                   .replace(/<ul[^>]*>|<\/ul\s*>/gis, '')
                   .replace(/<[^>]+>/g, ' ');

        // Also convert literal "<br/>" text that may remain after decoding to newlines
        text = text.replace(/\s*&lt;br\s*\/?&gt;\s*/gi, '\n');
        // Collapse excessive whitespace
        text = text.replace(/\s{2,}/g, ' ');

        const raw = text.split(/\r?\n+/).map(s => s.trim()).filter(Boolean);
        const out = [];
        const seen = new Set();
        const skipPattern = /(amazon\s+is\s+an\s+equal|privacy\s+notice|inclusive\s+culture|accommodations|recruiting\s+decisions|workforce|protecting\s+your\s+privacy)/i;
        raw.forEach(line => {
            let s = line
                // Strip any leading quote/bullet/whitespace markers robustly
                .replace(/^[>\-–—•*·\s\u00A0]+/, '')
                .replace(/[\s\.;:,]+$/, '')
                .trim();
            // Filter out obvious disclaimers/URLs/overly long paragraphs that aren't skills
            const looksLikeUrl = /https?:\/\//i.test(s);
            const isNA = /^(?:n\s*\/?\s*a|n\.?a\.?|nan)$/i.test(s);
            const isNanToken = /^nan$/i.test(s);
            const hasLetters = /[a-z]/i.test(s);
            if (s && hasLetters && !isNA && !isNanToken && s.length <= 180 && !looksLikeUrl && !skipPattern.test(s) && !seen.has(s)) {
                seen.add(s);
                out.push(s);
            }
        });
        return out;
    }

    // Compute skills from the qualifications in the details of visible rows
    function computeSkillsFromVisibleRows(visibleRows) {
        const map = new Map(); // name -> { basic_count, preferred_count }
        const getNextPHtml = (detailsEl, heading) => {
            const h4s = detailsEl.querySelectorAll('h4');
            for (const h4 of h4s) {
                if ((h4.textContent || '').toLowerCase().includes(heading)) {
                    const p = h4.nextElementSibling;
                    if (p && p.tagName === 'P') return p.innerHTML || p.textContent || '';
                }
            }
            return '';
        };

        visibleRows.forEach(row => {
            const details = row.querySelector('.details-container');
            if (!details) return;
            const basicHtml = getNextPHtml(details, 'basic qualifications');
            const prefHtml = getNextPHtml(details, 'preferred qualifications');
            const basicList = splitHtmlBullets(basicHtml);
            const prefList = splitHtmlBullets(prefHtml);

            basicList.forEach(name => {
                const rec = map.get(name) || { basic_count: 0, preferred_count: 0 };
                rec.basic_count += 1;
                map.set(name, rec);
            });
            prefList.forEach(name => {
                const rec = map.get(name) || { basic_count: 0, preferred_count: 0 };
                rec.preferred_count += 1;
                map.set(name, rec);
            });
        });

        const arr = Array.from(map.entries()).map(([name, counts]) => [name, counts, counts.basic_count + counts.preferred_count]);
        arr.sort((a, b) => b[2] - a[2]);
        return arr;
    }

    // Update pagination controls from a skills list
    function updateSkillsPaginationUI(skills) {
        const totalSkills = Array.isArray(skills) ? skills.length : 0;
        const totalPages = Math.max(1, Math.ceil(totalSkills / skillsPerPage));
        const paginationContainer = document.getElementById('skills-pagination');
        const prevBtn = document.getElementById('prev-page');
        const nextBtn = document.getElementById('next-page');
        const pageInfo = document.getElementById('page-info');

        // Clamp current page defensively
        currentPage = Math.min(Math.max(1, currentPage), totalPages);

        // Always update text so stale values don't persist
        if (pageInfo) pageInfo.textContent = `Page ${currentPage} of ${totalPages}`;

        if (paginationContainer) {
            if (totalSkills > skillsPerPage) {
                paginationContainer.classList.remove('hidden');
                if (prevBtn) prevBtn.disabled = currentPage === 1;
                if (nextBtn) nextBtn.disabled = currentPage === totalPages;
            } else {
                paginationContainer.classList.add('hidden');
            }
        }
    }

    function buildFacetCompanyCounts({ searchTerm, selectedCategory, selectedStatus }) {
        const counts = {};
        rows.forEach(row => {
            const roleText = row.children[0].querySelector('.summary-content').textContent.toLowerCase();
            const companyTextRaw = row.children[1].textContent;
            const companyText = companyTextRaw.trim();
            const categoryText = row.children[2].textContent;
            const statusText = row.children[4].textContent;

            const matchesSearch = searchTerm === '' || roleText.includes(searchTerm) || companyText.toLowerCase().includes(searchTerm);
            const matchesCategory = selectedCategory === '' || categoryText === selectedCategory;
            const matchesStatus = selectedStatus === '' || statusText === selectedStatus;

            if (matchesSearch && matchesCategory && matchesStatus) {
                counts[companyText] = (counts[companyText] || 0) + 1;
            }
        });
        return counts;
    }

    function initializeCompanyMultiSelect(allCounts) {
        if (!companyOptionsContainer) return;
        if (companyOptionsContainer.childElementCount > 0) return; // already
        const companies = Object.keys(allCounts).sort((a, b) => a.localeCompare(b));
        companies.forEach(name => {
            const id = `cmp_${name.replace(/[^a-zA-Z0-9_-]/g, '_')}`;
            const wrapper = document.createElement('label');
            wrapper.className = 'multi-select-option';
            wrapper.setAttribute('for', id);

            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.id = id;
            cb.value = name;

            const textSpan = document.createElement('span');
            textSpan.className = 'option-label';
            textSpan.textContent = name;

            const countSpan = document.createElement('span');
            countSpan.className = 'option-count';
            countSpan.textContent = `(${allCounts[name] || 0})`;

            wrapper.appendChild(cb);
            wrapper.appendChild(textSpan);
            wrapper.appendChild(countSpan);
            companyOptionsContainer.appendChild(wrapper);
        });

        if (companyToggle) {
            companyToggle.addEventListener('click', (e) => {
                e.stopPropagation();
                if (!companyMenu) return;
                companyMenu.classList.toggle('hidden');
                const expanded = !companyMenu.classList.contains('hidden');
                companyToggle.setAttribute('aria-expanded', String(expanded));
            });
        }

        // Close when clicking outside
        document.addEventListener('click', () => {
            if (!companyMenu || !companyToggle) return;
            if (!companyMenu.classList.contains('hidden')) {
                companyMenu.classList.add('hidden');
                companyToggle.setAttribute('aria-expanded', 'false');
            }
        });

        // Prevent clicks inside menu from bubbling to document
        if (companyMenu) {
            companyMenu.addEventListener('click', (e) => e.stopPropagation());
        }

        // React to any checkbox change
        if (companyOptionsContainer) {
            companyOptionsContainer.addEventListener('change', () => {
                applyFilters({ resetPage: true });
            });
        }
    }

    function updateCompanyOptionLabels(newCounts) {
        if (!companyOptionsContainer) return;
        companyOptionsContainer.querySelectorAll('.multi-select-option').forEach(label => {
            const name = label.querySelector('.option-label').textContent;
            const countSpan = label.querySelector('.option-count');
            const count = newCounts[name] || 0;
            countSpan.textContent = `(${count})`;
        });
    }

    function updateCompanyToggleLabel(selectedCompanies) {
        if (!companyToggle) return;
        if (selectedCompanies.length === 0) {
            companyToggle.textContent = 'Companies: All';
        } else if (selectedCompanies.length <= 3) {
            companyToggle.textContent = `Companies: ${selectedCompanies.join(', ')}`;
        } else {
            companyToggle.textContent = `Companies: ${selectedCompanies.length} selected`;
        }
    }

    // Build filtered jobs from jobsData (source of truth for Sankey), not from DOM state
    function getFilteredJobsForSankey({ searchTerm, selectedCategory, selectedStatus, selectedCompanies }) {
        if (!Array.isArray(window.jobsData)) return [];

        const norm = (s) => (s || '').toString();
        const term = (searchTerm || '').toLowerCase();
        const wantActive = selectedStatus === 'Active' ? true : selectedStatus === 'Inactive' ? false : null;

        return window.jobsData.filter(job => {
            const title = norm(job.title).toLowerCase();
            const company = norm(job.company);
            const companyLower = company.toLowerCase();
            const category = norm(job.job_category);
            const role = norm(job.role);
            const active = typeof job.active === 'boolean' ? job.active : Boolean(job.active);

            const matchesSearch = term === '' || title.includes(term) || companyLower.includes(term);
            const matchesCategory = selectedCategory === '' || category === selectedCategory;
            const matchesStatus = wantActive === null || active === wantActive;
            const matchesCompany = selectedCompanies.length === 0 || selectedCompanies.includes(company);

            return matchesSearch && matchesCategory && matchesStatus && matchesCompany && category && company && role;
        });
    }

    function renderSankeyFromJobs(filteredJobs) {
        const container = document.getElementById('sankey-chart');
        if (!container) return;

        if (!filteredJobs || filteredJobs.length === 0) {
            container.innerHTML = '<p>No jobs to display in Sankey.</p>';
            return;
        }

        // Build flows: Category -> Company, Company -> Role
        const ccCounts = new Map(); // key: cat||company -> count
        const crCounts = new Map(); // key: company||role -> count
        const nodeSet = new Set();

        filteredJobs.forEach(j => {
            const cat = (j.job_category || '').toString();
            const company = (j.company || '').toString();
            const role = (j.role || '').toString();
            if (!cat || !company || !role) return;

            nodeSet.add(cat); nodeSet.add(company); nodeSet.add(role);

            const k1 = `${cat}||${company}`;
            const k2 = `${company}||${role}`;
            ccCounts.set(k1, (ccCounts.get(k1) || 0) + 1);
            crCounts.set(k2, (crCounts.get(k2) || 0) + 1);
        });

        const nodes = Array.from(nodeSet);
        const idFor = new Map(nodes.map((n, i) => [n, i]));

        // Category color palette (fixed, consistent with Python version)
        const palette = [
            'rgba(255,0,255, 0.8)',   // magenta
            'rgba(255,165,0, 0.8)',  // orange
            'rgba(255,255,0, 0.8)',  // yellow
            'rgba(0,128,0, 0.8)',    // green
            'rgba(0,0,255, 0.8)',    // blue
            'rgba(128,0,128, 0.8)',  // purple
            'rgba(0,255,255, 0.8)',  // cyan
        ];
        const categories = Array.from(new Set(filteredJobs.map(j => (j.job_category || '').toString()).filter(Boolean)));
        const categoryColor = new Map(categories.map((c, i) => [c, palette[i % palette.length]]));

        // Assign colors to nodes based on their category
        const nodeColorMap = new Map();
        filteredJobs.forEach(j => {
            const cat = (j.job_category || '').toString();
            const company = (j.company || '').toString();
            const role = (j.role || '').toString();
            const col = categoryColor.get(cat) || '#CCCCCC';
            if (cat) nodeColorMap.set(cat, col);
            if (company) nodeColorMap.set(company, col);
            if (role) nodeColorMap.set(role, col);
        });

        const nodeColors = nodes.map(n => nodeColorMap.get(n) || '#CCCCCC');

        // Node labels with counts
        const nodeCount = new Map();
        filteredJobs.forEach(j => {
            const cat = (j.job_category || '').toString();
            const company = (j.company || '').toString();
            const role = (j.role || '').toString();
            if (cat) nodeCount.set(cat, (nodeCount.get(cat) || 0) + 1);
            if (company) nodeCount.set(company, (nodeCount.get(company) || 0) + 1);
            if (role) nodeCount.set(role, (nodeCount.get(role) || 0) + 1);
        });
        const nodeLabels = nodes.map(n => `${n} - ${nodeCount.get(n) || 0}`);

        // Links
        const sources = [];
        const targets = [];
        const values = [];
        const linkColors = [];
        const toLinkColor = (rgba) => rgba.replace('0.8', '0.4');

        for (const [k, v] of ccCounts.entries()) {
            const [cat, company] = k.split('||');
            if (!idFor.has(cat) || !idFor.has(company)) continue;
            sources.push(idFor.get(cat));
            targets.push(idFor.get(company));
            values.push(v);
            linkColors.push(toLinkColor(nodeColorMap.get(cat) || '#CCCCCC'));
        }
        for (const [k, v] of crCounts.entries()) {
            const [company, role] = k.split('||');
            if (!idFor.has(company) || !idFor.has(role)) continue;
            sources.push(idFor.get(company));
            targets.push(idFor.get(role));
            values.push(v);
            linkColors.push(toLinkColor(nodeColorMap.get(company) || '#CCCCCC'));
        }

        // Cache base state for highlighting
        const sankeyState = {
            nodes,
            idFor,
            sources,
            targets,
            baseNodeColors: nodeColors.slice(),
            baseLinkColors: linkColors.slice(),
            nodeLabels
        };
        container.__sankeyState = sankeyState;

        const data = [{
            type: 'sankey',
            arrangement: 'fixed',
            node: {
                pad: 15,
                thickness: 15,
                line: { color: 'black', width: 0.5 },
                color: sankeyState.baseNodeColors,
                label: sankeyState.nodeLabels,
                hovertemplate: '%{label}<extra></extra>'
            },
            link: {
                source: sankeyState.sources,
                target: sankeyState.targets,
                value: values,
                color: sankeyState.baseLinkColors
            }
        }];

        const layout = {
            title: 'Amazon Luxembourg Job Flow: Category → Company → Role',
            font: { size: 10 },
            height: 600,
            margin: { l: 10, r: 10, t: 50, b: 10 }
        };

        Plotly.react(container, data, layout).then(() => {
            // Reapply highlight if a focus exists
            if (container.__sankeyFocus) {
                applySankeyHighlight(container);
            }
        });

        // Bind click-to-highlight once
        if (!container.__sankeyClickBound) {
            container.on('plotly_click', (eventData) => {
                const p = eventData && eventData.points && eventData.points[0];
                if (!p) return;
                const label = typeof p.label === 'string' ? p.label : '';
                // Our labels are "name - count"
                const name = label.includes(' - ') ? label.split(' - ')[0] : label;
                if (!name) return;

                const st = container.__sankeyState;
                if (!st) return;

                // Toggle same selection off
                if (container.__sankeyFocus === name) {
                    container.__sankeyFocus = null;
                } else {
                    container.__sankeyFocus = name;
                }

                applySankeyHighlight(container);
            });
            container.__sankeyClickBound = true;
        }
    }

    function applySankeyHighlight(container) {
        const st = container.__sankeyState;
        if (!st) return;

        const focus = container.__sankeyFocus;
        const dimAlpha = (rgba, a = '0.15') => rgba.replace(/rgba\(([^)]+),\s*([0-9.]+)\)/, (m, colors, _a) => `rgba(${colors}, ${a})`);
        const linkDimAlpha = (rgba, a = '0.08') => rgba.replace(/rgba\(([^)]+),\s*([0-9.]+)\)/, (m, colors, _a) => `rgba(${colors}, ${a})`);

        let nodeColors = st.baseNodeColors.slice();
        let linkColors = st.baseLinkColors.slice();

        if (focus) {
            // Determine node index for focus
            const idx = st.idFor.get(focus);
            if (idx !== undefined) {
                // Highlight full path: include direct upstream into the node
                // and all downstream links/nodes until sinks (Roles)
                const highlightedNodes = new Set([idx]);
                const highlightedLinks = new Set();

                // Upstream: direct incoming (Category -> Company when focus is Company, or Company -> Role when focus is Role)
                st.targets.forEach((t, i) => {
                    if (t === idx) {
                        highlightedLinks.add(i);
                        highlightedNodes.add(st.sources[i]);
                    }
                });

                // Downstream BFS to sinks
                const queue = [idx];
                const visited = new Set([idx]);
                while (queue.length > 0) {
                    const n = queue.shift();
                    st.sources.forEach((s, i) => {
                        if (s === n) {
                            highlightedLinks.add(i);
                            const t = st.targets[i];
                            if (!visited.has(t)) {
                                visited.add(t);
                                highlightedNodes.add(t);
                                queue.push(t);
                            }
                        }
                    });
                }

                // Dim all nodes/links not in highlighted sets
                nodeColors = nodeColors.map((c, i) => highlightedNodes.has(i) ? c : dimAlpha(c));
                linkColors = linkColors.map((c, i) => highlightedLinks.has(i) ? c : linkDimAlpha(c));
            }
        }

        Plotly.restyle(container, { 'node.color': [nodeColors], 'link.color': [linkColors] }, [0]);
    }

    // Animate skill bars so width transitions are visible on render
    function animateSkillBars() {
        if (!skillsListContainer) return;
        const bars = skillsListContainer.querySelectorAll('.skill-bar-basic, .skill-bar-preferred');
        bars.forEach(el => {
            const target = el.style.width;
            el.style.width = '0%';
            // force reflow
            void el.offsetWidth;
            requestAnimationFrame(() => {
                el.style.width = target;
            });
        });
    }

    function applyFilters({ resetPage = true } = {}) {
        const searchTerm = searchInput.value.toLowerCase();
        const selectedCategory = categoryFilter.value;
        const selectedStatus = statusFilter.value;
        const selectedCompanies = getSelectedCompanies();

        // Faceted counts for company filter (ignore company selection for counts)
        const facetCounts = buildFacetCompanyCounts({ searchTerm, selectedCategory, selectedStatus });
        updateCompanyOptionLabels(facetCounts);
        updateCompanyToggleLabel(selectedCompanies);

        // Apply DOM filtering for table rows
        rows.forEach(row => {
            const roleText = row.children[0].querySelector('.summary-content').textContent.toLowerCase();
            const companyTextRaw = row.children[1].textContent;
            const companyText = companyTextRaw.toLowerCase().trim();
            const categoryText = row.children[2].textContent;
            const statusText = row.children[4].textContent;

            const matchesSearch = searchTerm === '' || roleText.includes(searchTerm) || companyText.includes(searchTerm);
            const matchesCategory = selectedCategory === '' || categoryText === selectedCategory;
            const matchesStatus = selectedStatus === '' || statusText === selectedStatus;
            const matchesCompany = selectedCompanies.length === 0 || selectedCompanies.includes(companyTextRaw.trim());

            if (matchesSearch && matchesCategory && matchesStatus && matchesCompany) {
                row.classList.remove('hidden');
            } else {
                row.classList.add('hidden');
            }
        });

        // Compute dynamic skills from currently visible rows
        const categoryKey = selectedCategory || 'All Categories';
        const categoryChanged = lastCategoryKey !== categoryKey;
        lastCategoryKey = categoryKey;

        const visibleRows = Array.from(tbody.querySelectorAll('tr.job-row:not(.hidden)'));
        currentSkillsTotalJobs = visibleRows.length;
        currentSkills = computeSkillsFromVisibleRows(visibleRows);

        if (resetPage || categoryChanged) {
            currentPage = 1;
        }

        if (skillsHeader) skillsHeader.textContent = `Top Skills for ${categoryKey} (${currentSkillsTotalJobs} jobs)`;
        if (skillsListContainer) {
            skillsListContainer.innerHTML = generateSkillsHtml(currentSkills, currentSkillsTotalJobs, currentPage);
            updateSkillsPaginationUI(currentSkills);
            animateSkillBars();
        }

        // Update Sankey from source-of-truth jobsData with same filters
        const filteredForSankey = getFilteredJobsForSankey({
            searchTerm,
            selectedCategory,
            selectedStatus,
            selectedCompanies
        });
        renderSankeyFromJobs(filteredForSankey);
    }

    // Pagination event listeners
    prevButton.addEventListener('click', () => {
        currentPage = Math.max(1, currentPage - 1);
        applyFilters({ resetPage: false }); // Re-apply filters to refresh skills list
    });

    nextButton.addEventListener('click', () => {
        const totalPages = Math.max(1, Math.ceil((currentSkills || []).length / skillsPerPage));
        currentPage = Math.min(totalPages, currentPage + 1);
        applyFilters({ resetPage: false }); // Re-apply filters to refresh skills list
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

    // Initialize company options with overall counts, then render
    const initialCounts = buildFacetCompanyCounts({ searchTerm: '', selectedCategory: '', selectedStatus: '' });
    initializeCompanyMultiSelect(initialCounts);
    // Initial sort on page load (newest jobs first)
    sortTable();
    // Initial render of skills and pagination
    applyFilters({ resetPage: true });

    // Event listeners
    searchInput.addEventListener('input', () => applyFilters({ resetPage: true }));
    categoryFilter.addEventListener('change', () => {
        // Explicitly reset page on category change to ensure correct pagination
        currentPage = 1;
        applyFilters({ resetPage: true });
    });
    statusFilter.addEventListener('change', () => applyFilters({ resetPage: true }));
    // company checkbox changes handled in initializeCompanyMultiSelect
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
