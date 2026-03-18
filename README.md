# Amazon + TheirStack Jobs Dashboard

A personal dashboard that automatically scrapes Amazon Jobs Luxembourg and displays the results as a web dashboard.

---

## 🚀 Overview

This project:
- **Scrapes Amazon Jobs** and **TheirStack** jobs (scheduled via GitHub Actions)
- **Combines raw CSVs** into a unified `data/processed/combined_jobs.csv`
- **Generates the dashboard** from the combined CSV
- **Deploys to GitHub Pages** (auto-updating dashboard)

For day-to-day repo state and navigation, start with `docs/START_HERE.md`.

---

## 📊 Dashboard

The dashboard shows:
- **Job statistics** (total, active, inactive)
- **Job listings** in a scrollable table with sorting, filtering, and searching capabilities.
- **Key job details** (title, role, team, category, posting date).
- **Interactive data visualizations**, including a Sankey diagram and a skill-prevalence chart.
- **Direct links** to job applications.

---

## 🛠️ Technology Stack

- **Python** (Pandas, Requests, BeautifulSoup, PyYAML, Plotly, python-dotenv, langdetect)
- **Optional engine**: Selenium + webdriver-manager (install via extras: `pip install '.[selenium]'`)
- **Dev/optional**: lxml, nbformat (used for notebooks and optional HTML/XML parsing backends)
- **GitHub Actions** (automation)
- **GitHub Pages** (hosting)

---

## 📁 Project Structure

```
├── .github/workflows/scraper.yml            # Automated workflow (scrape, combine, deploy)
├── config/
│   ├── scraper_config.yaml                  # Source configs and limits
│   ├── category_mapping.yaml                # Mapping rules for categories
│   ├── skill_aliases.yaml                   # Normalization/aliases for skill names
│   └── theirstack_titles.json               # Title normalization hints for TheirStack
├── docs/
│   ├── index.html                           # Dashboard (auto-generated)
│   ├── style.css                            # Main dashboard styles
│   ├── skills.css                           # Skills visualization styles
│   ├── dashboard_interactions.js            # Client-side interactions
│   ├── skills_taxonomy.json                 # Skills taxonomy used by extractor/visuals
│   └── reference/                           # API/reference assets
├── src/
│   ├── scraper/
│   │   ├── amazon_scraper.py                # Delegator: selects engine (api|selenium) and forwards run()
│   │   ├── amazon_api_scraper.py            # Amazon Jobs API scraper
│   │   ├── amazon_selenium_scraper.py       # Amazon Selenium scraper
│   │   ├── theirstack_scraper.py            # TheirStack API scraper
│   │   ├── theirstack_processor.py          # TheirStack → unified schema
│   │   ├── config.py                        # Scraper config helpers
│   │   └── engines.py                       # Scraper engines
│   ├── utils/
│   │   ├── paths.py                         # Centralized path helpers
│   │   ├── category_mapper.py               # Category inference
│   │   ├── raw_storage.py                   # Unified raw CSV writer with dedupe
│   │   ├── combine_jobs.py                  # Merge raw CSVs → combined CSV
│   │   ├── data_analytics.py                # Skill breakdown analytics
│   │   ├── data_processor.py                # Dashboard generator
│   │   ├── dashboard_template.py            # HTML template generator
│   │   ├── dashboard_visuals.py             # Sankey diagram and visuals
│   │   ├── description_parser.py            # JD section parser
│   │   ├── logging_utils.py                 # Logging configuration utilities
│   │   ├── health_check.py                  # Pre-flight checks and validations
│   │   ├── monitoring.py                    # Simple runtime metrics
│   │   ├── text_lang.py                     # Language detection helpers
│   │   └── theirstack_state.py              # TheirStack incremental state
│   └── scripts/
│       ├── run_scraper.py                   # Unified runner (CLI)
│       └── extract_skills.py                # Skill extraction/aggregation helper
├── data/
│   ├── raw/                                 # Raw per-source CSVs (artifacts)
│   ├── processed/
│   │   └── combined_jobs.csv                # Unified CSV for dashboard
│   └── backups/                             # TheirStack request/response backups
├── requirements.txt
├── requirements-dev.txt
├── constraints.txt
├── constraints-selenium.txt
├── renovate.json
├── mypy.ini
├── .flake8
├── setup.py
├── README.md
└── .gitignore
```

---

## ⚡ Usage

### View the Dashboard

- Visit the [GitHub Pages site](https://lorraine-dev.github.io/AmazonJobs/) (auto-updated after each scheduled run).

### Run Locally (Advanced)

1. Clone the repository.
2. Install dependencies (choose a mode):

   - API engine (minimum-supported, deterministic like CI):
     ```bash
     # Create/activate venv first
     pip install -r requirements.txt -c constraints.txt
     pip install -r requirements-dev.txt
     ```

   - Selenium engine (separate venv recommended):
     ```bash
     # Create/activate a separate venv for Selenium
     pip install -r requirements.txt -c constraints-selenium.txt '.[selenium]'
     ```
3. Create a `.env` file in the repo root (not committed):
   ```env
   THEIR_STACK_API_KEY=your_theirstack_token_here
   ```
4. Edit `config/scraper_config.yaml` if needed.
5. Run the unified scraper:
   ```bash
   python src/scripts/run_scraper.py
   ```
   Optional:
   ```bash
   # Run only TheirStack and skip dashboard
   python src/scripts/run_scraper.py --source theirstack --skip-dashboard

   # Run only Amazon
   python src/scripts/run_scraper.py --source amazon

   # Force Amazon engine (API or Selenium)
   python src/scripts/run_scraper.py --source amazon --amazon-engine api
   python src/scripts/run_scraper.py --source amazon --amazon-engine selenium
   ```
6. Generate the dashboard (if needed):
   ```bash
   python src/utils/data_processor.py
   ```

### Skill Extraction and Analysis

- Extract and canonicalize skills from the combined CSV, including inline alias scanning and taxonomy-based categories:

  ```bash
  # Default outputs to data/processed/
  python -m src.scripts.extract_skills --output-dir data/processed
  ```

  Outputs written to `data/processed/`:
  - `skills_raw_freq.csv` — Raw post-filter lines and their counts.
  - `skills_canonical_freq.csv` — Canonical skill counts via inline alias scanning.
  - `skills_pairs_sample.csv` — Sample of `(job_id, section, raw_skill, canonical_skill)` rows.
  - `skills_unmatched_lines.csv` — Lines skipped by the alias scanner (for taxonomy/alias improvements).
  - `skills_by_category.csv` — Aggregated canonical counts by taxonomy category.
  - `skills_with_category.csv` — Lookup of each canonical skill with its category and count.
  - `skills_suggestions.csv` — Proposed new skills/aliases inferred from unmatched lines and peer-of-matched tokens.

  Notes:
  - Canonical labels and aliases come from `config/skill_aliases.yaml` (overrides) merged with `docs/skills_taxonomy.json`.
  - Stop phrases in `skill_aliases.yaml` are filtered before scanning.

#### Skill Suggestions Triage Workflow

Use `skills_suggestions.csv` to iteratively improve coverage:

  1. Open `data/processed/skills_suggestions.csv` and scan top candidates by `frequency`.
  2. For accepted items:
    - Add or merge under the correct canonical in `docs/skills_taxonomy.json` (with `aliases`).
    - Optionally add to `config/skill_aliases.yaml` under `canonical_map` to override/expand aliases.
  3. Degrees and certifications are kept (e.g., Bachelor's, Master's, MBA, PhD) and categorized under `Education & Credentials`.
  4. Add noisy boilerplate phrases to `stop_phrases` in `config/skill_aliases.yaml` to reduce future noise.
  5. Re-run the extractor to validate updated counts and suggestions:
    ```bash
    python -m src.scripts.extract_skills --output-dir data/processed
    ```

#### Fuzzy merge candidates (strict)

Generate conservative fuzzy merge candidates anchored to known aliases and restricted to known canonicals:

```bash
python -m src.scripts.generate_fuzzy_merges \
  --strict-skill-like \
  --block-mode first_sig \
  --threshold 85 \
  --prune-jd-stopwords \
  --skip-both-long \
  --topk-per-variant 1 \
  --anchor-to-alias \
  --restrict-known-canonicals \
  --input-raw-csv data/processed/skills_raw_freq.csv \
  --aliases config/skill_aliases.yaml \
  --output data/processed/skills_fuzzy_candidates.csv
```

Notes:
- `--block-mode first_sig` avoids splitting related phrases into coarse length buckets.
- `--anchor-to-alias` boosts pairs sharing exactly one detected alias.
- `--restrict-known-canonicals` remaps candidate canonicals to a known alias when exactly one known alias is present in the variant.

#### Generate skills map JSON

Consolidate aliases and high-confidence fuzzy merges into a single mapping consumed by the dashboard:

```bash
python -m src.scripts.generate_skills_map \
  --aliases config/skill_aliases.yaml \
  --fuzzy-candidates data/processed/skills_fuzzy_candidates.csv \
  --min-score 100 \
  --output-config-json config/skills_map.json \
  --output-docs-json docs/skills_map.json
```

What it does:
- Builds `raw_skill` → `canonical_label` mapping from YAML aliases (authoritative) and fuzzy candidates with score ≥ `--min-score`.
- Writes the map to `config/skills_map.json` and copies to `docs/skills_map.json`.
- The dashboard (`docs/dashboard_interactions.js`) loads `docs/skills_map.json` and aggregates variants under their canonical labels.

---

## ⚙️ Configuration

Edit `config/scraper_config.yaml` to change scraping parameters (e.g., base URL, number of workers).

### HTTP reliability settings
- `common.http_retries` (default: 3) — Retry count for transient errors (429, 5xx) with exponential backoff.
- `common.http_backoff` (default: 0.5) — Backoff factor used for retries.

These apply to both Amazon API and TheirStack requests via a shared retrying HTTP session.

### Rate limiting (optional)
- `common.http_min_interval_seconds` (default: 0.0) — Minimum delay between paginated requests.
- `common.http_jitter_seconds` (default: 0.0) — Extra random delay added on top of `min_interval` (uniform in [0, jitter]).

If set, these throttle page-by-page calls in both scrapers to avoid rate limits.

### TheirStack request settings
- `theirstack.timeout_precheck` (default: 10s) — Timeout for the initial free pre-check calls.
- `theirstack.timeout_paid` (default: 15s) — Timeout for the paid paginated fetch calls.
- `theirstack.wide_fetch_limit` (default: 10) — Max jobs to fetch in the optional wide pre-check flow when pre-check finds nothing for the last-run window.

All settings are optional; sensible defaults are used if keys are absent.

---

## 🤖 Automation

- **GitHub Actions** runs every 5 days at 08:00 UTC (see `.github/workflows/scraper.yml`).
- You can also run it manually and choose the Amazon engine via the `amazon_engine` input (defaults to `api`).
- Secrets: add `THEIR_STACK_API_KEY` under Settings → Secrets and variables → Actions.
- Artifacts persisted between runs:
  - `job-data`: contents of `data/raw/` (raw CSVs)
  - `job-state`: `theirstack_state.json` (incremental scraping state)
  - `job-backups`: contents of `data/backups/` (request/response backups)
- The workflow includes a pre-run check step to ensure `THEIR_STACK_API_KEY` is present.

## 🔄 Dependency updates (Renovate)

- **What it is**: A hosted GitHub App that scans this repo and opens PRs to keep dependencies up to date. Zero runtime/storage footprint in this repo beyond a small config file.
- **What it does here**:
  - Monitors `.github/workflows/*.yml` for `uses: owner/repo@...` entries.
  - Keeps GitHub Actions pinned to commit SHAs for determinism and proposes PRs when the major tag (`v3`, `v4`, ...) advances.
- **Setup**:
  1. Install the Renovate GitHub App and grant it access to this repository.
  2. Ensure `renovate.json` exists at the repo root (already included) with the `github-actions` manager enabled and `pinDigests: true`.
  3. Renovate will open PRs grouped as "GitHub Actions updates" on the schedule `before 6am on monday`.
- **Adjust behavior**:
  - To auto-merge safe updates, set `"automerge": true` in the matching `packageRules`.
  - To stop pinning to SHAs (not recommended), set `"pinDigests": false`.
- **Docs**: https://docs.renovatebot.com

---

## 📦 Deterministic Python dependencies (constraints + Renovate)

This repo uses a constraints-based strategy to keep Python installs deterministic and upgrades safe.

### What we implemented

- **Constraints files**:
  - `constraints.txt` — exact pins for the API engine path (minimum-supported baseline used in CI).
  - `constraints-selenium.txt` — exact pins compatible with the Selenium engine (`urllib3>=2`, `requests 2.32.x`). Use this when running Selenium locally or in production.
- **Workflow install with constraints**: CI installs via:
  - `python -m pip install -U pip setuptools wheel`
  - `python -m pip install -r requirements.txt -c constraints.txt`
  - Note: Selenium is optional and installed only when explicitly selected (see Usage).
- **Caching aware of constraints**: `actions/setup-python` pip cache keys include both `requirements.txt` and `constraints.txt` to improve cache hit rate when pins change.
- **Renovate for Python deps**: `renovate.json` enables the `pip_requirements` manager for `constraints.txt` and groups PRs as "Python dependencies updates" on the same weekly schedule. Renovate does not touch `requirements.txt` (kept as lower bounds for humans).

### Why this helps

- **Reproducibility**: CI always installs the same versions → fewer surprise breakages from upstream releases.
- **Safer updates**: Renovate PRs propose pin bumps in `constraints.txt`. CI verifies the exact set before merge.
- **Performance**: Better pip cache because cache keys include the constraints file; minor speedups from disabling pip version check and pre-upgrading build tools.

### How to work locally

- API engine (baseline like CI):
  ```bash
  pip install -r requirements.txt -c constraints.txt
  pip install -r requirements-dev.txt
  ```
- Selenium engine (separate venv recommended):
  ```bash
  pip install -r requirements.txt -c constraints-selenium.txt '.[selenium]'
  ```
  If you previously installed Selenium into the API venv and hit `urllib3` conflicts, uninstall it there or recreate the venv.

### Renovate PRs you’ll see

- "GitHub Actions updates" — updates to Actions pinned SHAs.
- "Python dependencies updates" — updates to `constraints.txt` pins.

Review and merge when CI is green. If an update causes issues, revert the PR or pin back in `constraints.txt`.

---

## 🧪 Troubleshooting

- **Secret missing**: Workflow fails at step "Check TheirStack secret presence". Add `THEIR_STACK_API_KEY` in repo Settings.
- **No combined CSV**: Ensure raw CSVs exist in `data/raw/`. The combiner `src/utils/combine_jobs.py` writes `data/processed/combined_jobs.csv`.
- **Dashboard error page**: See `docs/index.html` content for the error message. Check logs in the workflow run.
- **TheirStack API issues**: Inspect JSON backups in `data/backups/` (also uploaded as `job-backups` artifact) and adjust filters in `config/scraper_config.yaml`.

---

## 📝 License

Personal project. For educational and personal use only.

---

*Last updated: Automatically updated via GitHub Actions*
