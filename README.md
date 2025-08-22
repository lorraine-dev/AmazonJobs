# Amazon + TheirStack Jobs Dashboard

A personal dashboard that automatically scrapes Amazon Jobs Luxembourg and displays the results as a web dashboard.

---

## 🚀 Overview

This project:
- **Scrapes Amazon Jobs** and **TheirStack** jobs (scheduled via GitHub Actions)
- **Combines raw CSVs** into a unified `data/processed/combined_jobs.csv`
- **Generates the dashboard** from the combined CSV
- **Deploys to GitHub Pages** (auto-updating dashboard)

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
│   └── theirstack_titles.json               # Title normalization hints for TheirStack
├── docs/
│   ├── index.html                           # Dashboard (auto-generated)
│   ├── style.css                            # Main dashboard styles
│   ├── skills.css                           # Skills visualization styles
│   ├── dashboard_interactions.js            # Client-side interactions
│   └── reference/                           # API/reference assets
├── src/
│   ├── scraper/
│   │   ├── amazon_scraper.py                # Legacy/simple Amazon scraper
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
│       └── run_scraper.py                   # Unified runner (CLI)
├── data/
│   ├── raw/                                 # Raw per-source CSVs (artifacts)
│   ├── processed/
│   │   └── combined_jobs.csv                # Unified CSV for dashboard
│   └── backups/                             # TheirStack request/response backups
├── README.md
├── requirements.txt
├── setup.py
└── .gitignore
```

---

## ⚡ Usage

### View the Dashboard

- Visit the [GitHub Pages site](https://lorraine-dev.github.io/AmazonJobs/) (auto-updated after each scheduled run).

### Run Locally (Advanced)

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   If you plan to use the Selenium engine, install the optional extras:
   ```bash
   pip install '.[selenium]'
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

- **GitHub Actions** runs 3x daily at 08:00, 14:00, and 18:00 UTC (see `.github/workflows/scraper.yml`).
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
