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

- **Python** (Selenium, Pandas)
- **GitHub Actions** (automation)
- **GitHub Pages** (hosting)

---

## 📁 Project Structure


```
├── .github/workflows/scraper.yml        # Automated unified workflow (scrape, combine, deploy)
├── src/scraper/amazon_scraper.py        # Amazon scraper (Selenium)
├── src/scraper/theirstack_scraper.py    # TheirStack API scraper
├── src/scraper/theirstack_processor.py  # Map TheirStack fields -> unified schema
├── src/utils/raw_storage.py             # Unified raw CSV writer with dedupe
├── src/utils/combine_jobs.py            # Merge raw CSVs -> combined CSV
├── src/utils/paths.py                   # Centralized path helpers
├── src/utils/category_mapper.py         # Category inference
├── src/utils/dashboard_template.py      # HTML template generator
├── src/utils/dashboard_visuals.py       # Sankey diagram and visuals
├── src/utils/data_analytics.py          # Skill breakdown analytics
├── src/utils/data_processor.py          # Dashboard generator
├── src/scripts/run_scraper.py           # Unified runner (CLI)
├── config/scraper_config.yaml           # Source configs and limits
├── config/category_mapping.yaml         # Mapping rules for categories
├── docs/index.html                      # Dashboard (auto-generated)
├── docs/skills.css                      # Skills visualization styles
├── docs/style.css                       # Main dashboard styles
├── data/raw/                            # Raw per-source CSVs (artifacts)
├── data/processed/combined_jobs.csv     # Unified CSV for dashboard
├── data/backups/                        # TheirStack API request/response backups
└── requirements.txt                     # Dependencies
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
   ```
6. Generate the dashboard (if needed):
   ```bash
   python src/utils/data_processor.py
   ```

---

## ⚙️ Configuration

Edit `config/scraper_config.yaml` to change scraping parameters (e.g., base URL, number of workers).

---

## 🤖 Automation

- **GitHub Actions** runs 3x daily (see `.github/workflows/scraper.yml`).
- Secrets: add `THEIR_STACK_API_KEY` under Settings → Secrets and variables → Actions.
- Artifacts persisted between runs:
  - `job-data`: contents of `data/raw/` (raw CSVs)
  - `job-state`: `theirstack_state.json` (incremental scraping state)
  - `job-backups`: contents of `data/backups/` (request/response backups)
- The workflow includes a pre-run check step to ensure `THEIR_STACK_API_KEY` is present.

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
