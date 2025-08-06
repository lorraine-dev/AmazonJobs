# Amazon Jobs Dashboard

A personal job monitoring dashboard that automatically scrapes Amazon Jobs Luxembourg and displays the results in a web dashboard.

## 🎯 Overview

This project automatically:
1. **Scrapes Amazon Jobs** (3x daily)
2. **Processes the data** (CSV to HTML)
3. **Deploys to GitHub Pages** (web dashboard)

## 🚀 Features

- **Automated scraping** (8am, 1pm, 6pm UTC)
- **Real-time dashboard** with job listings
- **Mobile-friendly** responsive design
- **Private repository** for personal use

## 📊 Dashboard

The dashboard shows:
- **Job statistics** (total, active, inactive)
- **Job listings** in a scrollable table
- **Job details** (title, role, team, category, posting date)
- **Direct links** to job applications

## 🛠️ Technology Stack

- **Python** - Core scraping logic
- **Selenium** - Web automation
- **Pandas** - Data processing
- **GitHub Actions** - Automated scheduling
- **GitHub Pages** - Web hosting

## 📁 Repository Structure

```
├── .github/workflows/scraper.yml    # Automated workflow
├── src/scraper/amazon_scraper.py    # Main scraper
├── src/utils/data_processor.py      # Dashboard generator
├── src/scripts/run_scraper.py       # Execution script
├── config/scraper_config.yaml       # Configuration
├── docs/index.html                  # Dashboard (auto-generated)
└── requirements.txt                  # Dependencies
```

## 🔧 Configuration

The scraper is configured via `config/scraper_config.yaml`:

```yaml
scraper:
  base_url: "https://amazon.jobs/en/search?..."
  max_workers: 3
  batch_size: 10
```

## 📈 Workflow

1. **GitHub Actions** runs the scraper 3x daily
2. **Scraper** collects job data and saves to CSV
3. **Data processor** converts CSV to HTML dashboard
4. **GitHub Pages** serves the dashboard automatically

## 🔒 Privacy

- **Private repository** by default
- **Personal use only**
- **No data sharing**

## 📝 License

Personal project - not for distribution.

---

*Last updated: Automatically updated 3x daily* 