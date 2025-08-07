# Amazon Jobs Dashboard

A personal dashboard that automatically scrapes Amazon Jobs Luxembourg and displays the results as a web dashboard.

---

## 🚀 Overview

This project:
- **Scrapes Amazon Jobs** (scheduled via GitHub Actions)
- **Processes the data** (CSV to HTML)
- **Deploys to GitHub Pages** (auto-updating dashboard)

---

## 📊 Dashboard

The dashboard shows:
- **Job statistics** (total, active, inactive)
- **Job listings** in a scrollable table
- **Job details** (title, role, team, category, posting date)
- **Direct links** to job applications

---

## 🛠️ Technology Stack

- **Python** (Selenium, Pandas)
- **GitHub Actions** (automation)
- **GitHub Pages** (hosting)

---

## 📁 Project Structure


```
├── .github/workflows/scraper.yml    # Automated workflow
├── src/scraper/amazon_scraper.py    # Main scraper
├── src/utils/data_processor.py      # Dashboard generator
├── src/scripts/run_scraper.py       # Execution script
├── config/scraper_config.yaml       # Configuration
├── docs/index.html                  # Dashboard (auto-generated)
└── requirements.txt                  # Dependencies
└── README.md                         # Project documentation
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
3. Edit `config/scraper_config.yaml` if needed.
4. Run the scraper:
   ```bash
   python src/scripts/run_scraper.py
   ```
5. Generate the dashboard:
   ```bash
   python src/utils/data_processor.py
   ```

---

## ⚙️ Configuration

Edit `config/scraper_config.yaml` to change scraping parameters (e.g., base URL, number of workers).

---

## 🤖 Automation

- **GitHub Actions** runs the scraper and updates the dashboard automatically on a schedule.
- No manual intervention required for regular operation.

---

## 📝 License

Personal project. For educational and personal use only.

---

*Last updated: Automatically updated via GitHub Actions*