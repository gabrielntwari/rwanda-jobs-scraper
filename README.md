# 🇷🇼 Rwanda Jobs Scraper

Automated job scraping system for Rwanda with multi-page dashboard.

## 📊 Features

- **10 Job Sources**: JobInRwanda, NewTimes, GreatRwandaJobs, Mucuruzi, MIFOTRA
- **Automated Scraping**: Runs daily at 6 AM EAT via GitHub Actions
- **Auto-Deduplication**: Removes duplicate jobs automatically
- **Beautiful Dashboard**: Multi-page Plotly Dash interface
- **Database**: Supabase PostgreSQL

## 🚀 Live Dashboard

🌐 **Dashboard URL**: [https://rwanda-jobs-scraper.onrender.com/]

## 📦 Project Structure

```
rwanda_jobs/
├── scrapers/
│   ├── jobinrwanda_scraper.py
│   ├── newtimesjobs_scraper.py
│   ├── greatrwandajobs_scraper.py
│   ├── mucuruzi_scraper.py
│   └── mifotra_scraper.py
├── db_adapter.py
├── run_all_5_scrapers.py         # Master scraper with auto-dedup
├── multi_page_dashboard.py       # Dashboard
├── requirements.txt              # Dependencies
└── .github/workflows/
    └── scrape_jobs.yml           # Automated scheduling
```

## ⚙️ Setup

### 1. Clone Repository

```bash
git clone https://github.com/gabrielntwari/rwanda-jobs-scraper.git
cd rwanda-jobs-scraper
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set Database URL

```bash
# Windows
set DATABASE_URL=your_postgresql_url

# Linux/Mac
export DATABASE_URL=your_postgresql_url
```

### 4. Run Scrapers

```bash
python run_all_5_scrapers.py
```

### 5. Run Dashboard

```bash
python multi_page_dashboard.py
# Open http://localhost:8050
```

## 🤖 Automated Scraping

GitHub Actions runs the scrapers **daily at 6:00 AM EAT** automatically.

To enable:
1. Add `DATABASE_URL` secret in GitHub Settings → Secrets
2. Push code to GitHub
3. GitHub Actions will run automatically

## 📊 Database Schema

**Table**: `jobs`

Key columns:
- `title`, `company`, `source`, `sector`
- `district`, `employment_type`, `job_level`
- `experience_years`, `education_level`
- `description`, `posted_date`, `deadline`
- `source_url`, `scraped_at`, `is_active`

## 🎨 Dashboard Pages

1. **Job Search Portal** - Browse and filter jobs
2. **Market Insights** - Charts and analytics
3. **Historical Jobs** - Wayback Machine integration (coming soon)

## 📈 Statistics

- **Total Jobs**: 550+
- **Active Sources**: 5
- **Update Frequency**: Daily
- **Duplicate Removal**: Automatic

## 🛠️ Technologies

- **Python 3.11**
- **Scrapers**: BeautifulSoup, Cloudscraper
- **Database**: PostgreSQL (Supabase)
- **Dashboard**: Plotly Dash, Bootstrap
- **Automation**: GitHub Actions
- **Hosting**: Render.com (dashboard), GitHub (scrapers)

## 📝 License

MIT License - Feel free to use for your projects!

## 👨‍💻 Author

**Gabriel Ntwari**
- GitHub: [@gabrielntwari](https://github.com/gabrielntwari)

## 🤝 Contributing

Pull requests welcome! For major changes, please open an issue first.

## 📧 Contact

For questions or suggestions, open an issue on GitHub.

---

**⭐ Star this repo if you find it useful!**
