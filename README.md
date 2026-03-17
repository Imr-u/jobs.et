# Afriworket Job Scraper

Automated scraper for [afriworket.com/jobs](https://afriworket.com/jobs) that runs daily via GitHub Actions and saves results as a Parquet file.

---

## What it scrapes

Each run collects up to **100 unique job listings**, visiting every detail page for the full data set:

| Field | Source |
|---|---|
| `title` | Detail page h1 |
| `company` | Listing card |
| `location` | Listing card |
| `industry` | Detail page (e.g. Media & Entertainment) |
| `posted_relative` | Listing card (e.g. "3 hours ago") |
| `posted_date` | Detail page (e.g. "March 16, 2026") |
| `deadline` | Both |
| `job_type` | Listing card (e.g. Onsite - Full Time) |
| `experience_level` | Detail page (Expert / Senior / Intermediate / Junior) |
| `vacancies` | Detail page |
| `education_qualification` | Detail page |
| `applicants_needed` | Detail page (Male / Female / Both) |
| `salary` | Detail page (numeric ETB) |
| `salary_period` | Detail page (Monthly / etc.) |
| `skills` | Detail page (comma-separated) |
| `work_address` | Detail page |
| `full_description` | Detail page |
| `company_profile_url` | Detail page |
| `company_jobs_posted` | Detail page |
| `detail_url` | Detail page URL |
| `scraped_at` | UTC timestamp of the scrape |

**Deduplication** is performed on `(title, company, deadline)` — both during scraping and as a final safety pass before saving.

---

## Setup

### 1. Fork / clone this repo

```bash
git clone https://github.com/YOUR_USERNAME/afriworket-scraper.git
cd afriworket-scraper
```

### 2. Enable GitHub Actions

Go to **Actions** tab in your repo → click **"I understand my workflows, go ahead and enable them"**.

### 3. Give Actions write permission

Go to **Settings → Actions → General → Workflow permissions** → select **"Read and write permissions"** → Save.

### 4. Run manually (optional)

Go to **Actions → Scrape Afriworket Jobs → Run workflow**.

The parquet file will be committed to `data/jobs.parquet` and also available as a downloadable artifact.

---

## Run locally

```bash
pip install -r requirements.txt
playwright install chromium
python scraper.py
```

---

## Schedule

The workflow runs automatically every day at **06:00 UTC**.  
You can change the schedule in `.github/workflows/scrape.yml` under the `cron` field.

---

## Output

Results are saved to `data/jobs.parquet`. To read them:

```python
import pandas as pd
df = pd.read_parquet("data/jobs.parquet")
print(df.head())
```
