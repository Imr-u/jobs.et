"""
Afriworket Job Scraper
======================
- Scrapes up to 100 job listings from afriworket.com/jobs per run
- Appends to existing parquet then deduplicates on (title, company, deadline)
- All missing fields are None — never empty string
- Numeric fields use pandas Int64 (nullable)
- Null % audit printed after every run
"""

import re
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_URL    = "https://afriworket.com"
JOBS_URL    = f"{BASE_URL}/jobs"
MAX_JOBS    = 100
OUTPUT_DIR  = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "jobs.parquet"
MONTHS      = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"

NAV_TEXTS = {"view details", "load more", "login", "register", "back"}


# ── helpers ────────────────────────────────────────────────────────────────────

def clean_or_none(text):
    if text is None:
        return None
    normalised = re.sub(r"\s+", " ", str(text)).strip()
    return normalised if normalised else None


def parse_or_none(pattern, text, group=1, flags=re.IGNORECASE):
    m = re.search(pattern, text, flags)
    if not m:
        return None
    return clean_or_none(m.group(group))


def to_int_or_none(value):
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


# ── detail page parser ─────────────────────────────────────────────────────────

def parse_detail_page(page, url, retries=3):
    empty = {
        "title":                   None,
        "company":                 None,
        "location":                None,
        "industry":                None,
        "posted_date":             None,
        "deadline":                None,
        "job_type":                None,
        "vacancies_raw":           None,
        "vacancies":               None,
        "education_qualification": None,
        "applicants_needed":       None,
        "work_address":            None,
        "salary_raw":              None,
        "salary":                  None,
        "salary_period":           None,
        "experience_level":        None,
        "skills":                  None,
        "full_description":        None,
        "company_profile_url":     None,
        "company_jobs_posted_raw": None,
        "company_jobs_posted":     None,
        "detail_url":              url,
        "detail_parse_ok":         False,
    }

    for attempt in range(1, retries + 1):
        try:
            page.goto(url, wait_until="networkidle", timeout=45_000)
            page.wait_for_timeout(2000)

            try:
                page.wait_for_selector("h1", timeout=10_000)
            except PWTimeout:
                print(f"  [attempt {attempt}/{retries}] No h1 yet — retrying ...")
                time.sleep(2 * attempt)
                continue

            h1 = page.query_selector("h1")
            h1_text = clean_or_none(h1.inner_text()) if h1 else None
            if not h1_text:
                print(f"  [attempt {attempt}/{retries}] h1 empty — retrying ...")
                time.sleep(2 * attempt)
                continue

            extra = dict(empty)
            extra["title"] = h1_text

            h2s = page.query_selector_all("h2")
            if h2s:
                extra["industry"] = clean_or_none(h2s[0].inner_text())

            raw_body = page.inner_text("body") or ""

            extra["posted_date"] = parse_or_none(
                rf"Posted\s+((?:{MONTHS})[a-z]*\.?\s+\d{{1,2}},?\s+\d{{4}})", raw_body
            )
            extra["deadline"] = parse_or_none(
                rf"Deadline[:\s]+((?:{MONTHS})[a-z]*\.?\s+\d{{1,2}},?\s+\d{{4}})", raw_body
            )
            extra["location"] = parse_or_none(
                r"([A-Za-z ]+,\s*Ethiopia|Addis Ababa(?:,\s*Ethiopia)?)", raw_body
            )
            extra["job_type"] = parse_or_none(
                r"Job Type[:\s]*((?:Onsite|Remote|Hybrid)\s*[-–]\s*(?:Full Time|Part Time|Contract|Freelance))",
                raw_body
            )
            if not extra["job_type"]:
                extra["job_type"] = parse_or_none(
                    r"((?:Onsite|Remote|Hybrid)\s*[-–]\s*(?:Full Time|Part Time|Contract|Freelance))",
                    raw_body
                )

            def kv(label):
                same = parse_or_none(
                    rf"^{re.escape(label)}\s*[:\-]?\s*(.+)",
                    raw_body, flags=re.IGNORECASE | re.MULTILINE
                )
                if same:
                    return same
                m = re.search(
                    rf"^{re.escape(label)}\s*$\s*^(.+)",
                    raw_body, flags=re.IGNORECASE | re.MULTILINE
                )
                return clean_or_none(m.group(1)) if m else None

            extra["vacancies_raw"]            = kv("Vacancies")
            extra["education_qualification"]  = kv("Education Qualification")
            extra["applicants_needed"]        = kv("Applicants Needed")
            extra["work_address"]             = kv("Work Address")
            extra["vacancies"]                = to_int_or_none(extra["vacancies_raw"])

            sal_m = re.search(r"([\d,]+)\s*ETB(?:\s+(\w+))?", raw_body, re.IGNORECASE)
            if sal_m:
                extra["salary_raw"]    = clean_or_none(sal_m.group(0))
                extra["salary"]        = to_int_or_none(sal_m.group(1))
                period = clean_or_none(sal_m.group(2))
                extra["salary_period"] = period if period and len(period) < 20 else None

            exp_m = re.search(
                r"\b(Expert|Senior|Intermediate|Junior|Entry[- ]?Level)\b",
                raw_body, re.IGNORECASE
            )
            extra["experience_level"] = clean_or_none(exp_m.group(1)) if exp_m else None

            skills_m = re.search(
                r"Skills And Expertise\s*\n([\s\S]+?)(?:\nWork Address|\nJob Description|\ncompany\b)",
                raw_body, re.IGNORECASE
            )
            if skills_m:
                raw_skills = skills_m.group(1).strip()
                tokens = re.findall(
                    r"[A-ZÁÉÍÓÚ][a-záéíóúäöü]+(?:\s+[A-ZÁÉÍÓÚ][a-záéíóúäöü]+)*",
                    raw_skills
                )
                extra["skills"] = ", ".join(tokens) if tokens else clean_or_none(raw_skills)

            desc_m = re.search(
                r"Job Description\s*\n([\s\S]+?)(?:\nSkills And Expertise|\n+Jobs Posted:|\Z)",
                raw_body, re.IGNORECASE
            )
            if desc_m:
                desc = clean_or_none(desc_m.group(1))
                extra["full_description"] = desc if desc and len(desc) >= 30 else None

            co_m = re.search(r"([^\n]{3,80})\s*\ncompany\s*\nJobs Posted:\s*\d+", raw_body, re.IGNORECASE)
            if co_m:
                extra["company"] = clean_or_none(co_m.group(1))
            if not extra["company"]:
                sib = page.query_selector("h1 + p, h1 + div, h1 ~ p")
                if sib:
                    candidate = clean_or_none(sib.inner_text())
                    if candidate and len(candidate) < 80:
                        extra["company"] = candidate

            company_link = page.query_selector("a[href*='/company/']")
            if company_link:
                href = company_link.get_attribute("href") or ""
                if href:
                    extra["company_profile_url"] = (
                        BASE_URL + href if href.startswith("/") else href
                    )

            jp_raw = parse_or_none(r"Jobs Posted:\s*(\d+)", raw_body)
            extra["company_jobs_posted_raw"] = jp_raw
            extra["company_jobs_posted"]     = to_int_or_none(jp_raw)

            extra["detail_parse_ok"] = True
            return extra

        except PWTimeout:
            print(f"  [attempt {attempt}/{retries}] Timeout on {url}")
            time.sleep(3 * attempt)
        except Exception as exc:
            print(f"  [attempt {attempt}/{retries}] Error: {exc}")
            time.sleep(2 * attempt)

    print(f"  FAILED after {retries} attempts: {url}")
    return empty


# ── main scraper ───────────────────────────────────────────────────────────────

def scrape():
    OUTPUT_DIR.mkdir(exist_ok=True)
    scraped = []   # raw results from this run only

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )

        def block_resources(route):
            if route.request.resource_type in ("image", "media", "font"):
                route.abort()
            else:
                route.continue_()

        list_page   = context.new_page()
        detail_page = context.new_page()
        list_page.route("**/*", block_resources)
        detail_page.route("**/*", block_resources)

        print(f"Opening {JOBS_URL} ...")
        list_page.goto(JOBS_URL, wait_until="networkidle", timeout=60_000)
        list_page.wait_for_timeout(2000)

        while len(scraped) < MAX_JOBS:
            anchors = list_page.query_selector_all("a[href^='/jobs/']")
            job_anchors = [
                a for a in anchors
                if re.search(r"/jobs/[0-9a-f-]{36}$", a.get_attribute("href") or "")
                and (a.inner_text() or "").strip().lower() not in NAV_TEXTS
            ]
            print(f"  Found {len(job_anchors)} valid job title links ...")

            for anchor in job_anchors:
                if len(scraped) >= MAX_JOBS:
                    break

                href       = anchor.get_attribute("href") or ""
                detail_url = BASE_URL + href if href.startswith("/") else href
                title      = clean_or_none(anchor.inner_text())

                # Walk up to card container for fallback fields
                card_text = ""
                container = anchor
                for _ in range(5):
                    parent = container.evaluate_handle("el => el.parentElement")
                    if not parent:
                        break
                    parent_el = parent.as_element()
                    if not parent_el:
                        break
                    text = parent_el.inner_text() or ""
                    container = parent_el
                    card_text = text
                    if re.search(r"Onsite|Remote|Hybrid|Posted\s+\d", text, re.IGNORECASE):
                        break

                company = None
                for line in [l.strip() for l in card_text.splitlines() if l.strip()]:
                    if line == title:
                        continue
                    if re.search(
                        r"Posted|Deadline|Onsite|Remote|Hybrid|Full Time|Part Time|"
                        r"Expert|Senior|Intermediate|Junior|Entry|View Details|ETB|\d{4}",
                        line, re.IGNORECASE
                    ):
                        continue
                    if len(line) < 3:
                        continue
                    company = clean_or_none(line)
                    break

                location        = parse_or_none(r"([A-Za-z ]+,\s*Ethiopia|Addis Ababa(?:,\s*Ethiopia)?)", card_text)
                posted_relative = parse_or_none(r"Posted\s+(.+?)(?:\n|$)", card_text)
                deadline        = parse_or_none(rf"((?:{MONTHS})[a-z]*\.?\s+\d{{1,2}},?\s+\d{{4}})", card_text)
                job_type        = parse_or_none(r"((?:Onsite|Remote|Hybrid)\s*[-–]\s*(?:Full Time|Part Time|Contract|Freelance))", card_text)
                exp_level       = parse_or_none(r"\b(Expert|Senior|Intermediate|Junior|Entry[- ]?Level)\b", card_text)

                print(f"  [{len(scraped)+1}/{MAX_JOBS}] {(title or '(no title)')[:55]} ...")

                detail = parse_detail_page(detail_page, detail_url)

                scraped.append({
                    "title":                   detail["title"] or title,
                    "company":                 detail["company"] or company,
                    "location":                detail["location"] or location,
                    "industry":                detail["industry"],
                    "posted_relative":         posted_relative,
                    "posted_date":             detail["posted_date"],
                    "deadline":                detail["deadline"] or deadline,
                    "job_type":                detail["job_type"] or job_type,
                    "experience_level":        detail["experience_level"] or exp_level,
                    "vacancies":               detail["vacancies"],
                    "education_qualification": detail["education_qualification"],
                    "applicants_needed":       detail["applicants_needed"],
                    "salary":                  detail["salary"],
                    "salary_period":           detail["salary_period"],
                    "salary_raw":              detail["salary_raw"],
                    "skills":                  detail["skills"],
                    "work_address":            detail["work_address"],
                    "full_description":        detail["full_description"],
                    "company_profile_url":     detail["company_profile_url"],
                    "company_jobs_posted":     detail["company_jobs_posted"],
                    "detail_url":              detail_url,
                    "detail_parse_ok":         detail["detail_parse_ok"],
                    "scraped_at":              datetime.utcnow().isoformat(),
                })
                time.sleep(1.2)

            if len(scraped) >= MAX_JOBS:
                break

            load_more = list_page.query_selector(
                "button:has-text('Load More'), a:has-text('Load More')"
            )
            if not load_more:
                print("  No 'Load More' button — listing page exhausted.")
                break
            print("  Clicking 'Load More' ...")
            load_more.scroll_into_view_if_needed()
            load_more.click()
            list_page.wait_for_timeout(3000)

        browser.close()

    print(f"\nScraped {len(scraped)} jobs this run.")

    # ── load existing + concat + dedup + save ──────────────────────────────────
    new_df = pd.DataFrame(scraped)

    if OUTPUT_FILE.exists():
        existing_df = pd.read_parquet(OUTPUT_FILE, engine="pyarrow")
        print(f"Existing records: {len(existing_df)}")
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        print("No existing parquet — creating fresh.")
        combined_df = new_df

    # Cast nullable integers
    for col in ("salary", "vacancies", "company_jobs_posted"):
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors="coerce").astype("Int64")

    # Dedup across full combined dataset — keep first occurrence (older entry wins)
    before = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=["title", "company", "deadline"], keep="first")
    dupes_dropped = before - len(combined_df)
    new_net = len(combined_df) - (len(existing_df) if OUTPUT_FILE.exists() else 0)

    print(f"Duplicates dropped: {dupes_dropped}")
    print(f"Net new records added: {new_net}")
    print(f"Total records in storage: {len(combined_df)}")

    # scraped_at always last column
    cols = [c for c in combined_df.columns if c != "scraped_at"] + ["scraped_at"]
    combined_df = combined_df[cols]

    # ── null audit on this run's new rows only ─────────────────────────────────
    print(f"\nNull % per column (this run, {len(new_df)} rows):")
    null_pct = (new_df.isna().sum() / len(new_df) * 100).round(1)
    for col, pct in null_pct.items():
        flag = "  <-- HIGH" if pct > 50 else ""
        print(f"  {col:<30} {pct:>5}%{flag}")

    failed = new_df[~new_df["detail_parse_ok"]].shape[0]
    if failed:
        print(f"\n  WARNING: {failed} rows had detail_parse_ok=False")

    combined_df.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")
    print(f"\nSaved → {OUTPUT_FILE}")


if __name__ == "__main__":
    scrape()