"""
Afriworket Job Scraper
======================
- Scrapes up to 100 job listings from afriworket.com/jobs
- Visits each job detail page for full data
- Deduplicates on (title, company, deadline)
- Saves results to data/jobs.parquet

Null / empty value strategy
----------------------------
- All optional text fields use None (pd.NA in DataFrame) — never ""
- Numeric fields (salary, vacancies, company_jobs_posted) are cast to
  pandas Int64 (nullable integer) so NaN doesn't force float dtype
- parse_or_none() wraps every regex so a no-match returns None, not ""
- clean_or_none() returns None for blank/whitespace-only strings
- Page-load failures set detail_parse_ok=False so downstream users can
  filter out rows where detail extraction may be incomplete
- A final audit after building the DataFrame logs null % per column
"""

import re
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_URL  = "https://afriworket.com"
JOBS_URL  = f"{BASE_URL}/jobs"
MAX_JOBS  = 20
OUTPUT_DIR  = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "jobs.parquet"

# Months for deadline parsing
MONTHS = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"


# ── helpers ────────────────────────────────────────────────────────────────────

def clean_or_none(text: str | None) -> str | None:
    """
    Normalise whitespace.
    Returns None (not "") when the result would be blank — so callers
    never have to distinguish '' from None themselves.
    """
    if text is None:
        return None
    normalised = re.sub(r"\s+", " ", text).strip()
    return normalised if normalised else None


def parse_or_none(pattern: str, text: str,
                  group: int = 1,
                  flags: int = re.IGNORECASE) -> str | None:
    """
    Run a regex and return the requested capture group, or None on no match.
    Strips and normalises the result before returning.
    """
    m = re.search(pattern, text, flags)
    if not m:
        return None
    return clean_or_none(m.group(group))


def to_int_or_none(value: str | None) -> int | None:
    """Parse a string to int, returning None if it can't be parsed."""
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


# ── detail page parser ─────────────────────────────────────────────────────────

def parse_detail_page(page, url: str) -> dict:
    """
    Visit an individual job page and extract every available field.
    All missing fields are explicitly None — never empty string.
    Returns a dict that also includes detail_parse_ok (bool) so callers
    know whether the page loaded successfully.
    """
    # Initialise every field to None so missing data is explicit
    extra: dict = {
        "title":                   None,
        "industry":                None,
        "posted_date":             None,
        "vacancies_raw":           None,   # raw string
        "vacancies":               None,   # int
        "education_qualification": None,
        "applicants_needed":       None,
        "work_address":            None,
        "salary_raw":              None,   # original text e.g. "16,000 ETB"
        "salary":                  None,   # int (ETB)
        "salary_period":           None,
        "experience_level":        None,
        "skills":                  None,
        "full_description":        None,
        "company_profile_url":     None,
        "company_jobs_posted_raw": None,
        "company_jobs_posted":     None,   # int
        "detail_url":              url,
        "detail_parse_ok":         False,
    }

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(1500)

        # ── guard: confirm page has meaningful content ───────────────────────
        h1 = page.query_selector("h1")
        if not h1:
            print(f"  ⚠️  No <h1> found — page may not have loaded: {url}")
            return extra          # detail_parse_ok stays False

        extra["title"] = clean_or_none(h1.inner_text())

        # ── industry ─────────────────────────────────────────────────────────
        h2s = page.query_selector_all("h2")
        if h2s:
            extra["industry"] = clean_or_none(h2s[0].inner_text())

        # ── full body text (used for most field extraction) ──────────────────
        # inner_text() gives us visible text with newlines preserved.
        # We do NOT collapse newlines here — the multiline text is used later
        # for block-boundary detection.
        raw_body = page.inner_text("body") or ""

        # ── posted date ──────────────────────────────────────────────────────
        # Match "Posted Month DD, YYYY" — avoids grabbing unrelated "Posted" hits
        extra["posted_date"] = parse_or_none(
            rf"Posted\s+({MONTHS}[a-z]*\s+\d{{1,2}},\s+\d{{4}})",
            raw_body
        )

        # ── structured key-value fields ──────────────────────────────────────
        # Each label appears on its own line; value is on the same line after
        # a colon/space, or on the very next non-empty line.
        def kv(label: str) -> str | None:
            """
            Extracts the value that follows a labelled field.
            Tries same-line first; falls back to next-line.
            """
            # Same-line: "Vacancies:  2"
            same = parse_or_none(
                rf"^{re.escape(label)}\s*[:\-]?\s*(.+)",
                raw_body, flags=re.IGNORECASE | re.MULTILINE
            )
            if same:
                return same
            # Next-line: label on one line, value on the next
            m = re.search(
                rf"^{re.escape(label)}\s*$\s*^(.+)",
                raw_body, flags=re.IGNORECASE | re.MULTILINE
            )
            return clean_or_none(m.group(1)) if m else None

        extra["vacancies_raw"]            = kv("Vacancies")
        extra["education_qualification"]  = kv("Education Qualification")
        extra["applicants_needed"]        = kv("Applicants Needed")
        extra["work_address"]             = kv("Work Address")

        # ── salary ───────────────────────────────────────────────────────────
        sal_m = re.search(r"([\d,]+)\s*ETB(?:\s+(\w+))?", raw_body, re.IGNORECASE)
        if sal_m:
            extra["salary_raw"]    = clean_or_none(sal_m.group(0))
            extra["salary"]        = to_int_or_none(sal_m.group(1))
            # Only keep salary_period if it's a real word (not a random token)
            period = clean_or_none(sal_m.group(2))
            extra["salary_period"] = period if period and len(period) < 20 else None

        # ── experience level ─────────────────────────────────────────────────
        # Match against a known controlled vocabulary; avoid free-text false hits
        exp_m = re.search(
            r"\b(Expert|Senior|Intermediate|Junior|Entry[- ]?Level)\b",
            raw_body, re.IGNORECASE
        )
        extra["experience_level"] = clean_or_none(exp_m.group(1)) if exp_m else None

        # ── skills ───────────────────────────────────────────────────────────
        skills_m = re.search(
            r"Skills And Expertise\s*\n([\s\S]+?)(?:\nWork Address|\nJob Description|\ncompany\b)",
            raw_body, re.IGNORECASE
        )
        if skills_m:
            raw_skills = skills_m.group(1).strip()
            # Skills are rendered as concatenated CamelCase tokens
            tokens = re.findall(r"[A-ZÁÉÍÓÚ][a-záéíóúäöü]+(?:\s+[A-ZÁÉÍÓÚ][a-záéíóúäöü]+)*",
                                raw_skills)
            if tokens:
                extra["skills"] = ", ".join(tokens)
            else:
                # Amharic or all-caps — keep as-is, cleaned
                extra["skills"] = clean_or_none(raw_skills)

        # ── full description ──────────────────────────────────────────────────
        desc_m = re.search(
            r"Job Description\s*\n([\s\S]+?)(?:\nSkills And Expertise|\n+Jobs Posted:|\Z)",
            raw_body, re.IGNORECASE
        )
        if desc_m:
            desc = clean_or_none(desc_m.group(1))
            # Reject descriptions that are just navigation boilerplate (< 30 chars)
            extra["full_description"] = desc if desc and len(desc) >= 30 else None

        # ── company profile URL ───────────────────────────────────────────────
        company_link = page.query_selector("a[href*='/company/']")
        if company_link:
            href = company_link.get_attribute("href") or ""
            if href:
                extra["company_profile_url"] = (
                    BASE_URL + href if href.startswith("/") else href
                )

        # ── company jobs posted ───────────────────────────────────────────────
        jp_raw = parse_or_none(r"Jobs Posted:\s*(\d+)", raw_body)
        extra["company_jobs_posted_raw"] = jp_raw
        extra["company_jobs_posted"]     = to_int_or_none(jp_raw)

        # ── convert vacancies to int ──────────────────────────────────────────
        extra["vacancies"] = to_int_or_none(extra["vacancies_raw"])

        extra["detail_parse_ok"] = True   # everything went OK

    except PWTimeout:
        print(f"  ⚠️  Timeout loading detail page: {url}")
    except Exception as exc:
        print(f"  ⚠️  Error on {url}: {exc}")

    return extra


# ── main scraper ───────────────────────────────────────────────────────────────

def scrape():
    OUTPUT_DIR.mkdir(exist_ok=True)
    all_jobs: list[dict] = []
    seen: set[tuple] = set()   # dedup key: (title_lower, company_lower, deadline_lower)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        )
        list_page   = context.new_page()
        detail_page = context.new_page()

        print(f"🌐  Opening {JOBS_URL} …")
        list_page.goto(JOBS_URL, wait_until="networkidle", timeout=60_000)
        list_page.wait_for_timeout(2000)

        while len(all_jobs) < MAX_JOBS:
            # ── collect job card links currently visible ──────────────────
            cards = list_page.query_selector_all("a[href^='/jobs/']")
            job_links = [
                c for c in cards
                if re.search(r"/jobs/[0-9a-f-]{36}$", c.get_attribute("href") or "")
            ]
            print(f"  Found {len(job_links)} job links visible so far …")

            for card in job_links:
                if len(all_jobs) >= MAX_JOBS:
                    print(f"✅  Reached {MAX_JOBS}-job limit.")
                    break

                href       = card.get_attribute("href") or ""
                detail_url = BASE_URL + href if href.startswith("/") else href
                card_text  = card.inner_text() or ""

                # ── listing-level fields from the card ────────────────────
                lines   = [l.strip() for l in card_text.splitlines() if l.strip()]
                title   = clean_or_none(lines[0]) if lines else None
                company = clean_or_none(lines[1]) if len(lines) > 1 else None

                deadline = parse_or_none(
                    rf"({MONTHS}[a-z]*\s+\d{{1,2}},\s+\d{{4}})", card_text
                )
                posted_relative = parse_or_none(
                    r"Posted\s+(.+?)(?:\n|$)", card_text
                )
                job_type = parse_or_none(
                    r"(Onsite|Remote|Hybrid)(?:\s*[-–]\s*(Full Time|Part Time|Contract|Freelance))?",
                    card_text
                )
                exp_level = parse_or_none(
                    r"\b(Expert|Senior|Intermediate|Junior|Entry[- ]?Level)\b", card_text
                )
                location = parse_or_none(
                    r"(Addis Ababa(?:,\s*Ethiopia)?|[A-Z][a-z]+,\s*Ethiopia)", card_text
                )

                # ── dedup ─────────────────────────────────────────────────
                key = (
                    (title   or "").lower(),
                    (company or "").lower(),
                    (deadline or "").lower(),
                )
                if key in seen:
                    print(f"  ⏭️  Duplicate skipped: {title or '(no title)':.40}")
                    continue
                seen.add(key)

                print(f"  [{len(all_jobs)+1}/{MAX_JOBS}] {title or '(no title)':.50} …")

                # ── detail page ───────────────────────────────────────────
                detail = parse_detail_page(detail_page, detail_url)

                job = {
                    # Core identity
                    "title":                   detail["title"] or title,
                    "company":                 company,
                    "location":                location,
                    "industry":                detail["industry"],
                    # Dates
                    "posted_relative":         posted_relative,
                    "posted_date":             detail["posted_date"],
                    "deadline":                deadline,
                    # Role details
                    "job_type":                job_type,
                    "experience_level":        detail["experience_level"] or exp_level,
                    "vacancies":               detail["vacancies"],          # Int64
                    "education_qualification": detail["education_qualification"],
                    "applicants_needed":       detail["applicants_needed"],
                    # Compensation
                    "salary":                  detail["salary"],             # Int64 ETB
                    "salary_period":           detail["salary_period"],
                    "salary_raw":              detail["salary_raw"],
                    # Skills & description
                    "skills":                  detail["skills"],
                    "work_address":            detail["work_address"],
                    "full_description":        detail["full_description"],
                    # Company
                    "company_profile_url":     detail["company_profile_url"],
                    "company_jobs_posted":     detail["company_jobs_posted"], # Int64
                    # Meta
                    "detail_url":              detail_url,
                    "detail_parse_ok":         detail["detail_parse_ok"],
                    "scraped_at":              datetime.utcnow().isoformat(),
                }
                all_jobs.append(job)
                time.sleep(0.5)

            if len(all_jobs) >= MAX_JOBS:
                break

            # ── load more ─────────────────────────────────────────────────
            load_more = list_page.query_selector(
                "button:has-text('Load More'), a:has-text('Load More')"
            )
            if not load_more:
                print("  ℹ️  No 'Load More' button — all listings exhausted.")
                break
            print("  🔄  Clicking 'Load More' …")
            load_more.scroll_into_view_if_needed()
            load_more.click()
            list_page.wait_for_timeout(3000)

        browser.close()

    # ── build DataFrame ────────────────────────────────────────────────────────
    df = pd.DataFrame(all_jobs)

    # Cast nullable integer columns — keeps NaN as pd.NA, not NaN-as-float
    for col in ("salary", "vacancies", "company_jobs_posted"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Final dedup safety net
    before = len(df)
    df = df.drop_duplicates(subset=["title", "company", "deadline"])
    dropped = before - len(df)
    if dropped:
        print(f"  🧹  Final dedup removed {dropped} row(s).")

    # ── null audit ────────────────────────────────────────────────────────────
    print("\n📊  Null % per column:")
    null_pct = (df.isna().sum() / len(df) * 100).round(1)
    for col, pct in null_pct.items():
        flag = "  ⚠️" if pct > 50 else ""
        print(f"     {col:<30} {pct:>5}%{flag}")

    # ── save ──────────────────────────────────────────────────────────────────
    df.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")
    print(f"\n✅  Saved {len(df)} jobs → {OUTPUT_FILE}")
    print(df[["title", "company", "deadline", "salary"]].to_string(index=False))


if __name__ == "__main__":
    scrape()
