"""
Afriworket Job Scraper
======================
- Scrapes up to 100 job listings from afriworket.com/jobs
- Visits each job detail page for full data
- Deduplicates on (title, company, deadline)
- Saves results to data/jobs.parquet

CSS selectors confirmed from browser DevTools:
  List page card:
    title    : <a href="/jobs/UUID"> inner text
    company  : span.font-medium.text-gray-500 (inside card container)
    location : span.text-stone-500 (inside card container)
    deadline : span:has-text("Deadline:") -> sibling p.whitespace-nowrap
    job_type : text matching Onsite/Remote/Hybrid - Full/Part Time

  Detail page:
    title    : h1
    company  : span.flex.items-center.gap-1.text-sm.font-medium.text-gray-500
    location : span.text-sm.font-normal.text-stone-500
    industry : first h2
    deadline : JS eval — span[text='Deadline:'] -> parent div -> next p
"""

import re
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_URL    = "https://afriworket.com"
JOBS_URL    = f"{BASE_URL}/jobs"
MAX_JOBS    = 30
OUTPUT_DIR  = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "jobs.parquet"

MONTHS = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"


# ── helpers ────────────────────────────────────────────────────────────────────

def clean_or_none(text):
    """Normalise whitespace. Returns None for blank strings."""
    if text is None:
        return None
    normalised = re.sub(r"\s+", " ", text).strip()
    return normalised if normalised else None


def el_text(el, selector):
    """Query selector within an element, return inner text or None."""
    child = el.query_selector(selector)
    return clean_or_none(child.inner_text()) if child else None


def parse_or_none(pattern, text, group=1, flags=re.IGNORECASE):
    """Regex match returning named group or None."""
    if not text:
        return None
    m = re.search(pattern, text, flags)
    if not m:
        return None
    return clean_or_none(m.group(group))


def to_int_or_none(value):
    """Strip non-digits and parse to int, or None."""
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


def extract_company_from_span(el, selector):
    """
    Get company name from a span that contains both text and an SVG icon.
    SVG aria-hidden icons contribute empty/garbage to inner_text() —
    we use JS to read only direct text nodes.
    """
    result = el.evaluate(f"""(root) => {{
        const span = root.querySelector('{selector}');
        if (!span) return null;
        // Collect only direct text node content (skip SVG children)
        let text = '';
        for (const node of span.childNodes) {{
            if (node.nodeType === Node.TEXT_NODE) {{
                text += node.textContent;
            }}
        }}
        return text.trim() || null;
    }}""")
    return clean_or_none(result)


def extract_deadline_from_card(card_el):
    """
    Find deadline in a card element using JS DOM traversal.
    Structure: <span>Deadline:</span> -> parent -> sibling <p class="whitespace-nowrap ...">
    """
    result = card_el.evaluate("""(root) => {
        // Find the Deadline label span
        const spans = Array.from(root.querySelectorAll('span'));
        const label = spans.find(s => s.textContent.trim() === 'Deadline:');
        if (!label) return null;

        // The value <p> is a sibling of the label's parent div
        const labelParent = label.parentElement;
        if (!labelParent) return null;

        // Try next sibling first
        let sib = labelParent.nextElementSibling;
        while (sib) {
            const p = sib.querySelector ? sib.querySelector('p.whitespace-nowrap, p') : null;
            if (p && p.textContent.trim()) return p.textContent.trim();
            if (sib.tagName === 'P' && sib.textContent.trim()) return sib.textContent.trim();
            sib = sib.nextElementSibling;
        }

        // Try parent's parent sibling
        const grandParent = labelParent.parentElement;
        if (grandParent) {
            const next = grandParent.nextElementSibling;
            if (next) {
                const p = next.querySelector('p.whitespace-nowrap, p');
                if (p) return p.textContent.trim();
            }
        }
        return null;
    }""")
    return clean_or_none(result)


# ── card parser (listing page) ─────────────────────────────────────────────────

def parse_card(anchor):
    """
    Extract all available fields from a job card on the listing page.
    The anchor <a> is the title link. All other fields are in parent/sibling elements.
    We walk up to the card root element and query by CSS class.
    """
    title = clean_or_none(anchor.inner_text())
    href  = anchor.get_attribute("href") or ""

    # Walk up to find card container — stop when we find the element
    # that contains company + location + deadline info
    card_el = anchor
    for _ in range(6):
        parent = card_el.evaluate_handle("el => el.parentElement")
        if not parent:
            break
        parent_el = parent.as_element()
        if not parent_el:
            break
        # Check if this level has company span or deadline span
        has_company  = parent_el.query_selector("span.font-medium, span.text-gray-500")
        has_deadline = parent_el.query_selector("span")
        if has_company or has_deadline:
            text_check = parent_el.inner_text() or ""
            if re.search(r"Deadline|Onsite|Remote|Hybrid|Posted", text_check, re.IGNORECASE):
                card_el = parent_el
                break
        card_el = parent_el

    card_text = card_el.inner_text() or ""

    # Company — span with font-medium text-gray-500 classes (contains SVG icon + company name)
    # Use JS text-node extraction to avoid SVG pollution
    company = extract_company_from_span(
        card_el,
        "span.font-medium.text-gray-500, span.text-sm.font-medium.text-gray-500"
    )

    # Location — span with text-stone-500
    location = el_text(card_el, "span.text-stone-500, span.text-sm.text-stone-500")

    # Deadline — DOM traversal via JS
    deadline = extract_deadline_from_card(card_el)
    # Fallback: regex on card text
    if not deadline:
        deadline = parse_or_none(
            rf"({MONTHS}[a-z]*\.?\s+\d{{1,2}},?\s+\d{{4}})", card_text
        )

    # Posted relative time
    posted_relative = parse_or_none(r"Posted\s+(.+?)(?:\n|$)", card_text)

    # Job type
    job_type = parse_or_none(
        r"((?:Onsite|Remote|Hybrid)\s*[-–]\s*(?:Full Time|Part Time|Contract|Freelance))",
        card_text
    )

    # Experience level
    exp_level = parse_or_none(
        r"\b(Expert|Senior|Intermediate|Junior|Entry[- ]?Level)\b", card_text
    )

    return {
        "title":           title,
        "company":         company,
        "location":        location,
        "deadline":        deadline,
        "posted_relative": posted_relative,
        "job_type":        job_type,
        "exp_level":       exp_level,
        "detail_url":      BASE_URL + href if href.startswith("/") else href,
    }


# ── detail page parser ─────────────────────────────────────────────────────────

def parse_detail_page(page, url):
    result = {
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
        "detail_parse_ok":         False,
    }

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(1500)

        h1 = page.query_selector("h1")
        if not h1:
            print(f"  WARNING: No h1 on {url}")
            return result

        # title
        result["title"] = clean_or_none(h1.inner_text())

        # company — span with exact Tailwind classes seen in DevTools
        # Use JS text-node extraction to avoid SVG aria-label pollution
        result["company"] = extract_company_from_span(
            page,
            "span.flex.items-center.gap-1.text-sm.font-medium.text-gray-500"
        )

        # location
        result["location"] = el_text(page, "span.text-sm.font-normal.text-stone-500")

        # industry — first h2
        h2s = page.query_selector_all("h2")
        if h2s:
            result["industry"] = clean_or_none(h2s[0].inner_text())

        # deadline — JS DOM traversal (same structure as card)
        result["deadline"] = page.evaluate("""() => {
            const spans = Array.from(document.querySelectorAll('span'));
            const label = spans.find(s => s.textContent.trim() === 'Deadline:');
            if (!label) return null;
            const labelParent = label.parentElement;
            if (!labelParent) return null;
            // Sibling <p> with the date
            let sib = labelParent.nextElementSibling;
            while (sib) {
                if (sib.tagName === 'P' && sib.textContent.trim()) {
                    return sib.textContent.trim();
                }
                const p = sib.querySelector && sib.querySelector('p.whitespace-nowrap, p');
                if (p && p.textContent.trim()) return p.textContent.trim();
                sib = sib.nextElementSibling;
            }
            // Grandparent sibling
            const gp = labelParent.parentElement;
            if (gp) {
                const next = gp.nextElementSibling;
                if (next) {
                    const p = next.querySelector('p.whitespace-nowrap, p');
                    if (p) return p.textContent.trim();
                }
            }
            return null;
        }""")
        result["deadline"] = clean_or_none(result["deadline"])

        raw_body = page.inner_text("body") or ""

        # Fallback deadline from body text
        if not result["deadline"]:
            result["deadline"] = parse_or_none(
                rf"Deadline[:\s]+({MONTHS}[a-z]*\.?\s+\d{{1,2}},?\s+\d{{4}})",
                raw_body
            )

        # posted date
        result["posted_date"] = parse_or_none(
            rf"Posted\s+({MONTHS}[a-z]*\.?\s+\d{{1,2}},?\s+\d{{4}})",
            raw_body
        )

        # job type
        result["job_type"] = parse_or_none(
            r"Job Type[:\s]*((?:Onsite|Remote|Hybrid)\s*[-–]\s*(?:Full Time|Part Time|Contract|Freelance))",
            raw_body
        )
        if not result["job_type"]:
            result["job_type"] = parse_or_none(
                r"((?:Onsite|Remote|Hybrid)\s*[-–]\s*(?:Full Time|Part Time|Contract|Freelance))",
                raw_body
            )

        # key-value fields
        def kv(label):
            v = parse_or_none(
                rf"^{re.escape(label)}\s*[:\-]?\s*(.+)",
                raw_body, flags=re.IGNORECASE | re.MULTILINE
            )
            if v:
                return v
            m = re.search(
                rf"^{re.escape(label)}\s*$\s*^(.+)",
                raw_body, flags=re.IGNORECASE | re.MULTILINE
            )
            return clean_or_none(m.group(1)) if m else None

        result["vacancies_raw"]            = kv("Vacancies")
        result["education_qualification"]  = kv("Education Qualification")
        result["applicants_needed"]        = kv("Applicants Needed")
        result["work_address"]             = kv("Work Address")

        # salary
        sal_m = re.search(r"([\d,]+)\s*ETB(?:\s+(\w+))?", raw_body, re.IGNORECASE)
        if sal_m:
            result["salary_raw"]    = clean_or_none(sal_m.group(0))
            result["salary"]        = to_int_or_none(sal_m.group(1))
            period = clean_or_none(sal_m.group(2))
            result["salary_period"] = period if period and len(period) < 20 else None

        # experience level
        exp_m = re.search(
            r"\b(Expert|Senior|Intermediate|Junior|Entry[- ]?Level)\b",
            raw_body, re.IGNORECASE
        )
        result["experience_level"] = clean_or_none(exp_m.group(1)) if exp_m else None

        # skills
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
            result["skills"] = ", ".join(tokens) if tokens else clean_or_none(raw_skills)

        # full description
        desc_m = re.search(
            r"Job Description\s*\n([\s\S]+?)(?:\nSkills And Expertise|\n+Jobs Posted:|\Z)",
            raw_body, re.IGNORECASE
        )
        if desc_m:
            desc = clean_or_none(desc_m.group(1))
            result["full_description"] = desc if desc and len(desc) >= 30 else None

        # company profile URL
        company_link = page.query_selector("a[href*='/company/']")
        if company_link:
            href = company_link.get_attribute("href") or ""
            if href:
                result["company_profile_url"] = (
                    BASE_URL + href if href.startswith("/") else href
                )

        # company jobs posted
        jp_raw = parse_or_none(r"Jobs Posted:\s*(\d+)", raw_body)
        result["company_jobs_posted_raw"] = jp_raw
        result["company_jobs_posted"]     = to_int_or_none(jp_raw)

        result["vacancies"] = to_int_or_none(result["vacancies_raw"])

        result["detail_parse_ok"] = True

    except PWTimeout:
        print(f"  WARNING: Timeout on {url}")
    except Exception as exc:
        print(f"  WARNING: Error on {url}: {exc}")

    return result


# ── main scraper ───────────────────────────────────────────────────────────────

def scrape():
    OUTPUT_DIR.mkdir(exist_ok=True)
    all_jobs = []
    seen = set()

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

        print(f"Opening {JOBS_URL} ...")
        list_page.goto(JOBS_URL, wait_until="networkidle", timeout=60_000)
        list_page.wait_for_timeout(2000)

        while len(all_jobs) < MAX_JOBS:
            anchors = list_page.query_selector_all("a[href^='/jobs/']")
            job_anchors = [
                a for a in anchors
                if re.search(r"/jobs/[0-9a-f-]{36}$", a.get_attribute("href") or "")
            ]
            print(f"  Found {len(job_anchors)} job links visible so far ...")

            for anchor in job_anchors:
                if len(all_jobs) >= MAX_JOBS:
                    print(f"Reached {MAX_JOBS}-job limit.")
                    break

                card = parse_card(anchor)
                title      = card["title"]
                detail_url = card["detail_url"]

                # Dedup on title + deadline before hitting the detail page
                key = ((title or "").lower(), (card["deadline"] or "").lower())
                if key in seen:
                    print(f"  Duplicate skipped: {title or '(no title)'}")
                    continue
                seen.add(key)

                print(f"  [{len(all_jobs)+1}/{MAX_JOBS}] {(title or '(no title)')[:55]} ...")

                detail = parse_detail_page(detail_page, detail_url)

                job = {
                    # Detail page is authoritative; card values are fallback
                    "title":                   detail["title"] or title,
                    "company":                 detail["company"] or card["company"],
                    "location":                detail["location"] or card["location"],
                    "industry":                detail["industry"],
                    "posted_relative":         card["posted_relative"],
                    "posted_date":             detail["posted_date"],
                    "deadline":                detail["deadline"] or card["deadline"],
                    "job_type":                detail["job_type"] or card["job_type"],
                    "experience_level":        detail["experience_level"] or card["exp_level"],
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
                }
                all_jobs.append(job)
                time.sleep(0.5)

            if len(all_jobs) >= MAX_JOBS:
                break

            load_more = list_page.query_selector(
                "button:has-text('Load More'), a:has-text('Load More')"
            )
            if not load_more:
                print("  No 'Load More' button — all listings exhausted.")
                break
            print("  Clicking 'Load More' ...")
            load_more.scroll_into_view_if_needed()
            load_more.click()
            list_page.wait_for_timeout(3000)

        browser.close()

    # ── build DataFrame ────────────────────────────────────────────────────────
    df = pd.DataFrame(all_jobs)

    for col in ("salary", "vacancies", "company_jobs_posted"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    before = len(df)
    df = df.drop_duplicates(subset=["title", "company", "deadline"])
    dropped = before - len(df)

    # scraped_at always last
    cols = [c for c in df.columns if c != "scraped_at"] + ["scraped_at"]
    df = df[cols]

    # Null audit
    print("\nNull % per column:")
    null_pct = (df.isna().sum() / len(df) * 100).round(1)
    for col, pct in null_pct.items():
        flag = "  <-- WARNING" if pct > 50 else ""
        print(f"  {col:<30} {pct:>5}%{flag}")

    df.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")
    print(f"\nSaved {len(df)} jobs to {OUTPUT_FILE}")

if __name__ == "__main__":
    scrape()
