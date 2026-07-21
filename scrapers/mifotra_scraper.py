"""
MIFOTRA Scraper - Robust Version
=================================
Gets basic job info + detailed qualifications by clicking each job.

Why this rewrite: the previous version used one large regex that required an
exact line order (title/company/APPLY/Level/Posts/contract/Posted on/date/
Deadline/date). The real page renders "Posted on" and "Deadline" as
side-by-side column HEADERS with both dates underneath, injects countdown
timer lines (08 / Days / 07 / Hours ...), and sometimes puts "Level:0.I
Posts:4" on a single line — so the regex matched nothing and the scraper
always returned 0 jobs.

This version anchors on the "Level:" line (exactly one per job card) and
searches nearby lines for each field, so it tolerates reordering and extra
lines. Dates are taken as: earlier = posted, later = deadline.

Env vars:
  MIFOTRA_SKIP_DETAILS=1   skip the slow per-job popup clicking (faster CI)
"""

import os
import re
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Dict

import pandas as pd
from schema import enforce

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://recruitment.mifotra.gov.rw"

DATE_RE = re.compile(r'[A-Z][a-z]{2,8} \d{1,2}, \d{4}')          # "Jul 29, 2026" / "July 29, 2026"
ACRONYM_RE = re.compile(r'\(([A-Z][A-Z0-9\-\. ]{1,20})\)\s*$')   # "... (RUTSIRO)" / "... (CHUK)"
LEVEL_RE = re.compile(r'^Level\s*:', re.IGNORECASE)
POSTS_RE = re.compile(r'Posts?\s*:\s*(\d+)', re.IGNORECASE)
NOISE_LINES = {
    'APPLY', 'Days', 'Hours', 'Mins', 'Minutes', 'Seconds', 'Secs',
    'Posted on', 'Deadline', 'Under Contract', 'Under Statute',
}


class MifotraScraper:

    def __init__(self, headless=True):
        self.headless = headless
        self.SOURCE = "mifotra"

    # ------------------------------------------------------------------ #
    # Selenium driver
    # ------------------------------------------------------------------ #
    def _start_driver(self):
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service

        options = Options()
        if self.headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1400,2400')

        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            return webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logger.warning(f"webdriver_manager failed ({e}), trying system chromedriver...")
            try:
                return webdriver.Chrome(options=options)
            except Exception as e2:
                logger.error(f"Could not start Chrome: {e2}")
                logger.error("MIFOTRA requires Chrome/Chromium installed. Skipping.")
                return None

    def _wait_for_jobs(self, driver, timeout=45):
        """Wait until the Angular app has actually rendered job cards."""
        from selenium.webdriver.common.by import By
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                body = driver.find_element(By.TAG_NAME, "body").text
                if 'Level:' in body and DATE_RE.search(body):
                    return body
            except Exception:
                pass
            time.sleep(1.5)
        # Return whatever we have; parser will just find nothing
        try:
            return driver.find_element(By.TAG_NAME, "body").text
        except Exception:
            return ""

    # ------------------------------------------------------------------ #
    # Main entry point
    # ------------------------------------------------------------------ #
    def scrape(self) -> pd.DataFrame:
        logger.info("=" * 60)
        logger.info("MIFOTRA Scraper - Robust Version")
        logger.info("=" * 60)

        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys

        driver = self._start_driver()
        if driver is None:
            return pd.DataFrame()

        jobs = []
        try:
            logger.info(f"Loading: {BASE_URL}")
            driver.get(BASE_URL)
            page_text = self._wait_for_jobs(driver)

            basic_jobs = self._parse_jobs(page_text)
            logger.info(f"Found {len(basic_jobs)} jobs")

            skip_details = os.getenv("MIFOTRA_SKIP_DETAILS", "0") == "1"
            if skip_details:
                logger.info("MIFOTRA_SKIP_DETAILS=1 -> skipping popup details")
                jobs = basic_jobs
            else:
                logger.info("Fetching detailed information...")
                for i, job in enumerate(basic_jobs, 1):
                    logger.info(f"  [{i}/{len(basic_jobs)}] {job['title'][:50]}...")
                    try:
                        clicked = False
                        for link in driver.find_elements(By.TAG_NAME, "a"):
                            link_text = (link.text or "").strip()
                            if link_text and job['title'] in link_text:
                                driver.execute_script("arguments[0].scrollIntoView(true);", link)
                                time.sleep(0.5)
                                link.click()
                                clicked = True
                                time.sleep(2)
                                break
                        if clicked:
                            popup_text = driver.find_element(By.TAG_NAME, "body").text
                            job.update(self._extract_details(popup_text))
                            driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
                            time.sleep(0.5)
                    except Exception as e:
                        logger.warning(f"    Could not fetch details: {e}")
                    jobs.append(job)

        except Exception as e:
            logger.error(f"Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            driver.quit()

        if not jobs:
            logger.warning("No MIFOTRA jobs parsed — page layout may have changed again.")
            return pd.DataFrame()

        records = []
        for job in jobs:
            records.append({
                'id': hashlib.md5(f"{self.SOURCE}_{job['title']}_{job['company']}".encode()).hexdigest(),
                'title': job['title'],
                'company': job['company'],
                'description': job.get('description', ''),
                'responsibilities': job.get('responsibilities', ''),
                'qualifications': job.get('qualifications', ''),
                'experience_required': job.get('experience', ''),
                'experience_years': job.get('experience', ''),   # <- what db_adapter actually reads
                'education_level': job.get('education', ''),
                'location_raw': job.get('location_raw', ''),
                'district': job.get('district', 'Kigali'),
                'country': 'Rwanda',
                'is_remote': False,
                'rwanda_eligible': True,
                'eligibility_reason': 'Rwanda Government Job',
                'confidence_score': 5,
                'sector': 'Government',
                'employment_type': job.get('contract_type', 'Contract'),
                'posted_date': job.get('posted_date', ''),
                'deadline': job.get('deadline', ''),
                'scraped_at': datetime.now(timezone.utc).isoformat(),
                'source': self.SOURCE,
                'source_url': BASE_URL,
                'source_job_id': job.get('level', ''),
                'is_active': True,
                'last_checked': datetime.now(timezone.utc).isoformat(),
                'duplicate_hash': hashlib.md5(f"{job['title']}{job['company']}".encode()).hexdigest(),
            })

        df = pd.DataFrame(records)
        df = enforce(df)

        logger.info(f"Success - {len(df)} complete jobs!")
        logger.info("=" * 60)
        return df

    # ------------------------------------------------------------------ #
    # Parsing — anchor on "Level:" lines instead of one rigid regex
    # ------------------------------------------------------------------ #
    def _parse_jobs(self, text: str) -> List[Dict]:
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        jobs, seen = [], set()

        for i, line in enumerate(lines):
            if not LEVEL_RE.search(line):
                continue

            # --- company: nearest previous line ending in (ACRONYM) ---
            company, company_idx = None, None
            for j in range(i - 1, max(i - 7, -1), -1):
                if ACRONYM_RE.search(lines[j]):
                    company, company_idx = lines[j], j
                    break
            if company is None:
                continue

            # --- title: nearest meaningful line above the company ---
            title = None
            for k in range(company_idx - 1, max(company_idx - 5, -1), -1):
                cand = lines[k]
                if (cand in NOISE_LINES or DATE_RE.fullmatch(cand) or
                        LEVEL_RE.search(cand) or POSTS_RE.fullmatch(cand) or
                        cand.isdigit() or len(cand) < 3):
                    continue
                title = cand
                break
            if title is None:
                continue

            key = (title, company)
            if key in seen:
                continue
            seen.add(key)

            # --- forward window: level/posts, contract type, dates ---
            window = lines[i:i + 16]
            wtext = '\n'.join(window)

            level_match = re.search(r'Level\s*:\s*([^\s]+)', line, re.IGNORECASE)
            posts_match = POSTS_RE.search('\n'.join(lines[i:i + 3]))
            level_str = level_match.group(1) if level_match else ''
            posts_str = f"Posts:{posts_match.group(1)}" if posts_match else ''

            if 'Under Statute' in wtext:
                contract_type = 'Permanent'
            else:
                contract_type = 'Contract'

            # Dates: order-independent. Earlier = posted, later = deadline.
            raw_dates = DATE_RE.findall(wtext)[:2]
            parsed_dates = sorted(d for d in (self._parse_date(x) for x in raw_dates) if d)
            posted_date = parsed_dates[0] if parsed_dates else ''
            deadline = parsed_dates[-1] if parsed_dates else ''

            # Location: the company line usually names the district/institution
            district = 'Kigali'
            m = ACRONYM_RE.search(company)
            base_name = company[:m.start()].strip() if m else company
            dm = re.match(r'([A-Z][a-z]+)\s+District', base_name)
            if dm:
                district = dm.group(1)

            jobs.append({
                'title': title,
                'company': company,
                'level': f"{('Level:' + level_str) if level_str else ''} {posts_str}".strip(),
                'contract_type': contract_type,
                'posted_date': posted_date,
                'deadline': deadline,
                'district': district,
                'location_raw': base_name,
            })

        return jobs

    # ------------------------------------------------------------------ #
    # Popup details (unchanged logic, tolerant)
    # ------------------------------------------------------------------ #
    def _extract_details(self, popup_text: str) -> Dict:
        details = {}

        if "Job responsibilities" in popup_text:
            resp_start = popup_text.find("Job responsibilities")
            resp_end = popup_text.find("Qualifications", resp_start)
            if resp_end > resp_start:
                resp = popup_text[resp_start + len("Job responsibilities"):resp_end]
                resp = resp.replace("Duties and Responsibilities:", "").strip()
                details['description'] = resp[:500]
                details['responsibilities'] = resp[:1000]

        if "Qualifications" in popup_text:
            qual_start = popup_text.find("Qualifications")
            qual_text = popup_text[qual_start + len("Qualifications"):qual_start + 1500]

            qual_lines = []
            for line in qual_text.split('\n')[:15]:
                line = line.strip()
                if line and (line[0].isdigit() or 'degree' in line.lower()
                             or 'bachelor' in line.lower() or 'master' in line.lower()):
                    qual_lines.append(line)
            details['qualifications'] = ' | '.join(qual_lines)

            qual_lower = qual_text.lower()
            if 'phd' in qual_lower or 'doctorate' in qual_lower:
                details['education'] = 'PhD'
            elif 'master' in qual_lower:
                details['education'] = 'Master'
            elif 'bachelor' in qual_lower:
                details['education'] = 'Bachelor'

            exp_match = re.search(r'(\d+)\s*years?\s*of\s*(?:relevant\s*)?experience', qual_lower)
            if exp_match:
                details['experience'] = f"{exp_match.group(1)} years"

        return details

    def _parse_date(self, text: str) -> str:
        """Parse 'Jul 29, 2026' or 'July 29, 2026' -> '2026-07-29'"""
        months = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        match = re.search(r'([A-Z][a-z]{2})[a-z]* (\d{1,2}), (\d{4})', text)
        if match:
            month_str, day, year = match.groups()
            month = months.get(month_str.lower())
            if month:
                return f"{year}-{month}-{day.zfill(2)}"
        return ""


def main():
    print("\n" + "=" * 60)
    print("MIFOTRA Scraper - Robust Version")
    print("=" * 60 + "\n")

    scraper = MifotraScraper(headless=True)
    df = scraper.scrape()

    if not df.empty:
        print(f"\n[OK] SUCCESS! Scraped {len(df)} jobs!\n")
        cols = [c for c in ['title', 'company', 'district', 'employment_type', 'posted_date', 'deadline'] if c in df.columns]
        print(df[cols].to_string(index=False))
    else:
        print("\n[ERROR] No jobs found")


if __name__ == "__main__":
    main()
