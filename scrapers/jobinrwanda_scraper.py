"""
Job Scraper for jobinrwanda.com
Outputs a rich, structured schema for each job listing.
Features: concurrent requests, deduplication, Rwanda eligibility inference.
"""

import re
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------------------------------
# Logging
# ----------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("job_scraper.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ----------------------------------------------
# Constants / lookup tables
# ----------------------------------------------

RWANDA_DISTRICTS = [
    "kigali", "gasabo", "kicukiro", "nyarugenge",
    "musanze", "rubavu", "rusizi", "huye", "rwamagana",
    "muhanga", "karongi", "nyamasheke", "nyagatare",
    "gatsibo", "kayonza", "kirehe", "ngoma", "bugesera",
    "nyanza", "gisagara", "nyaruguru", "ruhango",
    "kamonyi", "gakenke", "rulindo", "gicumbi", "burera",
    "ngororero", "nyabihu", "rutsiro", "rubirizi",
]

SECTOR_KEYWORDS = {
    # -- Specific / unambiguous terms first ------------------------------
    "Health": [
        "nurse", "doctor", "physician", "surgeon", "pharmacist", "midwife",
        "clinical", "epidemiolog", "laboratory", "radiology", "dentist",
        "public health", "hospital", "nutrition", "dietitian", "health worker",
        "community health", "hiv", "malaria", "tuberculosis",
    ],
    "Finance": [
        "accountant", "accounting", "auditor", "audit", "tax consultant",
        "financial analyst", "chief financial", "cfo", "treasurer",
        "budget analyst", "microfinance", "investment analyst",
        "credit analyst", "actuary", "bookkeeper",
    ],
    "Agriculture": [
        "agronomist", "agronomy", "horticulture", "livestock", "veterinary",
        "crop", "soil", "irrigation", "aquaculture", "agri-business",
        "agricultural extension", "rural development", "food security",
    ],
    "Education": [
        "teacher", "lecturer", "professor", "headmaster", "principal",
        "curriculum developer", "academic", "school", "university",
        "early childhood", "pedagog", "education officer",
    ],
    "Construction": [
        "architect", "structural engineer", "quantity surveyor",
        "site engineer", "construction manager", "civil engineer",
        "urban planner", "land surveyor", "building inspector",
    ],
    "Logistics": [
        "supply chain", "logistics officer", "warehouse", "procurement officer",
        "fleet manager", "customs officer", "freight", "import/export",
        "inventory manager", "distribution",
    ],
    "HR": [
        "human resource", "hr officer", "hr manager", "recruiter",
        "talent acquisition", "payroll officer", "people operations",
        "organizational development", "hr business partner",
    ],
    "Marketing": [
        "marketing officer", "brand manager", "communications officer",
        "digital marketer", "social media manager", "content creator",
        "public relations", "copywriter", "media officer", "advertising",
    ],
    "NGO": [
        "ngo", "ingo", "unicef", "undp", "usaid", "world bank", "oxfam",
        "save the children", "care international", "irc", "mercy corps",
        "msf", "humanitarian", "donor relations", "grant", "m&e officer",
        "monitoring and evaluation",
    ],
    # -- IT last and with precise terms only -----------------------------
    "IT": [
        "software engineer", "software developer", "web developer",
        "mobile developer", "frontend developer", "backend developer",
        "fullstack developer", "data engineer", "data scientist",
        "machine learning", "devops engineer", "cloud engineer",
        "network engineer", "cybersecurity", "database administrator",
        "systems administrator", "it support", "it officer",
        "it manager", "ict officer", "programmer", "ui/ux designer",
    ],
}

JOB_LEVEL_KEYWORDS = {
    "Internship": ["intern", "attachment", "apprentice"],
    "Entry": ["entry", "junior", "fresh", "graduate", "trainee", "0-1 year", "1 year"],
    "Mid": ["mid", "2-3 year", "3-5 year", "2 years", "3 years", "4 years", "5 years"],
    "Senior": ["senior", "lead", "principal", "expert", "6+ year", "7+ year",
               "8+ year", "10+ year", "manager", "head of", "director"],
    "Executive": ["ceo", "cfo", "cto", "coo", "vp ", "vice president",
                  "executive director", "country director", "chief"],
}

EMPLOYMENT_TYPE_KEYWORDS = {
    "Full-time": ["full-time", "full time", "permanent", "indefinite"],
    "Part-time": ["part-time", "part time"],
    "Contract": ["contract", "fixed-term", "fixed term", "temporary"],
    "Internship": ["intern", "attachment", "apprentice"],
    "Consultancy": ["consultant", "consultancy", "freelance"],
    "Volunteer": ["volunteer", "voluntary"],
}

EDUCATION_KEYWORDS = {
    "PhD": ["phd", "doctorate", "doctoral"],
    "Master's": ["master", "msc", "mba", "ma ", "m.sc", "m.a."],
    "Bachelor's": ["bachelor", "bsc", "ba ", "b.sc", "b.a.", "degree", "undergraduate", "university"],
    "Diploma": ["diploma", "a-level", "advanced level", "hnd"],
    "Certificate": ["certificate", "o-level", "tvet", "vocational"],
}

CURRENCY_SYMBOLS = {
    "RWF": ["rwf", "frw", "francs rwandais"],
    "USD": ["usd", "$", "dollars"],
    "EUR": ["eur", "EUR", "euros"],
}


# ----------------------------------------------
# Helper functions
# ----------------------------------------------

def clean(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    return " ".join(text.split()).strip() or None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def extract_regex(pattern: str, text: str, group: int = 1) -> Optional[str]:
    if not text:
        return None
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(group).strip() if m else None


def infer_field(text: str, keyword_map: Dict[str, List[str]],
                title: str = "") -> Optional[str]:
    """
    Return the best matching category.
    Strategy:
      1. Try matching keywords against the job title alone (more reliable signal).
      2. Fall back to matching against the full text.
    Longer keyword phrases are preferred to avoid false positives.
    """
    def best_match(haystack: str) -> Optional[str]:
        h = haystack.lower()
        best_cat, best_len = None, 0
        for category, keywords in keyword_map.items():
            for kw in sorted(keywords, key=len, reverse=True):
                if kw in h and len(kw) > best_len:
                    best_cat, best_len = category, len(kw)
                    break
        return best_cat

    if title:
        result = best_match(title)
        if result:
            return result

    return best_match(text) if text else None


def extract_experience_years(text: str) -> str:
    """
    Extract experience requirement from free text.
    Returns normalized strings like '2', '3-5', '5+', '0', or ''.

    Examples:
      'at least 3 years'       -> '3+'
      'minimum 2 years'        -> '2+'
      '3 to 5 years'           -> '3-5'
      '3 - 5 years experience' -> '3-5'
      '2+ years'               -> '2+'
      'no experience required' -> '0'
    """
    if not text:
        return ""
    t = text.lower()

    if re.search(r"no\s+(prior\s+)?experience", t):
        return "0"

    # Range: "3 to 5 years" / "3 - 5 years" / "3-5 years"
    m = re.search(r"(\d+)\s*(?:to|-|-|-)\s*(\d+)\s*(?:years?|yrs?)", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    # "X+ years"
    m = re.search(r"(\d+)\s*\+\s*(?:years?|yrs?)", t)
    if m:
        return f"{m.group(1)}+"

    # "at least / minimum / over X years"
    m = re.search(r"(?:at\s+least|minimum|over|more\s+than)\s+(\d+)\s*(?:years?|yrs?)", t)
    if m:
        return f"{m.group(1)}+"

    # Plain "X years"
    m = re.search(r"(\d+)\s*(?:years?|yrs?)", t)
    if m:
        return m.group(1)

    return ""


def extract_salary(text: str) -> Dict[str, Any]:
    """
    Extract salary range and currency from free text.
    Only extracts numbers when a salary/currency keyword is present.
    Caps at 9,999,999,999 to fit NUMERIC(12,2) database column.
    """
    result = {"salary_min": None, "salary_max": None,
              "currency": "", "salary_disclosed": False}
    if not text:
        return result

    # Detect currency
    t = text.lower()
    for currency, symbols in CURRENCY_SYMBOLS.items():
        if any(s in t for s in symbols):
            result["currency"] = currency
            break

    # Only extract numbers when a salary keyword is nearby.
    # Prevents phone numbers, job refs, years from triggering salary_disclosed.
    salary_trigger = re.compile(
        r"salary|remuneration|compensation|pay|stipend|rwf|usd|eur|frw",
        re.IGNORECASE
    )
    if not salary_trigger.search(text):
        return result

    MAX_SALARY = 9_999_999_999  # NUMERIC(12,2) upper bound

    m = re.search(r"([\d,\.]+)\s*[-to]+\s*([\d,\.]+)", text.replace(",", ""))
    if m:
        try:
            lo = float(m.group(1))
            hi = float(m.group(2))
            if lo <= MAX_SALARY and hi <= MAX_SALARY:
                result["salary_min"] = lo
                result["salary_max"] = hi
                result["salary_disclosed"] = True
        except ValueError:
            pass
    else:
        single = re.search(r"([\d,\.]{4,})", text.replace(",", ""))
        if single:
            try:
                val = float(single.group(1))
                if val <= MAX_SALARY:
                    result["salary_min"] = val
                    result["salary_disclosed"] = True
            except ValueError:
                pass

    return result


def infer_location(text: str) -> Dict[str, Any]:
    """Parse location string into structured fields."""
    loc: Dict[str, Any] = {
        "location_raw": clean(text) or "",
        "district": "",
        "country": "Rwanda",
        "is_remote": False,
        "is_hybrid": False,
    }
    if not text:
        return loc

    t = text.lower()

    if "remote" in t:
        loc["is_remote"] = True
    if "hybrid" in t:
        loc["is_hybrid"] = True

    # District detection
    for district in RWANDA_DISTRICTS:
        if district in t:
            loc["district"] = district.capitalize()
            break

    # Special case: "kigali" covers Gasabo/Kicukiro/Nyarugenge
    if "kigali" in t and not loc["district"]:
        loc["district"] = "Kigali"

    return loc


def infer_rwanda_eligibility(job: Dict) -> Dict:
    """
    Simple rule-based eligibility checker.
    Returns updated fields: rwanda_eligible, eligibility_reason, confidence_score.
    """
    text = " ".join(filter(None, [
        job.get("title", ""),
        job.get("description", ""),
        job.get("location_raw", ""),
    ])).lower()

    if job.get("is_remote"):
        return {"rwanda_eligible": True,
                "eligibility_reason": "Remote job - open globally",
                "confidence_score": 4}

    # Hard disqualifiers
    disqualifiers = ["us citizens only", "eu residents only",
                     "must be based in", "authorized to work in the us",
                     "work permit required (not rwanda)"]
    for d in disqualifiers:
        if d in text:
            return {"rwanda_eligible": False,
                    "eligibility_reason": f"Disqualifying clause: '{d}'",
                    "confidence_score": 5}

    # Strong Rwanda signals
    rwanda_signals = ["rwanda", "kigali", "rwandan", "based in kigali", "based in rwanda"]
    for sig in rwanda_signals:
        if sig in text:
            return {"rwanda_eligible": True,
                    "eligibility_reason": "Explicitly mentions Rwanda location",
                    "confidence_score": 5}

    # Source is jobinrwanda.com - assume eligible unless flagged
    return {"rwanda_eligible": True,
            "eligibility_reason": "Listed on jobinrwanda.com",
            "confidence_score": 3}


# ----------------------------------------------
# Scraper class
# ----------------------------------------------

class JobScraper:
    BASE_URL = "https://www.jobinrwanda.com"
    SOURCE_NAME = "jobinrwanda"

    def __init__(self, request_delay: float = 0.5,
                 timeout: int = 15,
                 max_workers: int = 8):
        self.request_delay = request_delay
        self.timeout = timeout
        self.max_workers = max_workers
        self.session = self._build_session()

    # ------------------------------------------------------------------
    # Network helpers
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.8,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        })
        return session

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        time.sleep(self.request_delay)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except requests.RequestException as e:
            logger.warning(f"GET failed [{url}]: {e}")
            return None

    # ------------------------------------------------------------------
    # Category / pagination
    # ------------------------------------------------------------------

    def _get_category_links(self) -> List[str]:
        soup = self._get(self.BASE_URL)
        if not soup:
            return []
        links = []
        for a in soup.select('a[class*="nav-link--jobs-"]'):
            href = a.get("href")
            if href:
                links.append(urljoin(self.BASE_URL, href))
        unique = list(dict.fromkeys(links))
        logger.info(f"Found {len(unique)} categories")
        return unique

    def _get_paginated_urls(self, category_url: str) -> List[str]:
        """Return all page URLs for a category (handles ?page=N pagination)."""
        pages = [category_url]
        soup = self._get(category_url)
        if not soup:
            return pages

        # Detect last page number from pager
        pager = soup.select("li.pager__item a")
        page_nums = []
        for a in pager:
            href = a.get("href", "")
            m = re.search(r"page=(\d+)", href)
            if m:
                page_nums.append(int(m.group(1)))

        if page_nums:
            last = max(page_nums)
            for p in range(1, last + 1):
                sep = "&" if "?" in category_url else "?"
                pages.append(f"{category_url}{sep}page={p}")

        return pages

    # ------------------------------------------------------------------
    # Job listing page -> list of job stub dicts
    # ------------------------------------------------------------------

    def _parse_listing_page(self, page_url: str) -> List[Dict]:
        soup = self._get(page_url)
        if not soup:
            return []

        stubs = []
        for card in soup.select("div.card"):
            try:
                stub: Dict[str, Any] = {}

                # Title
                t = card.select_one("span.field--name-title")
                stub["title"] = clean(t.text) if t else None

                # Job URL
                a = card.select_one("a[href]")
                if not a:
                    continue
                href = a["href"]
                stub["source_url"] = (
                    href if href.startswith("http") else urljoin(self.BASE_URL, href)
                )

                # Source job ID from URL slug / path
                stub["source_job_id"] = urlparse(stub["source_url"]).path.rstrip("/").split("/")[-1]

                # Company
                comp_a = card.select_one("p.card-text a[href]")
                if comp_a:
                    stub["company"] = comp_a["href"].split("/")[-1].replace("-", " ").title()
                else:
                    stub["company"] = None

                # Meta paragraph (experience, published date)
                meta_p = card.select_one("p.card-text")
                meta = clean(meta_p.text) if meta_p else ""
                stub["experience_years"] = extract_regex(
                    r"Experience[:\s]+([^\|]+?)(?:\s*\||\s*Published|$)", meta
                )
                stub["posted_date"] = extract_regex(
                    r"Published[:\s]+([^\|]+?)(?:\s*\||$)", meta
                )

                # Deadline
                dl = card.select_one("time")
                stub["deadline"] = clean(dl.text) if dl else extract_regex(
                    r"Deadline[:\s]+(.+?)(?:\s*\||$)", meta
                )

                # Employment type badge
                badge = card.select_one("span.badge")
                stub["employment_type_raw"] = clean(badge.text) if badge else None

                # Location - extracted from the meta paragraph text.
                # The card format is: "Company | [PIN] Kigali | Published on ..."
                # There is no dedicated .location element; location sits inline.
                stub["location_raw"] = (
                    extract_regex(
                        r"[\||]\s*(?:[PIN]\s*)?([A-Za-z][A-Za-z\s,]+?)\s*[\||].*?Published",
                        meta,
                    )
                    or extract_regex(
                        # Fallback: grab the token right after a pin emoji or "Location"
                        r"(?:[PIN]|Location[:\s]+)\s*([A-Za-z][A-Za-z\s,]+?)(?:\s*[\||]|$)",
                        meta,
                    )
                    or ""
                )

                stubs.append(stub)
            except Exception as e:
                logger.debug(f"Card parse error: {e}")
                continue

        logger.info(f"  {len(stubs)} job stubs from {page_url}")
        return stubs

    # ------------------------------------------------------------------
    # Individual job detail page
    # ------------------------------------------------------------------

    def _parse_job_detail(self, job_url: str) -> Dict:
        details: Dict[str, Any] = {}
        soup = self._get(job_url)
        if not soup:
            return details

        try:
            # Info block (sector, contract type, positions, location)
            info = soup.find("ul", class_=re.compile(r"list-group"))
            if info:
                raw = clean(info.get_text("\n")) or ""
                details["sector_raw"] = extract_regex(r"Sector[:\s]+(.+)", raw)
                details["contract_type_raw"] = extract_regex(r"Contract\s*type[:\s]+(.+)", raw)
                details["positions_count"] = extract_regex(r"Positions[:\s]+(\d+)", raw)
                loc_raw = extract_regex(r"Location[:\s]*([\w][\w\s,/\-]+?)(?:\n|$)", raw)
                if loc_raw:
                    details["location_raw"] = loc_raw
                edu_raw = extract_regex(r"Education[:\s]+(.+)", raw)
                if edu_raw:
                    details["education_raw"] = edu_raw
                exp_raw = extract_regex(r"Experience[:\s]+(.+)", raw)
                if exp_raw:
                    details["experience_raw"] = exp_raw

            # Full description
            desc_div = soup.find("div", class_=re.compile(r"field--name-field-job-full-description"))
            if desc_div:
                details["description"] = clean(desc_div.get_text(" "))

            # Application link
            apply_li = soup.find("li", class_="job-apply-btn")
            if apply_li:
                apply_a = apply_li.find("a")
                if apply_a and apply_a.get("href"):
                    details["application_link"] = urljoin(self.BASE_URL, apply_a["href"])

            # Salary (search full page text)
            page_text = clean(soup.get_text(" ")) or ""
            salary_pat = re.compile(
                r"salary|compensation|remuneration|pay|stipend", re.IGNORECASE
            )
            if salary_pat.search(page_text):
                # grab surrounding context
                m = salary_pat.search(page_text)
                if m:
                    snippet = page_text[max(0, m.start() - 30): m.end() + 80]
                    details["salary_snippet"] = snippet

        except Exception as e:
            logger.debug(f"Detail parse error [{job_url}]: {e}")

        return details

    # ------------------------------------------------------------------
    # Build final structured record
    # ------------------------------------------------------------------

    def _build_record(self, stub: Dict, detail: Dict) -> Dict:
        # Merge raw inputs
        title = stub.get("title") or ""
        company = stub.get("company") or ""
        description = detail.get("description") or ""
        sector_raw = detail.get("sector_raw") or ""
        contract_raw = (
            stub.get("employment_type_raw")
            or detail.get("contract_type_raw")
            or ""
        )
        location_raw = detail.get("location_raw") or stub.get("location_raw") or ""
        education_raw = detail.get("education_raw") or ""
        experience_raw = (
            detail.get("experience_raw")
            or stub.get("experience_years")
            or ""
        )
        combo = f"{title} {description} {sector_raw}".strip()

        # Location
        loc = infer_location(location_raw)

        # Salary
        salary_info = extract_salary(detail.get("salary_snippet") or "")

        # Field inference - pass title separately so it gets priority
        sector = (
            infer_field(combo, SECTOR_KEYWORDS, title=title)
            or clean(sector_raw)
            or ""
        )
        employment_type = (
            infer_field(contract_raw, EMPLOYMENT_TYPE_KEYWORDS, title=title)
            or infer_field(combo, EMPLOYMENT_TYPE_KEYWORDS)
            or clean(contract_raw)
            or ""
        )
        job_level = infer_field(
            f"{experience_raw} {description}", JOB_LEVEL_KEYWORDS, title=title
        ) or ""
        education_level = (
            infer_field(education_raw, EDUCATION_KEYWORDS)
            or infer_field(description, EDUCATION_KEYWORDS)
            or clean(education_raw)
            or ""
        )

        # Experience years - combine all available text sources
        exp_sources = " ".join(filter(None, [
            experience_raw,
            stub.get("experience_years", ""),
            description,
        ]))
        exp_years = extract_experience_years(exp_sources)

        source_url = stub.get("source_url", "")

        record: Dict[str, Any] = {
            # 1. BASIC INFO
            "id": make_hash(source_url),
            "title": title,
            "company": company,
            "description": description,

            # 2. LOCATION
            "location_raw": loc["location_raw"],
            "district": loc["district"],
            "country": loc["country"],
            "is_remote": loc["is_remote"],
            "is_hybrid": loc["is_hybrid"],

            # 3. RWANDA ELIGIBILITY (filled after)
            "rwanda_eligible": True,
            "eligibility_reason": "",
            "confidence_score": 0,

            # 4. JOB DETAILS
            "sector": sector,
            "job_level": job_level,
            "experience_years": exp_years,
            "employment_type": employment_type,
            "education_level": education_level,

            # 5. SALARY
            "salary_min": salary_info["salary_min"],
            "salary_max": salary_info["salary_max"],
            "currency": salary_info["currency"],
            "salary_disclosed": salary_info["salary_disclosed"],

            # 6. DATES
            "posted_date": stub.get("posted_date") or "",
            "deadline": stub.get("deadline") or "",
            "scraped_at": now_iso(),

            # 7. SOURCE
            "source": self.SOURCE_NAME,
            "source_url": source_url,
            "source_job_id": stub.get("source_job_id") or "",

            # 8. SYSTEM CONTROL
            "is_active": True,
            "last_checked": now_iso(),
            "duplicate_hash": make_hash(source_url),
        }

        # Rwanda eligibility
        eligibility = infer_rwanda_eligibility(record)
        record.update(eligibility)

        return record

    # ------------------------------------------------------------------
    # Public scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        logger.info("=" * 50)
        logger.info("Starting scrape - jobinrwanda.com")
        t0 = time.time()

        # Step 1: category links
        categories = self._get_category_links()
        if not categories:
            logger.error("No categories found - aborting")
            return pd.DataFrame()

        # Step 2: collect all page URLs (with pagination)
        all_pages: List[str] = []
        for cat in categories:
            all_pages.extend(self._get_paginated_urls(cat))
        all_pages = list(dict.fromkeys(all_pages))  # deduplicate
        logger.info(f"Total listing pages to scrape: {len(all_pages)}")

        # Step 3: scrape listing pages concurrently -> collect stubs
        all_stubs: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self._parse_listing_page, url): url
                       for url in all_pages}
            for future in as_completed(futures):
                try:
                    all_stubs.extend(future.result())
                except Exception as e:
                    logger.warning(f"Listing page error: {e}")

        # Deduplicate by URL before fetching details
        seen: set = set()
        unique_stubs = []
        for s in all_stubs:
            url = s.get("source_url")
            if url and url not in seen:
                seen.add(url)
                unique_stubs.append(s)
        logger.info(f"Unique jobs to detail-fetch: {len(unique_stubs)}")

        # Step 4: fetch detail pages concurrently
        records: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {
                ex.submit(self._parse_job_detail, s["source_url"]): s
                for s in unique_stubs if s.get("source_url")
            }
            for future in as_completed(futures):
                stub = futures[future]
                try:
                    detail = future.result()
                    records.append(self._build_record(stub, detail))
                except Exception as e:
                    logger.warning(f"Detail fetch error: {e}")

        df = pd.DataFrame(records)

        elapsed = time.time() - t0
        logger.info(f"Scrape complete in {elapsed:.1f}s - {len(df)} jobs")
        logger.info("=" * 50)
        return df

    # ------------------------------------------------------------------
    # Save helpers
    # ------------------------------------------------------------------

    def save_csv(self, df: pd.DataFrame, path: str = "rwanda_jobs.csv"):
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved CSV -> {path}")

    def save_excel(self, df: pd.DataFrame, path: str = "rwanda_jobs.xlsx"):
        df.to_excel(path, index=False, engine="openpyxl")
        logger.info(f"Saved Excel -> {path}")

    def save_json(self, df: pd.DataFrame, path: str = "rwanda_jobs.json"):
        df.to_json(path, orient="records", indent=2, force_ascii=False)
        logger.info(f"Saved JSON -> {path}")


# ----------------------------------------------
# Entry point
# ----------------------------------------------

def main():
    scraper = JobScraper(
        request_delay=0.5,   # 0.5 s between requests (was 1.0)
        timeout=15,
        max_workers=8,       # concurrent threads
    )

    df = scraper.scrape()

    if df.empty:
        logger.warning("No jobs scraped.")
        return

    # Summary
    print("\n" + "=" * 55)
    print("  SCRAPE SUMMARY")
    print("=" * 55)
    print(f"  Total jobs         : {len(df)}")
    print(f"  Rwanda eligible    : {df['rwanda_eligible'].sum()}")
    print(f"  Salary disclosed   : {df['salary_disclosed'].sum()}")
    print(f"  Remote jobs        : {df['is_remote'].sum()}")
    print(f"\n  Top sectors:")
    for sector, count in df["sector"].value_counts().head(5).items():
        print(f"    {sector:<20} {count}")
    print(f"\n  Employment types:")
    for etype, count in df["employment_type"].value_counts().head(5).items():
        print(f"    {etype:<20} {count}")
    print("=" * 55)

    scraper.save_csv(df, "rwanda_jobs.csv")
    scraper.save_excel(df, "rwanda_jobs.xlsx")
    scraper.save_json(df, "rwanda_jobs.json")


if __name__ == "__main__":
    main()