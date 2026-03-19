"""
Job Scraper for jobnziza.com
=============================
Outputs a rich structured schema matching all other Rwanda Jobs scrapers.

CONFIRMED SITE STRUCTURE (fetched 2026-03-19):
  Listing : https://jobnziza.com/latest-jobs?category=job
            -> All active jobs on ONE page, fully static HTML, no JS needed
            -> Additional categories: consultancy, internship, volunteer, training
  Detail  : https://jobnziza.com/read_job_post.php?slug={slug}
            -> Fully static HTML, all structured fields available

LISTING CARD HTML (confirmed):
  <a href="/read_job_post.php?slug=SLUG">
    <h5>Job Title</h5>
    <h6>Company Name</h6>
  </a>
  **Location:** Kigali , Rwanda
  **Category:** job
  **Positions:** 2
  **Published:** 18/03/2026
  **Deadline:** 19/04/2026 17:00
  <img alt="Company Name logo" src="/uploads/logos/xxx.jpg">

DETAIL PAGE FIELDS (confirmed from live fetch):
  Title:       h4 tag
  Company:     h5 tag
  Location:    "[PIN] Location: Kigali , Rwanda"
  Type:        "[DOC] Type: Full-time"
  Education:   "[EDU] Education: Bachelor's Degree (A0)"
  Experience:  "[WORK] Experience: 2-3 years"
  Positions:   "[PEOPLE] Positions: 2"
  Deadline:    "[DATE] Deadline: 19/04/2026"
  Posted:      "[NEW] Posted: 18/03/2026"
  Description: Full text body

CATEGORIES SCRAPED:
  job, consultancy, internship, volunteer, training
  Skipped: tender, cyamunara, public (not employment listings)
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
        logging.FileHandler("job_scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ----------------------------------------------
# Constants / lookup tables  (identical to all other scrapers)
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
    "Entry":      ["entry", "junior", "fresh", "graduate", "trainee", "0-1 year", "1 year"],
    "Mid":        ["mid", "2-3 year", "3-5 year", "2 years", "3 years", "4 years", "5 years"],
    "Senior":     ["senior", "lead", "principal", "expert", "6+ year", "7+ year",
                   "8+ year", "10+ year", "manager", "head of", "director"],
    "Executive":  ["ceo", "cfo", "cto", "coo", "vp ", "vice president",
                   "executive director", "country director", "chief"],
}

EMPLOYMENT_TYPE_KEYWORDS = {
    "Full-time":   ["full-time", "full time", "permanent", "indefinite"],
    "Part-time":   ["part-time", "part time"],
    "Contract":    ["contract", "fixed-term", "fixed term", "temporary"],
    "Internship":  ["internship", "attachment", "apprentice"],
    "Consultancy": ["consultant", "consultancy", "freelance"],
    "Volunteer":   ["volunteer", "voluntary"],
}

EDUCATION_KEYWORDS = {
    "PhD":         ["phd", "doctorate", "doctoral"],
    "Master's":    ["master", "msc", "mba", "ma ", "m.sc", "m.a.", "a2"],
    "Bachelor's":  ["bachelor", "bsc", "ba ", "b.sc", "b.a.", "degree",
                    "undergraduate", "university", "a0"],
    "Diploma":     ["diploma", "a-level", "advanced level", "hnd", "a1"],
    "Certificate": ["certificate", "o-level", "tvet", "vocational"],
}

CURRENCY_SYMBOLS = {
    "RWF": ["rwf", "frw", "francs rwandais"],
    "USD": ["usd", "$", "dollars"],
    "EUR": ["eur", "euros"],
}

# JobNziza category -> employment_type mapping (confirmed from live data)
CATEGORY_TO_EMPLOYMENT_TYPE = {
    "job":         "Full-time",
    "consultancy": "Consultancy",
    "internship":  "Internship",
    "volunteer":   "Volunteer",
    "training":    "Training",
    "other":       "Contract",
}

# Categories to scrape (skip tender, cyamunara, public - not job listings)
SCRAPE_CATEGORIES = ["job", "consultancy", "internship", "volunteer", "training"]


# ----------------------------------------------
# Helper functions  (identical to all other scrapers)
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
    Return best matching category.
    1. Match title first (stronger signal).
    2. Fall back to full text. Longer keyword phrases preferred.
    Short / ambiguous keywords use word-boundary matching.
    """
    BOUNDARY_WORDS = {"ceo", "cfo", "cto", "coo", "vp", "intern", "ngo",
                      "ingo", "msf", "irc", "hiv", "ba ", "ma "}

    def best_match(haystack: str) -> Optional[str]:
        h = haystack.lower()
        best_cat, best_len = None, 0
        for category, keywords in keyword_map.items():
            for kw in sorted(keywords, key=len, reverse=True):
                if len(kw) <= 4 or kw.rstrip() in BOUNDARY_WORDS:
                    matched = bool(re.search(rf"\b{re.escape(kw.strip())}\b", h))
                else:
                    matched = kw in h
                if matched and len(kw) > best_len:
                    best_cat, best_len = category, len(kw)
                    break
        return best_cat

    if title:
        result = best_match(title)
        if result:
            return result
    return best_match(text) if text else None


def extract_experience_years(text: str) -> str:
    if not text:
        return ""
    t = text.lower()
    if re.search(r"no\s+(prior\s+)?experience", t):
        return "0"
    m = re.search(r"(\d+)\s*(?:to|-|-|-)\s*(\d+)\s*(?:years?|yrs?)", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(\d+)\s*\+\s*(?:years?|yrs?)", t)
    if m:
        return f"{m.group(1)}+"
    m = re.search(r"(?:at\s+least|minimum|over|more\s+than)\s+(\d+)\s*(?:years?|yrs?)", t)
    if m:
        return f"{m.group(1)}+"
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
    if "remote" in t or "home-based" in t or "home based" in t:
        loc["is_remote"] = True
    if "hybrid" in t:
        loc["is_hybrid"] = True
    for district in RWANDA_DISTRICTS:
        if district in t:
            loc["district"] = district.capitalize()
            break
    if "kigali" in t and not loc["district"]:
        loc["district"] = "Kigali"
    return loc


def infer_rwanda_eligibility(job: Dict) -> Dict:
    text = " ".join(filter(None, [
        job.get("title", ""),
        job.get("description", ""),
        job.get("location_raw", ""),
    ])).lower()

    if job.get("is_remote"):
        return {"rwanda_eligible": True,
                "eligibility_reason": "Remote job - open globally",
                "confidence_score": 4}

    disqualifiers = [
        "us citizens only", "eu residents only", "must be based in",
        "authorized to work in the us",
    ]
    for d in disqualifiers:
        if d in text:
            return {"rwanda_eligible": False,
                    "eligibility_reason": f"Disqualifying clause: '{d}'",
                    "confidence_score": 5}

    for sig in ["rwanda", "kigali", "rwandan"]:
        if sig in text:
            return {"rwanda_eligible": True,
                    "eligibility_reason": "Explicitly mentions Rwanda location",
                    "confidence_score": 5}

    return {"rwanda_eligible": True,
            "eligibility_reason": "Listed on jobnziza.com",
            "confidence_score": 5}


def parse_jobnziza_date(raw: str) -> str:
    """
    Convert jobnziza date format to dashboard format.
    'DD/MM/YYYY HH:MM' or 'DD/MM/YYYY' -> 'on DD-MM-YYYY'
    """
    if not raw:
        return ""
    date_part = raw.strip().split()[0]  # strip time component
    try:
        dt = datetime.strptime(date_part, "%d/%m/%Y")
        return f"on {dt.strftime('%d-%m-%Y')}"
    except ValueError:
        return raw.strip()


# ----------------------------------------------
# Scraper class
# ----------------------------------------------

class JobScraper:
    BASE_URL    = "https://jobnziza.com"
    SOURCE_NAME = "jobnziza"

    def __init__(self, request_delay: float = 0.5,
                 timeout: int = 20,
                 max_workers: int = 8):
        self.request_delay = request_delay
        self.timeout       = timeout
        self.max_workers   = max_workers
        self.session       = self._build_session()

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
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
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
    # Listing pages
    #
    # Scrapes each employment category separately to capture all types.
    # Tenders / cyamunara / public are skipped (not employment listings).
    #
    # Card HTML structure (confirmed from live page):
    #
    #   <a href="/read_job_post.php?slug=SLUG">
    #     <h5>Job Title</h5>
    #     <h6>Company Name</h6>  (optional - sometimes missing)
    #   </a>
    #   **Location:** Kigali , Rwanda
    #   **Category:** job
    #   **Positions:** 2
    #   **Published:** 18/03/2026
    #   **Deadline:** 19/04/2026 17:00
    #   <img alt="Company Name logo" src="/uploads/logos/xxx.jpg">
    #
    # The card fields sit OUTSIDE the <a> tag as sibling elements,
    # so we find each card's parent container and extract the siblings.
    # ------------------------------------------------------------------

    def _get_all_stubs(self) -> List[Dict]:
        all_stubs: List[Dict] = []

        for category in SCRAPE_CATEGORIES:
            url = f"{self.BASE_URL}/latest-jobs?category={category}"
            logger.info(f"  Scraping category '{category}': {url}")
            soup = self._get(url)
            if not soup:
                continue

            stubs = self._parse_listing_page(soup, category)
            logger.info(f"  Category '{category}': {len(stubs)} stubs found.")
            all_stubs.extend(stubs)

        return all_stubs

    def _parse_listing_page(self, soup: BeautifulSoup, category: str) -> List[Dict]:
        """
        Parse all job card links from a category listing page.

        Each card is identified by links to /read_job_post.php?slug=...
        The structured fields (location, deadline, etc.) are in the
        surrounding container element.
        """
        stubs = []

        # Find all job card links
        card_links = [
            a for a in soup.find_all("a", href=True)
            if "read_job_post" in a.get("href", "")
               and a.find("h5")  # must have a title heading
        ]

        for a in card_links:
            try:
                href = a.get("href", "")
                source_url    = urljoin(self.BASE_URL, href)
                source_job_id = extract_regex(r"slug=([^&]+)", href) or \
                                urlparse(source_url).path.rstrip("/").split("/")[-1]

                # Title from h5, company from h6 inside the link
                h5 = a.find("h5")
                h6 = a.find("h6")
                title   = clean(h5.get_text()) if h5 else ""
                company = clean(h6.get_text()) if h6 else ""

                # Card metadata sits in the parent container as siblings
                # Walk up to find the card wrapper
                container = a.parent
                card_text  = container.get_text(" ", strip=True) if container else ""

                # Extract structured fields using bold-label patterns
                # Location stops at first occurrence of another field label
                location = extract_regex(
                    r"Location[:\s]+([A-Za-z][^*\n]{1,60}?)(?:\s*\*\*|\s*Category|\s*Positions|\s*Published|\s*Deadline|$)",
                    card_text
                )
                published = extract_regex(r"Published[:\s]+(\d{2}/\d{2}/\d{4})", card_text)
                deadline  = extract_regex(r"Deadline[:\s]+(\d{2}/\d{2}/\d{4})", card_text)

                # Company fallback from img alt tag
                if not company:
                    img = container.find("img", alt=True) if container else None
                    if img:
                        alt = img.get("alt", "")
                        if "logo" in alt.lower():
                            company = clean(alt.replace("logo", "").strip())
                        else:
                            company = clean(alt)

                if not title:
                    continue

                stubs.append({
                    "title":          title,
                    "company":        company or "",
                    "category":       category,
                    "source_url":     source_url,
                    "source_job_id":  source_job_id,
                    "location_raw":   clean(location) or "Rwanda",
                    "posted_date":    parse_jobnziza_date(published or ""),
                    "deadline":       parse_jobnziza_date(deadline or ""),
                    "employment_type": CATEGORY_TO_EMPLOYMENT_TYPE.get(category, "Contract"),
                })

            except Exception as e:
                logger.debug(f"Card parse error: {e}")
                continue

        return stubs

    # ------------------------------------------------------------------
    # Individual job detail page
    #
    # Confirmed fields from live fetch of /read_job_post.php?slug=loan-officer:
    #
    #   h4  -> title  (e.g. "Loan Officer")
    #   h5  -> company (e.g. "Icyerekezo SACCO Nyarugenge (ISN)")
    #   "[PIN] Location: Kigali , Rwanda"
    #   "[DOC] Type: Full-time"
    #   "[EDU] Education: Bachelor's Degree (A0)"
    #   "[WORK] Experience: 2-3 years"
    #   "[PEOPLE] Positions: 2"
    #   "[DATE] Deadline: 19/04/2026"
    #   "[NEW] Posted: 18/03/2026"
    #   "[NOTE] Quick Job Overview" -> summary paragraph
    #   "[DOC] Job Description" -> full formatted description
    # ------------------------------------------------------------------

    def _parse_job_detail(self, job_url: str) -> Dict:
        details: Dict[str, Any] = {}
        soup = self._get(job_url)
        if not soup:
            return details

        try:
            # Remove nav/footer/ads to avoid polluting text
            for tag in soup(["nav", "footer", "header", "script", "style",
                              "aside", ".sidebar"]):
                tag.decompose()

            page_text = soup.get_text(" ", strip=True)

            # Title (h4 is the job title on detail pages)
            h4 = soup.find("h4")
            if h4:
                details["title"] = clean(h4.get_text())

            # Company (h5 immediately after h4)
            h5 = soup.find("h5")
            if h5:
                details["company_raw"] = clean(h5.get_text())

            # Structured meta fields (emoji-labelled)
            details["location_raw"] = extract_regex(
                r"Location[:\s]+(.+?)(?:\n|[DOC]|[NOTE]|[EDU]|[WORK]|[VIEW]|[PEOPLE]|[DATE]|[NEW]|$)", page_text
            )
            details["employment_type_raw"] = extract_regex(
                r"Type[:\s]+(.+?)(?:\n|[PIN]|[NOTE]|[EDU]|[WORK]|[VIEW]|[PEOPLE]|[DATE]|[NEW]|$)", page_text
            )
            details["education_raw"] = extract_regex(
                r"Education[:\s]+(.+?)(?:\n|[PIN]|[DOC]|[NOTE]|[WORK]|[VIEW]|[PEOPLE]|[DATE]|[NEW]|$)", page_text
            )
            details["experience_raw"] = extract_regex(
                r"Experience[:\s]+(.+?)(?:\n|[PIN]|[DOC]|[NOTE]|[EDU]|[VIEW]|[PEOPLE]|[DATE]|[NEW]|$)", page_text
            )
            deadline_raw = extract_regex(
                r"Deadline[:\s]+(\d{2}/\d{2}/\d{4})", page_text
            )
            posted_raw = extract_regex(
                r"Posted[:\s]+(\d{2}/\d{2}/\d{4})", page_text
            )
            if deadline_raw:
                details["deadline"]     = parse_jobnziza_date(deadline_raw)
            if posted_raw:
                details["posted_date"]  = parse_jobnziza_date(posted_raw)

            # Full description - "[DOC] Job Description" section
            desc_section = soup.find(
                lambda tag: tag.name in ("div", "section") and
                "job" in (tag.get("class") or [""])[0].lower()
            )
            if not desc_section:
                # Fallback: grab largest text block after h4
                desc_section = soup.find("div", class_=re.compile(r"description|content|body", re.I))

            if desc_section:
                for tag in desc_section(["script", "style"]):
                    tag.decompose()
                details["description"] = clean(desc_section.get_text(" "))
            else:
                # Last resort: full page text (trimmed)
                details["description"] = clean(page_text)[:3000]

            # Salary context
            sal_pat = re.compile(r"salary|remuneration|compensation|pay", re.I)
            m = sal_pat.search(page_text)
            if m:
                details["salary_snippet"] = page_text[max(0, m.start()-20): m.end()+100]

        except Exception as e:
            logger.debug(f"Detail parse error [{job_url}]: {e}")

        return details

    # ------------------------------------------------------------------
    # Build the final 30-column record
    # ------------------------------------------------------------------

    def _build_record(self, stub: Dict, detail: Dict) -> Dict:
        title        = detail.get("title")        or stub.get("title")   or ""
        company      = detail.get("company_raw")  or stub.get("company") or ""
        description  = detail.get("description")  or ""
        location_raw = detail.get("location_raw") or stub.get("location_raw") or "Rwanda"
        emp_type_raw = detail.get("employment_type_raw") or stub.get("employment_type") or ""
        edu_raw      = detail.get("education_raw") or ""
        exp_raw      = detail.get("experience_raw") or ""

        combo = f"{title} {description}".strip()

        loc = infer_location(location_raw)

        # Employment type: detail page value takes precedence over category default
        employment_type = (
            infer_field(emp_type_raw, EMPLOYMENT_TYPE_KEYWORDS)
            or stub.get("employment_type")
            or "Full-time"
        )

        sector          = infer_field(combo, SECTOR_KEYWORDS, title=title) or ""
        job_level       = infer_field(combo, JOB_LEVEL_KEYWORDS, title=title) or ""

        # Education: detail page has structured field "Bachelor's Degree (A0)"
        education_level = (
            infer_field(edu_raw, EDUCATION_KEYWORDS)
            or infer_field(description, EDUCATION_KEYWORDS)
            or clean(edu_raw)
            or ""
        )

        # Experience: detail page has "2-3 years" directly
        exp_years = (
            extract_experience_years(exp_raw)
            or extract_experience_years(description)
        )

        salary_info = extract_salary(detail.get("salary_snippet") or "")
        source_url  = stub.get("source_url", "")

        record: Dict[str, Any] = {
            # 1. BASIC INFO
            "id":          make_hash(source_url),
            "title":       title,
            "company":     company,
            "description": description,

            # 2. LOCATION
            "location_raw": loc["location_raw"],
            "district":     loc["district"],
            "country":      loc["country"],
            "is_remote":    loc["is_remote"],
            "is_hybrid":    loc["is_hybrid"],

            # 3. RWANDA ELIGIBILITY (filled below)
            "rwanda_eligible":    True,
            "eligibility_reason": "",
            "confidence_score":   0,

            # 4. JOB DETAILS
            "sector":           sector,
            "job_level":        job_level,
            "experience_years": exp_years,
            "employment_type":  employment_type,
            "education_level":  education_level,

            # 5. SALARY
            "salary_min":       salary_info["salary_min"],
            "salary_max":       salary_info["salary_max"],
            "currency":         salary_info["currency"],
            "salary_disclosed": salary_info["salary_disclosed"],

            # 6. DATES
            "posted_date": detail.get("posted_date") or stub.get("posted_date") or "",
            "deadline":    detail.get("deadline")    or stub.get("deadline")    or "",
            "scraped_at":  now_iso(),

            # 7. SOURCE
            "source":        self.SOURCE_NAME,
            "source_url":    source_url,
            "source_job_id": stub.get("source_job_id") or "",

            # 8. SYSTEM CONTROL
            "is_active":      True,
            "last_checked":   now_iso(),
            "duplicate_hash": make_hash(source_url),
        }

        eligibility = infer_rwanda_eligibility(record)
        record.update(eligibility)
        return record

    # ------------------------------------------------------------------
    # Public scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        logger.info("=" * 50)
        logger.info("Starting scrape - jobnziza.com")
        t0 = time.time()

        # Step 1: collect stubs from all job categories
        all_stubs = self._get_all_stubs()
        if not all_stubs:
            logger.error("No stubs found - aborting.")
            return pd.DataFrame()

        # Step 2: deduplicate by source URL
        seen: set = set()
        unique_stubs = []
        for s in all_stubs:
            url = s.get("source_url")
            if url and url not in seen:
                seen.add(url)
                unique_stubs.append(s)
        logger.info(f"Unique jobs to detail-fetch: {len(unique_stubs)}")

        # Step 3: fetch detail pages concurrently
        records: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {
                ex.submit(self._parse_job_detail, s["source_url"]): s
                for s in unique_stubs
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
    # Save helpers  (identical interface to all other scrapers)
    # ------------------------------------------------------------------

    def save_csv(self, df: pd.DataFrame, path: str = "jobnziza_jobs.csv"):
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved CSV -> {path}")

    def save_excel(self, df: pd.DataFrame, path: str = "jobnziza_jobs.xlsx"):
        df.to_excel(path, index=False, engine="openpyxl")
        logger.info(f"Saved Excel -> {path}")

    def save_json(self, df: pd.DataFrame, path: str = "jobnziza_jobs.json"):
        df.to_json(path, orient="records", indent=2, force_ascii=False)
        logger.info(f"Saved JSON -> {path}")


# ----------------------------------------------
# Entry point
# ----------------------------------------------

def main():
    scraper = JobScraper(
        request_delay=0.5,
        timeout=20,
        max_workers=8,
    )

    df = scraper.scrape()

    if df.empty:
        logger.warning("No jobs scraped.")
        return

    print("\n" + "=" * 55)
    print("  SCRAPE SUMMARY")
    print("=" * 55)
    print(f"  Total jobs         : {len(df)}")
    print(f"  Rwanda eligible    : {df['rwanda_eligible'].sum()}")
    print(f"  Salary disclosed   : {df['salary_disclosed'].sum()}")
    print(f"  Remote jobs        : {df['is_remote'].sum()}")
    print(f"  Deadline captured  : {(df['deadline'] != '').sum()}")
    print(f"  Education captured : {(df['education_level'] != '').sum()}")
    print(f"\n  Top sectors:")
    for sector, count in df["sector"].value_counts().head(5).items():
        print(f"    {sector:<20} {count}")
    print(f"\n  Employment types:")
    for etype, count in df["employment_type"].value_counts().head(5).items():
        print(f"    {etype:<20} {count}")
    print(f"\n  Job levels:")
    for level, count in df["job_level"].value_counts().head(5).items():
        print(f"    {level:<20} {count}")
    print("=" * 55)

    scraper.save_csv(df, "jobnziza_jobs.csv")
    scraper.save_excel(df, "jobnziza_jobs.xlsx")
    scraper.save_json(df, "jobnziza_jobs.json")


if __name__ == "__main__":
    main()
