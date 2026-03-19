"""
Job Scraper for impactpool.org/countries/Rwanda
================================================
Outputs a rich structured schema matching all other Rwanda Jobs scrapers.

CONFIRMED SITE STRUCTURE (fetched 2026-03-19):
  Listing : https://www.impactpool.org/countries/Rwanda
            -> 19 jobs, all on one page, fully static HTML (no JS needed)
  Detail  : https://www.impactpool.org/jobs/{id}
            -> fully static HTML, all fields accessible

LISTING CARD HTML STRUCTURE (two variants):
  Cards WITH org logo:
    <a href="/jobs/ID">
      <img alt="Org Name" ...>       <- logo, alt=company
      Title
      Org Name                       <- duplicated after logo
      Location (pipe-separated)
      Grade / Level string
    </a>

  Cards WITHOUT org logo:
    <a href="/jobs/ID">
      Title
      Org Name
      Location
      Grade / Level string
    </a>

DETAIL PAGE FIELDS (confirmed from live page):
  <h1>                         -> title
  "Org * Location * Grade * Language"  -> meta line (bullet-dot separated)
  "Application deadline: DATE" -> deadline
  Full description body        -> description

GRADE STRING EXAMPLES (confirmed from live data):
  "IPSA-9, International Personnel Services Agreement - Junior level"
  "NO-B, National Professional Officer - Locally recruited position - Junior level"
  "NPSA-10, National Personnel Services Agreement - Mid level"
  "Associate Level - Open for both International and National Professionals - Mid level"
  "Managerial Level - ... - Senior level"
  "International Consultant - Internationally recruited Contractors Agreement - Consultancy"
  "Level not specified"
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
    "Contract":    ["contract", "fixed-term", "fixed term", "temporary", "agreement"],
    "Internship":  ["intern", "attachment", "apprentice"],
    "Consultancy": ["consultant", "consultancy", "freelance", "individual contractor"],
    "Volunteer":   ["volunteer", "voluntary", "unv", "un volunteer"],
}

EDUCATION_KEYWORDS = {
    "PhD":         ["phd", "doctorate", "doctoral"],
    "Master's":    ["master", "msc", "mba", "ma ", "m.sc", "m.a.", "advanced university"],
    "Bachelor's":  ["bachelor", "bsc", "ba ", "b.sc", "b.a.", "degree",
                    "undergraduate", "university"],
    "Diploma":     ["diploma", "a-level", "advanced level", "hnd"],
    "Certificate": ["certificate", "o-level", "tvet", "vocational"],
}

CURRENCY_SYMBOLS = {
    "RWF": ["rwf", "frw", "francs rwandais"],
    "USD": ["usd", "$", "dollars"],
    "EUR": ["eur", "euros"],
}


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
    Short / ambiguous keywords use word-boundary matching to avoid
    false positives (e.g. 'cto' in 'contractor', 'intern' in 'international').
    """
    # These short words need word-boundary matching regardless of length
    BOUNDARY_WORDS = {"ceo", "cfo", "cto", "coo", "vp", "intern", "ngo", "ingo",
                      "msf", "irc", "hiv", "ba ", "ma "}

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
            "eligibility_reason": "Listed on impactpool.org Rwanda page",
            "confidence_score": 5}


def grade_to_level(grade_str: str) -> str:
    """
    Parse impactpool grade string to job level.
    Examples:
      "IPSA-9 ... - Junior level"           -> "Entry"
      "NO-B ... - Junior level"             -> "Entry"
      "NPSA-10 ... - Mid level"             -> "Mid"
      "Associate Level ... - Mid level"     -> "Mid"
      "Managerial Level ... - Senior level" -> "Senior"
      "NPSA-4 ... - Administrative support" -> "Entry"
      "Level not specified"                 -> ""
    """
    if not grade_str:
        return ""
    g = grade_str.lower()
    # Explicit level suffixes (most reliable - impactpool always appends these)
    if "junior level" in g or "administrative support" in g or "entry level" in g:
        return "Entry"
    if "mid level" in g:
        return "Mid"
    if "senior level" in g or "managerial level" in g:
        return "Senior"
    if "executive level" in g or "director level" in g:
        return "Executive"
    if "internship" in g or "intern level" in g:
        return "Internship"
    # Fallback: keyword inference
    return infer_field(grade_str, JOB_LEVEL_KEYWORDS) or ""


def grade_to_employment_type(grade_str: str) -> str:
    """
    Parse impactpool grade string to employment type.
    Uses word boundaries for short tokens to avoid 'intern' inside 'international'.
    """
    if not grade_str:
        return "Contract"
    g = grade_str.lower().strip()
    # "CON" is impactpool's abbreviation for Consultant/Contractor
    if g == "con" or any(k in g for k in ["consultant", "consultancy", "contractor"]):
        return "Consultancy"
    if any(k in g for k in ["volunteer", "unv"]):
        return "Volunteer"
    # Word boundary: "intern" alone, not inside "international"
    if re.search(r"\bintern\b", g) or "internship" in g or "attachment" in g:
        return "Internship"
    return "Contract"


# ----------------------------------------------
# Scraper class
# ----------------------------------------------

class JobScraper:
    BASE_URL      = "https://www.impactpool.org"
    LISTING_URL   = "https://www.impactpool.org/countries/Rwanda"
    SOURCE_NAME   = "impactpool"

    def __init__(self, request_delay: float = 1.0,
                 timeout: int = 20,
                 max_workers: int = 5):
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
    # Listing page scrape
    #
    # All 19 Rwanda jobs appear on a single page - no pagination needed.
    # "Show more" link exists but points to /search filtered by Rwanda (wl[]=184)
    # which returns the same set.
    #
    # Card HTML (two variants confirmed from live page):
    #
    # WITH logo:
    #   <a href="/jobs/ID" class="...">
    #     <img alt="Org Name" src="logo.svg">
    #     <h3 or div>Job Title</h3>
    #     <span or p>Org Name</span>        <- ellipse dot separator
    #     <span or p>Location</span>        <- ellipse dot separator
    #     <span or p>Grade/Level</span>     <- ellipse dot separator
    #   </a>
    #
    # WITHOUT logo (UNDP jobs without image in this list):
    #   <a href="/jobs/ID">
    #     <h3 or div>Job Title</h3>
    #     <span or p>Org Name</span>
    #     <span or p>Location</span>
    #     <span or p>Grade/Level</span>
    #   </a>
    #
    # The ellipse images (*) are <img> with src containing "ellipse"
    # used as visual separators - we strip them when parsing text.
    # ------------------------------------------------------------------

    def _get_listing_stubs(self) -> List[Dict]:
        logger.info(f"  Fetching listing page: {self.LISTING_URL}")
        soup = self._get(self.LISTING_URL)
        if not soup:
            return []

        # All job card links: href="/jobs/{id}"
        job_links = [
            a for a in soup.find_all("a", href=True)
            if re.match(r"^/jobs/\d+$", a.get("href", ""))
        ]
        logger.info(f"  Found {len(job_links)} job card links.")

        stubs = []
        for a in job_links:
            stub = self._parse_card(a)
            if stub:
                stubs.append(stub)
        return stubs

    def _parse_card(self, a_tag) -> Optional[Dict]:
        """
        Parse one job card <a> element.

        Text extraction strategy:
          1. Check for org logo <img alt="..."> -> company name
          2. Get all text nodes excluding ellipse separator images
          3. Lines: [possibly_org_if_logo, title, org, location, grade]
        """
        href = a_tag.get("href", "")
        source_url    = urljoin(self.BASE_URL, href)
        source_job_id = href.strip("/").split("/")[-1]

        # Remove ellipse separator images from text extraction
        for img in a_tag.find_all("img", src=re.compile(r"ellipse")):
            img.decompose()

        # Check for org logo (img that is NOT an ellipse)
        logo_img  = a_tag.find("img", src=re.compile(r"logo|logos|cdn"))
        logo_alt  = clean(logo_img.get("alt", "")) if logo_img else ""
        if logo_img:
            logo_img.decompose()   # remove before text extraction

        # Extract all remaining text segments
        text = a_tag.get_text("\n", strip=True)
        lines = [clean(l) for l in text.split("\n") if clean(l)]

        # "Closing today" badge sometimes appears - remove it
        lines = [l for l in lines if l and l.lower() not in ("closing today", "closing soon")]

        if not lines:
            return None

        # Parse fields based on whether there was a logo
        # WITH logo: lines = [title, org_name_duplicate, location, grade]
        #   (logo alt = company; the text org is a duplicate at index 1)
        # WITHOUT:   lines = [title, org_name, location, grade]
        if logo_alt:
            # Remove the org duplicate line (matches logo_alt at any position)
            lines = [l for l in lines if l.lower() != logo_alt.lower()]
            title    = lines[0] if len(lines) > 0 else ""
            company  = logo_alt
            location = lines[1] if len(lines) > 1 else ""
            grade    = lines[2] if len(lines) > 2 else ""
        else:
            title    = lines[0] if len(lines) > 0 else ""
            company  = lines[1] if len(lines) > 1 else ""
            location = lines[2] if len(lines) > 2 else ""
            grade    = lines[3] if len(lines) > 3 else ""

        # Strip " | " separated multi-locations - keep first Rwanda-relevant one
        if "|" in location:
            loc_parts = [p.strip() for p in location.split("|")]
            # Prefer Kigali or any Rwanda location; otherwise keep first
            rwanda_locs = [p for p in loc_parts
                           if any(r in p.lower() for r in ["kigali", "rwanda", "remote"])]
            location = rwanda_locs[0] if rwanda_locs else loc_parts[0]

        if not title:
            return None

        return {
            "title":          title,
            "company":        company,
            "grade":          grade,
            "source_url":     source_url,
            "source_job_id":  source_job_id,
            "location_raw":   location,
            "posted_date":    "",
            "deadline":       "",
        }

    # ------------------------------------------------------------------
    # Individual job detail page
    #
    # Confirmed structure from live page fetch:
    #   <h1>                              -> title (authoritative)
    #   <img alt="Org Name"> (logo)       -> company (or nearby text)
    #   meta dot-line: "Loc * Scope * Grade * Language"
    #   "Application deadline: DATE"      -> deadline
    #   Full description text body
    #
    # Salary: almost never disclosed on impactpool (UN/INGO jobs)
    # ------------------------------------------------------------------

    def _parse_job_detail(self, job_url: str) -> Dict:
        details: Dict[str, Any] = {}
        soup = self._get(job_url)
        if not soup:
            return details

        try:
            # Title
            h1 = soup.find("h1")
            if h1:
                details["title"] = clean(h1.get_text())

            # Company from logo alt text or page title pattern "Title | Org | Impactpool"
            page_title = soup.find("title")
            if page_title:
                parts = page_title.get_text().split("|")
                if len(parts) >= 2:
                    details["company_raw"] = clean(parts[1])

            # Deadline: "Application deadline: March 20, 2026 (6 days)"
            # On impactpool the label and date are often in SEPARATE sibling <span> tags.
            # soup.find(string=...) only returns the label span, not the date.
            # Fix: find the label, walk up to its parent, get the parent's full text.
            deadline_label = soup.find(string=re.compile(r"application deadline", re.I))
            if deadline_label:
                # Walk up until the parent element contains the date too
                parent = deadline_label.parent
                for _ in range(4):   # max 4 levels up
                    parent_text = parent.get_text(" ", strip=True) if parent else ""
                    if re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|\d{4})\b",
                                 parent_text, re.I):
                        break
                    if parent and parent.parent:
                        parent = parent.parent
                full_dl = parent.get_text(" ", strip=True) if parent else ""
                m = re.search(
                    r"application deadline[:\s]+(.+?)(?:\s*\(|$)",
                    full_dl, re.IGNORECASE
                )
                if m:
                    details["deadline"] = clean(m.group(1))

            # Full description - main content div (everything after the meta line)
            # Impactpool uses a large content div; extract all paragraphs
            # Exclude nav, footer, sidebar elements
            for tag in soup(["nav", "footer", "header", "script", "style"]):
                tag.decompose()
            # Find the main article/content area
            main = (
                soup.find("main") or
                soup.find("article") or
                soup.find("div", class_=re.compile(r"content|description|body|job", re.I))
            )
            if main:
                details["description"] = clean(main.get_text(" "))
            else:
                details["description"] = clean(soup.get_text(" "))

            # Salary context
            page_text = details.get("description", "")
            sal_pat = re.compile(r"salary|remuneration|compensation|pay scale", re.I)
            m = sal_pat.search(page_text or "")
            if m:
                details["salary_snippet"] = page_text[max(0, m.start()-30): m.end()+100]

            # Posted date - impactpool doesn't show posted date on listing
            # but sometimes shows "X days ago" or a date in description
            posted = extract_regex(
                r"(?:posted|published)[:\s]+(\d{1,2}\s+\w+\s+\d{4}|\w+\s+\d{1,2},?\s+\d{4})",
                page_text or ""
            )
            if posted:
                details["posted_date"] = f"on {posted}"

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
        grade        = stub.get("grade") or ""
        location_raw = stub.get("location_raw")   or "Kigali, Rwanda"

        combo = f"{title} {description} {grade}".strip()

        loc = infer_location(location_raw)

        # Grade string gives us level and employment type directly
        job_level       = grade_to_level(grade)         or infer_field(combo, JOB_LEVEL_KEYWORDS, title=title) or ""
        employment_type = grade_to_employment_type(grade)

        sector = (
            infer_field(combo, SECTOR_KEYWORDS, title=title)
            or "NGO"    # impactpool is exclusively UN/INGO/impact sector
        )
        education_level = infer_field(description, EDUCATION_KEYWORDS) or ""
        exp_years       = extract_experience_years(description)
        salary_info     = extract_salary(detail.get("salary_snippet") or "")

        source_url = stub.get("source_url", "")

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
        logger.info("Starting scrape - impactpool.org/countries/Rwanda")
        t0 = time.time()

        # Step 1: listing page
        all_stubs = self._get_listing_stubs()
        if not all_stubs:
            logger.error("No stubs found - aborting.")
            return pd.DataFrame()

        # Step 2: deduplicate by URL
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

    def save_csv(self, df: pd.DataFrame, path: str = "impactpool_rwanda.csv"):
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved CSV -> {path}")

    def save_excel(self, df: pd.DataFrame, path: str = "impactpool_rwanda.xlsx"):
        df.to_excel(path, index=False, engine="openpyxl")
        logger.info(f"Saved Excel -> {path}")

    def save_json(self, df: pd.DataFrame, path: str = "impactpool_rwanda.json"):
        df.to_json(path, orient="records", indent=2, force_ascii=False)
        logger.info(f"Saved JSON -> {path}")


# ----------------------------------------------
# Entry point
# ----------------------------------------------

def main():
    scraper = JobScraper(
        request_delay=1.0,
        timeout=20,
        max_workers=5,
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

    scraper.save_csv(df, "impactpool_rwanda.csv")
    scraper.save_excel(df, "impactpool_rwanda.xlsx")
    scraper.save_json(df, "impactpool_rwanda.json")


if __name__ == "__main__":
    main()
