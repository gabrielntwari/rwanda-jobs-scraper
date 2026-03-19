"""
Job Scraper for unjobs.org/duty_stations/kgl  (Kigali, Rwanda)
===============================================================
Outputs a rich structured schema matching all other Rwanda Jobs scrapers.

CONFIRMED LIVE BEHAVIOUR (tested 2026-03-19):
  Page 1  GET /duty_stations/kgl   -> 200  (28 cards, 25 unique after dedup)
  Page 2+ GET /duty_stations/kgl/2 -> 403  (stop pagination immediately)
  Detail  GET /vacancies/XXXXXXX   -> 403  (no detail fetching possible)

CARD HTML (confirmed structure):
  <div class="job">
    <a class="jtitle" href="https://unjobs.org/vacancies/XXXXXXX">
      Job Title[, Grade][, Type], Kigali, Rwanda[, #RefNum]
    </a>
    <br>
    Organization[ | Deadline: DD Month YYYY]
    OR
    Organization[ | Closing Date: MM/DD/YYYY]
    OR
    Organization[ | Application close: Mon DD YYYY]
  </div>

FIXES IN THIS VERSION vs previous:
  1. Deadline extracted from <br> sibling "Org | Deadline: ..." pattern
  2. Employment type defaults to "Contract" for unjobs (UN fixed-term is standard)
     instead of blank - only overridden when title explicitly says Consultant/etc.
  3. Grade stripping from title: "Programme Officer HACT, NO-2, Fixed Term Position"
     -> title="Programme Officer HACT", grade="NO-2", type="Contract"
  4. Unicode fix: logger uses ASCII separators (no cp1252 crash on Windows)
"""

import re
import sys
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# ----------------------------------------------
# Logging  (ASCII separators - safe on Windows cp1252)
# ----------------------------------------------
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_fh  = logging.FileHandler("job_scraper.log", encoding="utf-8")
_sh  = logging.StreamHandler()
_fh.setFormatter(_fmt)
_sh.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=[_fh, _sh])
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
    "Contract":    ["contract", "fixed-term", "fixed term", "fixed term position", "temporary"],
    "Internship":  ["intern", "internship", "attachment", "apprentice"],
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

# UN grade codes embedded in jtitle text
UN_GRADE_RE  = re.compile(
    r"\b(P[1-6]|G[1-7]|D[12]|NO-?[A-D1-4]|ASG|USG|FS-\d)\b", re.IGNORECASE
)
UN_GRADE_LEVEL = {
    r"\bP[12]\b|\bG[1-5]\b|\bNO-?[AB12]\b": "Entry",
    r"\bP3\b|\bNO-?[CD34]\b":               "Mid",
    r"\bP[456]\b":                           "Senior",
    r"\bD[12]\b|\bASG\b|\bUSG\b":           "Executive",
}

# Deadline patterns in the <br> sibling text
DEADLINE_RE = re.compile(
    r"(?:deadline|closing date|application close|apply by|closes?)[:\s]+"
    r"(.+?)(?:\s*\||$)",
    re.IGNORECASE
)

# Tokens to strip from the end of jtitle text
_STRIP_SUFFIXES = [
    r",\s*#\d+\s*$",                          # job ref numbers: ", #00107210"
    r",\s*kigali\s*,\s*rwanda\s*$",           # ", Kigali, Rwanda"
    r",\s*rwanda\s*$",                         # ", Rwanda"
    r",\s*kigali\s*$",                         # ", Kigali"
]


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
    Short keywords (<=4 chars) use word-boundary matching to avoid
    false positives like 'cto' inside 'contractor'.
    """
    def best_match(haystack: str) -> Optional[str]:
        h = haystack.lower()
        best_cat, best_len = None, 0
        for category, keywords in keyword_map.items():
            for kw in sorted(keywords, key=len, reverse=True):
                if len(kw) <= 4:
                    # Word-boundary match for short tokens (ceo, cfo, cto, vp, etc.)
                    matched = bool(re.search(rf"\b{re.escape(kw)}\b", h))
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
            "eligibility_reason": "Listed on unjobs.org Kigali duty station",
            "confidence_score": 5}


def grade_to_level(text: str) -> str:
    """Map UN grade string to job level. e.g. 'NO-2' -> 'Entry', 'P4' -> 'Senior'."""
    if not text:
        return ""
    for pattern, level in UN_GRADE_LEVEL.items():
        if re.search(pattern, text, re.IGNORECASE):
            return level
    return ""


def infer_employment_type(raw_text: str) -> str:
    """
    Infer employment type from jtitle raw text.
    unjobs Kigali is almost entirely UN agency fixed-term positions,
    so "Contract" is the correct default when no specific keyword is found.
    """
    t = raw_text.lower()
    # Check explicit keywords in priority order
    if any(k in t for k in ["individual contractor", "consultancy", "consultant", "freelance"]):
        return "Consultancy"
    if any(k in t for k in ["volunteer", "unv", "un volunteer", "voluntary"]):
        return "Volunteer"
    if any(k in t for k in ["intern", "internship", "attachment", "apprentice"]):
        return "Internship"
    if any(k in t for k in ["part-time", "part time"]):
        return "Part-time"
    if any(k in t for k in ["permanent", "indefinite", "full-time", "full time"]):
        return "Full-time"
    # Default: all remaining unjobs Kigali listings are UN fixed-term contracts
    return "Contract"


def parse_br_sibling(sibling_text: str) -> Dict[str, str]:
    """
    Parse the NavigableString after <br> in a card.
    Format variants confirmed from live data:
      "UNICEF"
      "WFP | Deadline: 7 January 2026"
      "World Bank Group | Closing Date: 1/26/2026"
      "UNDP | Application close: Mar 16 2026"
    Returns: {"company": "...", "deadline": "..."}
    """
    if not sibling_text:
        return {"company": "", "deadline": ""}

    parts   = sibling_text.split("|", 1)
    company = clean(parts[0]) or ""
    rest    = parts[1].strip() if len(parts) > 1 else ""

    deadline = ""
    if rest:
        m = DEADLINE_RE.search(rest)
        if m:
            deadline = clean(m.group(1)) or ""

    return {"company": company, "deadline": deadline}


def strip_title(raw_text: str, company: str, grade: str) -> str:
    """
    Remove location suffixes, job refs, grade tokens, type tokens,
    and company name from the jtitle raw text to get a clean title.
    """
    title = raw_text

    # Remove job reference numbers: ", #00107210"
    title = re.sub(r",\s*#\d+\s*$", "", title).strip()

    # Remove location suffixes (case-insensitive, from end)
    for pat in _STRIP_SUFFIXES:
        title = re.sub(pat, "", title, flags=re.IGNORECASE).strip()

    # Remove company name if it appears at the end of the title
    if company:
        title = re.sub(rf",\s*{re.escape(company)}\s*$", "", title,
                       flags=re.IGNORECASE).strip()

    # Remove UN grade token: ", NO-2" / ", P4" etc.
    if grade:
        title = re.sub(rf",\s*{re.escape(grade)}\b", "", title,
                       flags=re.IGNORECASE).strip()

    # Remove common employment type tokens embedded in title
    for tok in ["Fixed Term Position", "Fixed Term Appointment", "Fixed-Term",
                "Temporary Job Opening", "Individual Contractor"]:
        title = re.sub(rf",\s*{re.escape(tok)}\s*$", "", title,
                       flags=re.IGNORECASE).strip()

    return clean(title) or raw_text


# ----------------------------------------------
# Scraper class
# ----------------------------------------------

class JobScraper:
    BASE_URL     = "https://unjobs.org"
    LISTING_PATH = "/duty_stations/kgl"
    SOURCE_NAME  = "unjobs"

    def __init__(self, request_delay: float = 3.0,
                 timeout: int = 20,
                 max_workers: int = 1,
                 max_pages: int = 10):
        self.request_delay = request_delay
        self.timeout       = timeout
        self.max_workers   = max_workers   # always 1 - concurrent = instant 403
        self.max_pages     = max_pages
        self.session       = self._build_session()

    # ------------------------------------------------------------------
    # Network helpers
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=2,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
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
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language":         "en-US,en;q=0.9",
            "Accept-Encoding":         "gzip, deflate, br",
            "Connection":              "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "DNT":                     "1",
        })
        return session

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        time.sleep(self.request_delay)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"GET failed [{url}]: {e}")
            return None
        except requests.RequestException as e:
            logger.warning(f"GET failed [{url}]: {e}")
            return None

    # ------------------------------------------------------------------
    # Paginated listing scrape
    # Page 1 = 200 (28 cards confirmed). Page 2+ = 403 (stop immediately).
    # ------------------------------------------------------------------

    def _get_all_stubs(self) -> List[Dict]:
        all_stubs: List[Dict] = []
        base = self.BASE_URL + self.LISTING_PATH

        for page in range(1, self.max_pages + 1):
            url = base if page == 1 else f"{base}/{page}"
            logger.info(f"  Scraping listing page {page}: {url}")

            soup = self._get(url)
            if not soup:
                logger.info(f"  Page {page} blocked or failed - stopping.")
                break

            cards = soup.find_all("div", class_="job")
            if not cards:
                logger.info(f"  Page {page}: 0 cards - end of listings.")
                break

            logger.info(f"  Page {page}: {len(cards)} job cards found.")
            for card in cards:
                stub = self._parse_card(card)
                if stub:
                    all_stubs.append(stub)

        return all_stubs

    # ------------------------------------------------------------------
    # Parse a single <div class="job"> card.
    #
    # Extracts from jtitle text:
    #   - raw title tokens
    #   - UN grade (NO-2, P4, etc.)
    #   - employment type keywords
    #
    # Extracts from <br> sibling:
    #   - company name  (before "|")
    #   - deadline      (after "Deadline:" / "Closing Date:" / etc.)
    # ------------------------------------------------------------------

    def _parse_card(self, card) -> Optional[Dict]:
        a_tag = card.find("a", class_="jtitle")
        if not a_tag:
            return None

        raw_text   = clean(a_tag.get_text()) or ""
        href       = a_tag.get("href", "")
        source_url = href if href.startswith("http") else urljoin(self.BASE_URL, href)
        source_job_id = urlparse(source_url).path.rstrip("/").split("/")[-1]

        # -- <br> sibling: company + deadline ------------------------
        br  = a_tag.find_next("br")
        sib = ""
        if br and br.next_sibling and isinstance(br.next_sibling, NavigableString):
            sib = clean(str(br.next_sibling)) or ""
        parsed = parse_br_sibling(sib)
        company  = parsed["company"]
        deadline = parsed["deadline"]

        # -- UN grade from jtitle -------------------------------------
        grade_m = UN_GRADE_RE.search(raw_text)
        grade   = grade_m.group(1).upper() if grade_m else ""

        # -- Clean title ----------------------------------------------
        title = strip_title(raw_text, company, grade)

        # -- Employment type ------------------------------------------
        employment_type = infer_employment_type(raw_text)

        if not title:
            return None

        return {
            "title":           title,
            "company":         company,
            "raw_text":        raw_text,      # used for classification
            "grade":           grade,
            "source_url":      source_url,
            "source_job_id":   source_job_id,
            "location_raw":    "Kigali, Rwanda",
            "employment_type": employment_type,
            "posted_date":     "",
            "deadline":        deadline,
        }

    # ------------------------------------------------------------------
    # Build the final 30-column record from a stub.
    # No detail pages - all fields derived from card data.
    # ------------------------------------------------------------------

    def _build_record(self, stub: Dict) -> Dict:
        title        = stub.get("title") or ""
        company      = stub.get("company") or ""
        raw_text     = stub.get("raw_text") or ""
        grade        = stub.get("grade") or ""
        location_raw = stub.get("location_raw") or "Kigali, Rwanda"
        description  = raw_text   # best available without detail pages

        combo = f"{title} {raw_text} {company}".strip()

        loc = infer_location(location_raw)

        sector = (
            infer_field(combo, SECTOR_KEYWORDS, title=title)
            or "NGO"          # unjobs Kigali = exclusively UN/INGO
        )

        # Grade-based level takes priority over keyword inference
        job_level = (
            grade_to_level(grade)
            or grade_to_level(raw_text)
            or infer_field(combo, JOB_LEVEL_KEYWORDS, title=title)
            or ""
        )

        education_level = infer_field(combo, EDUCATION_KEYWORDS) or ""
        exp_years       = extract_experience_years(combo)
        salary_info     = extract_salary(combo)
        source_url      = stub.get("source_url", "")

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
            "employment_type":  stub.get("employment_type") or "",
            "education_level":  education_level,

            # 5. SALARY
            "salary_min":       salary_info["salary_min"],
            "salary_max":       salary_info["salary_max"],
            "currency":         salary_info["currency"],
            "salary_disclosed": salary_info["salary_disclosed"],

            # 6. DATES
            "posted_date": stub.get("posted_date") or "",
            "deadline":    stub.get("deadline") or "",
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
        logger.info("Starting scrape - unjobs.org/duty_stations/kgl")
        t0 = time.time()

        all_stubs = self._get_all_stubs()
        if not all_stubs:
            logger.error("No stubs found - aborting.")
            return pd.DataFrame()

        # Deduplicate by source URL
        seen: set = set()
        unique_stubs = []
        for s in all_stubs:
            url = s.get("source_url")
            if url and url not in seen:
                seen.add(url)
                unique_stubs.append(s)
        logger.info(f"Unique jobs after dedup: {len(unique_stubs)}")

        records = [self._build_record(s) for s in unique_stubs]

        df = pd.DataFrame(records)
        elapsed = time.time() - t0
        logger.info(f"Scrape complete in {elapsed:.1f}s - {len(df)} jobs")
        logger.info("=" * 50)
        return df

    # ------------------------------------------------------------------
    # Save helpers  (identical interface to all other scrapers)
    # ------------------------------------------------------------------

    def save_csv(self, df: pd.DataFrame, path: str = "unjobs_kigali.csv"):
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved CSV -> {path}")

    def save_excel(self, df: pd.DataFrame, path: str = "unjobs_kigali.xlsx"):
        df.to_excel(path, index=False, engine="openpyxl")
        logger.info(f"Saved Excel -> {path}")

    def save_json(self, df: pd.DataFrame, path: str = "unjobs_kigali.json"):
        df.to_json(path, orient="records", indent=2, force_ascii=False)
        logger.info(f"Saved JSON -> {path}")


# ----------------------------------------------
# Entry point
# ----------------------------------------------

def main():
    scraper = JobScraper(
        request_delay=3.0,
        timeout=20,
        max_workers=1,
        max_pages=10,
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
    for etype, count in df["employment_type"].value_counts().head(6).items():
        print(f"    {etype:<20} {count}")
    print("=" * 55)

    scraper.save_csv(df, "unjobs_kigali.csv")
    scraper.save_excel(df, "unjobs_kigali.xlsx")
    scraper.save_json(df, "unjobs_kigali.json")


if __name__ == "__main__":
    main()
