"""
Job Scraper for mucuruzi.com - ENHANCED WITH SMART POSITION SPLITTER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✨ NEW: Automatically splits multi-position announcements into individual jobs!
  - "13 Positions at RSSB" → Creates 13 separate database rows
  - "15 CIP Data Collectors at NISR" → Creates 15 separate database rows
  - Works with ANY format - no hardcoding!

Site structure (confirmed):
  - REST API     → 404 (disabled)
  - /all-jobs/   → JS-rendered, unscrapable
  - /category/job/page/N/  → paginated blog archive ✓ PRIMARY SOURCE
  - /jobs/slug/  → individual WP Job Manager posts  ✓
  - "Trending" roundup posts → contain raw URLs to jobs ✓ BONUS SOURCE

Strategy:
  1. Walk /category/job/page/1/ … /page/N/ collecting all <article> links
  2. For each post, check if it's a "roundup" (title starts with a number +
     "Job Positions Trending") — if so, extract embedded URLs from the body
     and add those as extra job links to fetch
  3. Fetch every individual job post and parse title + content
  4. Build the structured schema record for each
  5. ✨ NEW: Intelligently detect and split multi-position announcements
"""

import re
import time
import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed


# ── Logging ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("mucuruzi_scraper.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

BASE_URL = "https://mucuruzi.com"

# All archive entry points with pagination
ARCHIVE_URLS = [
    f"{BASE_URL}/category/job/",
    f"{BASE_URL}/category/opportunities/",
    f"{BASE_URL}/category/scholarships/",   # optional — remove if not needed
]

# Roundup posts (lists of job links) — mucuruzi publishes these regularly
# We also discover them automatically during crawl
KNOWN_ROUNDUP_PATTERNS = [
    r"job.positions.trending",
    r"\d+.job.positions.trending",
    r"trending.on.mucuruzi",
]


# ── Lookup tables ─────────────────────────────────────────────────────

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
    "Internship":  ["intern", "attachment", "apprentice"],
    "Consultancy": ["consultant", "consultancy", "freelance", "terms of reference", "tor"],
    "Volunteer":   ["volunteer", "voluntary"],
}

EDUCATION_KEYWORDS = {
    "PhD":         ["phd", "doctorate", "doctoral"],
    "Master's":    ["master", "msc", "mba", "ma ", "m.sc", "m.a."],
    "Bachelor's":  ["bachelor", "bsc", "ba ", "b.sc", "b.a.", "degree",
                    "undergraduate", "university", "a0"],
    "Diploma":     ["diploma", "a-level", "advanced level", "hnd", "a1", "a2"],
    "Certificate": ["certificate", "o-level", "tvet", "vocational"],
}

CURRENCY_SYMBOLS = {
    "RWF": ["rwf", "frw", "francs rwandais"],
    "USD": ["usd", "$", "dollars"],
    "EUR": ["eur", "euros"],
}


# ── Pure helpers ──────────────────────────────────────────────────────

def clean(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    return " ".join(text.split()).strip() or None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def infer_field(text: str, keyword_map: Dict[str, List[str]],
                title: str = "") -> Optional[str]:
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
    if not text:
        return ""
    t = text.lower()
    if re.search(r"no\s+(prior\s+)?experience", t):
        return "0"
    m = re.search(r"(\d+)\s*(?:to|-|–|—)\s*(\d+)\s*(?:years?|yrs?)", t)
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
    result = {"salary_min": None, "salary_max": None,
              "currency": "", "salary_disclosed": False}
    if not text:
        return result
    t = text.lower()
    for currency, symbols in CURRENCY_SYMBOLS.items():
        if any(s in t for s in symbols):
            result["currency"] = currency
            break
    m = re.search(r"(\d[\d,]*)\s*[-–to]+\s*(\d[\d,]*)", text.replace(",", ""))
    if m:
        try:
            result["salary_min"] = float(m.group(1))
            result["salary_max"] = float(m.group(2))
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
    if "remote" in t:
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
                "eligibility_reason": "Remote — open globally",
                "confidence_score": 4}

    for d in ["us citizens only", "eu residents only", "authorized to work in the us"]:
        if d in text:
            return {"rwanda_eligible": False,
                    "eligibility_reason": f"Disqualifying clause: '{d}'",
                    "confidence_score": 5}

    for sig in ["rwanda", "kigali", "rwandan"]:
        if sig in text:
            return {"rwanda_eligible": True,
                    "eligibility_reason": "Explicitly mentions Rwanda",
                    "confidence_score": 5}

    return {"rwanda_eligible": True,
            "eligibility_reason": "Listed on mucuruzi.com",
            "confidence_score": 3}


def parse_post_title(raw_title: str) -> Dict[str, str]:
    """
    Decompose mucuruzi post title:
      "Accountant at RDB: (Deadline 28 Feb 2026)"
      "10 Job Positions at NISR: (Deadline Ongoing)"
      "5 Positions of Nurse at CHUK: (Deadline 1 March 2026)"
    """
    result = {"title": raw_title, "company": "", "deadline": "", "positions_count": ""}
    if not raw_title:
        return result

    # Extract deadline
    dl = re.search(r"\(Deadline[:\s]+(.+?)\)", raw_title, re.IGNORECASE)
    if dl:
        result["deadline"] = dl.group(1).strip()
    core = re.sub(r"\s*:?\s*\(Deadline.+?\)", "", raw_title, flags=re.IGNORECASE).strip()
    # Also strip trailing colon
    core = core.rstrip(":").strip()

    # "X Positions of <role> at <company>"
    m = re.match(r"^(\d+)\s+(?:job\s+)?positions?\s+(?:of\s+(.+?)\s+)?at\s+(.+)$",
                 core, re.IGNORECASE)
    if m:
        result["positions_count"] = m.group(1)
        role = m.group(2)
        result["company"] = clean(m.group(3)) or ""
        # Keep the number in title for splitter detection!
        if role:
            result["title"] = clean(role)
        else:
            result["title"] = f"{m.group(1)} Positions at {clean(m.group(3))}"
        return result

    # "Role at Company"
    m = re.match(r"^(.+?)\s+at\s+(.+)$", core, re.IGNORECASE)
    if m:
        result["title"] = clean(m.group(1)) or raw_title
        result["company"] = clean(m.group(2)) or ""
        return result

    result["title"] = clean(core) or raw_title
    return result


def extract_deadline_from_text(text: str) -> str:
    """
    Enhanced deadline extraction that searches for deadlines in job description.
    
    Patterns it catches:
    - "Deadline: 15 March 2026"
    - "Application deadline: March 15, 2026"
    - "Closing date: 15/03/2026"
    - "Applications close on 15-03-2026"
    - "Apply before 15 March 2026"
    - "Deadline is 15th March 2026"
    """
    if not text:
        return ""
    
    # Patterns to search for
    deadline_patterns = [
        # "Deadline: 15 March 2026"
        r"deadline[:\s]+(\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4})",
        
        # "Application deadline: March 15, 2026"
        r"(?:application|submission)\s+deadline[:\s]+((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{1,2},?\s+\d{4})",
        
        # "Closing date: 15/03/2026" or "15-03-2026"
        r"(?:closing|due)\s+date[:\s]+(\d{1,2}[-/]\d{1,2}[-/]\d{4})",
        
        # "Applications close on 15 March 2026"
        r"applications?\s+close\s+(?:on|by)[:\s]+(\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4})",
        
        # "Apply before 15 March 2026"
        r"apply\s+(?:before|by)[:\s]+(\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4})",
        
        # "Deadline is 15th March 2026"
        r"deadline\s+is[:\s]+(\d{1,2}(?:st|nd|rd|th)?\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{4})",
        
        # Just a standalone date at the end "Deadline: 15/03/2026"
        r"deadline[:\s]+(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})",
    ]
    
    for pattern in deadline_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            deadline_str = match.group(1).strip()
            
            # Normalize the date string - remove ordinal suffixes (1st, 2nd, 3rd, 4th)
            deadline_str = re.sub(r'(\d+)(?:st|nd|rd|th)', r'\1', deadline_str)
            
            return deadline_str
    
    # Check for "Ongoing" or "Continuous" deadlines
    if re.search(r'deadline[:\s]*(?:ongoing|continuous|rolling|open)', text, re.IGNORECASE):
        return "Ongoing"
    
    return ""


def is_roundup_post(title: str, url: str) -> bool:
    """True if this post is a 'Trending jobs' roundup containing a list of URLs."""
    combined = (title + " " + url).lower()
    return any(re.search(p, combined) for p in KNOWN_ROUNDUP_PATTERNS)


def is_real_job_url(url: str) -> bool:
    """
    Filter out non-job URLs (scholarships, tenders, category pages, etc.)
    Keep only job post URLs.
    """
    if not url.startswith(BASE_URL):
        return False
    path = urlparse(url).path.lower()
    # Skip pagination, categories, tags, admin, feed pages
    skip = ["/page/", "/category/", "/tag/", "/wp-", "/feed", "?", "#",
            "/scholarships/", "/tenders/", "/tender-"]
    if any(s in path for s in skip):
        return False
    # Must have a non-trivial slug (more than just the domain)
    parts = [p for p in path.strip("/").split("/") if p]
    return len(parts) >= 1


# ── Smart Position Splitter ──────────────────────────────────────────

class SmartPositionSplitter:
    """
    Intelligently detects and splits multi-position announcements.
    No hardcoded patterns - adapts to ANY format!
    """
    
    def __init__(self):
        # Job title validation keywords
        self.job_keywords = {
            'manager', 'officer', 'specialist', 'coordinator', 'assistant',
            'director', 'head', 'chief', 'senior', 'junior', 'accountant',
            'engineer', 'developer', 'analyst', 'consultant', 'technician',
            'supervisor', 'administrator', 'clerk', 'secretary', 'receptionist',
            'teacher', 'lecturer', 'professor', 'nurse', 'doctor', 'driver',
            'designer', 'architect', 'lawyer', 'auditor', 'planner', 'adviser',
            'advisor', 'expert', 'intern', 'trainee', 'collector', 'agent',
            'representative', 'cashier', 'teller', 'guard', 'security',
        }
        
        self.list_indicators = [
            'following positions', 'following vacancies', 'positions are',
            'vacancies are', 'hiring for', 'looking for', 'positions:', 'jobs:',
        ]
    
    def is_multi_position(self, title: str, description: str) -> tuple:
        """Check if announcement is multi-position. Returns (is_multi, count)"""
        # Check title for number
        match = re.search(r'(\d+)\s+(?:job\s+)?(?:positions?|vacancies|jobs)', 
                         title.lower())
        if match:
            count = int(match.group(1))
            if count > 1:
                return (True, count)
        
        # Check for "multiple", "several"
        if any(w in title.lower() for w in ['multiple', 'several', 'various']):
            return (True, 0)
        
        return (False, 1)
    
    def extract_positions(self, description: str, title: str, company: str) -> List[Dict]:
        """Extract individual positions using multiple methods"""
        results = []
        
        # Try 4 extraction methods
        numbered = self._extract_numbered(description)
        if numbered:
            results.append(('numbered', numbered))
        
        bulleted = self._extract_bulleted(description)
        if bulleted:
            results.append(('bulleted', bulleted))
        
        newline = self._extract_newline(description)
        if newline:
            results.append(('newline', newline))
        
        comma = self._extract_comma_separated(description)
        if comma:
            results.append(('comma', comma))
        
        # Pick best result
        if not results:
            return []
        
        method, positions = max(results, key=lambda x: len(x[1]))
        
        return [{
            'title': pos,
            'company': company,
            'extraction_method': method
        } for pos in positions]
    
    def _extract_numbered(self, text: str) -> List[str]:
        """Extract from numbered lists: 1. Position, 2. Position"""
        positions = []
        pattern = r'^\s*(\d+)[\.\)]\s*(.+?)$'
        
        for line in text.split('\n'):
            match = re.match(pattern, line.strip())
            if match:
                pos = match.group(2).strip()
                if self._is_valid_title(pos):
                    positions.append(pos)
        return positions
    
    def _extract_bulleted(self, text: str) -> List[str]:
        """Extract from bulleted lists: • Position, - Position"""
        positions = []
        pattern = r'^\s*[•\-\*▪○]\s*(.+?)$'
        
        for line in text.split('\n'):
            match = re.match(pattern, line.strip())
            if match:
                pos = match.group(1).strip()
                if self._is_valid_title(pos):
                    positions.append(pos)
        return positions
    
    def _extract_newline(self, text: str) -> List[str]:
        """Extract positions on separate lines"""
        positions = []
        
        # NEW: Special pattern for Mucuruzi format
        # "Position Title at Company Name: (Deadline Date)"
        # Make it case-insensitive and flexible
        mucuruzi_pattern = r'^(.+?)\s+at\s+.+?:\s*\(deadline'
        
        for line in text.split('\n'):
            line_stripped = line.strip()
            if not line_stripped or len(line_stripped) < 10:
                continue
            
            # Check for Mucuruzi-style job title (case-insensitive)
            match = re.match(mucuruzi_pattern, line_stripped, re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                # Validate it's a real job title
                if self._is_valid_title(title) and 5 < len(title) < 150:
                    # Avoid duplicates
                    if title not in positions:
                        positions.append(title)
                    continue
        
        # If we found positions with Mucuruzi pattern, return them
        if positions:
            return positions
        
        # Otherwise, fall back to original logic
        lines = text.split('\n')
        
        # Find start of list
        start = 0
        for i, line in enumerate(lines):
            if any(ind in line.lower() for ind in self.list_indicators):
                start = i + 1
                break
        
        # Extract titles
        consecutive = 0
        for i in range(start, min(start + 50, len(lines))):
            line = lines[i].strip()
            
            if not line:
                if consecutive >= 2:
                    break
                continue
            
            if self._is_valid_title(line) and len(line) < 150:
                if not (line.endswith('.') and len(line.split()) > 15):
                    positions.append(line)
                    consecutive += 1
            else:
                if consecutive >= 2:
                    break
                consecutive = 0
        
        return positions
    
    def _extract_comma_separated(self, text: str) -> List[str]:
        """Extract from comma-separated list in single line"""
        positions = []
        
        for line in text.split('\n'):
            line = line.strip()
            if 20 < len(line) < 500 and ',' in line:
                parts = [p.strip() for p in line.split(',')]
                valid = [p for p in parts if self._is_valid_title(p) and len(p) < 100]
                
                if len(valid) >= 3 and len(valid) / len(parts) > 0.5:
                    positions.extend(valid)
        
        return positions
    
    def _is_valid_title(self, text: str) -> bool:
        """Validate if text looks like a job title"""
        if not text or len(text) < 5:
            return False
        
        text_lower = text.lower()
        
        # Must have job keyword
        if not any(kw in text_lower for kw in self.job_keywords):
            return False
        
        # Skip full sentences
        if text.endswith('.') and len(text.split()) > 15:
            return False
        
        # Skip common non-titles
        skip = ['deadline', 'apply', 'click here', 'contact', 'email',
                'requirements', 'qualifications', 'how to apply']
        if any(s in text_lower for s in skip):
            return False
        
        return True


# ── Scraper ───────────────────────────────────────────────────────────

class MucuruziScraper:
    SOURCE_NAME = "mucuruzi"

    def __init__(self,
                 request_delay: float = 0.5,
                 timeout: int = 20,
                 max_workers: int = 6,
                 max_archive_pages: int = 100):
        """
        Args:
            request_delay:     Seconds to wait between requests (per thread).
            timeout:           HTTP request timeout in seconds.
            max_workers:       Number of concurrent threads for post fetching.
            max_archive_pages: Pagination cap per archive URL.
        """
        self.request_delay = request_delay
        self.timeout = timeout
        self.max_workers = max_workers
        self.max_archive_pages = max_archive_pages
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=4,
            backoff_factor=1,
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
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
        })
        return session

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        time.sleep(self.request_delay)
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except requests.HTTPError as e:
            logger.warning(f"HTTP {e.response.status_code} -> {url}")
            return None
        except Exception as e:
            logger.warning(f"Request failed [{url}]: {e}")
            return None

    # ── Step 1: Collect all post URLs ─────────────────────────────────

    def _collect_all_post_urls(self) -> Set[str]:
        """
        Walk all archive pages and collect post URLs.
        Also identifies roundup posts to extract bonus job links.
        """
        post_urls: Set[str] = set()
        roundup_urls: Set[str] = set()

        for archive_url in ARCHIVE_URLS:
            logger.info(f"Archive: {archive_url}")

            for page_num in range(1, self.max_archive_pages + 1):
                page_url = (archive_url if page_num == 1
                            else archive_url.rstrip("/") + f"/page/{page_num}/")

                soup = self._get(page_url)
                if not soup:
                    logger.info(f"  Stopped at page {page_num} (no response)")
                    break

                found = self._extract_post_stubs(soup)
                if not found:
                    logger.info(f"  Stopped at page {page_num} (no articles)")
                    break

                for url, title in found:
                    if is_roundup_post(title, url):
                        roundup_urls.add(url)
                    else:
                        if is_real_job_url(url):
                            post_urls.add(url)

                logger.info(f"  Page {page_num}: {len(found)} posts "
                            f"(total so far: {len(post_urls)}, roundups: {len(roundup_urls)})")

                # Stop if WordPress returns no "next page" link
                if not soup.select_one("a.next.page-numbers, a[rel='next']"):
                    break

        # Step 1b: extract bonus job links from roundup posts
        if roundup_urls:
            logger.info(f"Processing {len(roundup_urls)} roundup posts for extra links...")
            bonus = self._extract_links_from_roundups(roundup_urls)
            logger.info(f"  Found {len(bonus)} extra job URLs from roundups")
            post_urls.update(bonus)

        logger.info(f"Total unique job URLs collected: {len(post_urls)}")
        return post_urls

    def _extract_post_stubs(self, soup: BeautifulSoup) -> List[tuple]:
        """
        Extract (url, title) tuples from an archive page.
        Handles both <article> and plain <h2>/<h3> link patterns.
        """
        stubs = []

        # Primary: WordPress <article> elements
        for article in soup.select("article"):
            a = (article.select_one("h2.entry-title a, h3.entry-title a, h1.entry-title a")
                 or article.select_one("a[rel='bookmark']")
                 or article.select_one("a[href*='mucuruzi.com']"))
            if a:
                href = a.get("href", "")
                title = clean(a.get_text()) or ""
                if href.startswith(BASE_URL):
                    stubs.append((href, title))

        # Fallback: h2/h3 links in main content if no articles found
        if not stubs:
            main = soup.select_one("main, div#main, div#content, div.site-content")
            if main:
                for a in main.select("h2 a[href], h3 a[href]"):
                    href = a.get("href", "")
                    title = clean(a.get_text()) or ""
                    if href.startswith(BASE_URL):
                        stubs.append((href, title))

        return stubs

    def _extract_links_from_roundups(self, roundup_urls: Set[str]) -> Set[str]:
        """
        Roundup posts contain raw mucuruzi.com URLs as text, like:
          https://mucuruzi.com/accountant-at-rdb-deadline-28-feb-2026/
        Extract them all.
        """
        bonus_links: Set[str] = set()

        for url in roundup_urls:
            soup = self._get(url)
            if not soup:
                continue

            content = soup.select_one(
                "div.entry-content, div.post-content, div[class*='entry-content']"
            )
            if not content:
                continue

            # Find all <a href> links pointing back to mucuruzi.com
            for a in content.select("a[href]"):
                href = a["href"]
                if is_real_job_url(href):
                    bonus_links.add(href)

            # Also find raw URLs in text (mucuruzi posts sometimes list them as plain text)
            raw_text = content.get_text()
            for m in re.finditer(r"https?://mucuruzi\.com/[^\s,\n\"'<>]+", raw_text):
                href = m.group(0).rstrip(".,/")
                # Clean up trailing commas/slashes mucuruzi sometimes adds
                href = re.sub(r"[,/]+$", "", href)
                if is_real_job_url(href):
                    bonus_links.add(href)

        return bonus_links

    # ── Step 2: Parse individual job post ────────────────────────────

    def _parse_post(self, url: str) -> Optional[Dict]:
        soup = self._get(url)
        if not soup:
            return None

        raw: Dict[str, Any] = {
            "source_url":    url,
            "source_job_id": urlparse(url).path.strip("/").split("/")[-1][:80],
        }

        try:
            # Title
            h1 = soup.select_one(
                "h1.entry-title, h1.post-title, h1[class*='title'], article h1"
            )
            raw["raw_title"] = clean(h1.get_text()) if h1 else ""

            # Published date
            time_el = soup.select_one("time[datetime], time.entry-date, time.published")
            if time_el:
                raw["posted_date"] = (time_el.get("datetime")
                                      or clean(time_el.get_text()) or "")
            else:
                meta = soup.find("meta", property="article:published_time")
                raw["posted_date"] = meta["content"] if meta else ""

            # Main content
            content_el = soup.select_one(
                "div.entry-content, div.post-content, div[class*='entry-content']"
            )
            raw["description"] = clean(content_el.get_text(" ")) if content_el else ""

            # Location — search for explicit label first
            location_raw = ""
            desc = raw["description"] or ""
            loc_m = re.search(
                r"(?:Location|Place\s+of\s+work|Work\s+station|Based\s+in)"
                r"[:\s]+([^\n\.]{3,60})",
                desc, re.IGNORECASE
            )
            if loc_m:
                location_raw = loc_m.group(1).strip()
            # Fallback: district name anywhere in first 500 chars
            if not location_raw:
                snippet = desc[:500].lower()
                for dist in RWANDA_DISTRICTS:
                    if dist in snippet:
                        location_raw = dist.capitalize()
                        break
            if not location_raw and "kigali" in desc[:500].lower():
                location_raw = "Kigali"
            raw["location_raw"] = location_raw

            # Application link — look for explicit apply anchor
            raw["application_link"] = ""
            if content_el:
                for a in content_el.select("a[href]"):
                    href = a.get("href", "")
                    text = (a.get_text() or "").lower()
                    if (any(kw in text for kw in ["apply", "submit", "click here", "application"])
                            and not href.startswith(BASE_URL)):
                        raw["application_link"] = href
                        break

        except Exception as e:
            logger.debug(f"Parse error [{url}]: {e}")

        return raw

    # ── Step 3: Build structured record ──────────────────────────────

    def _build_record(self, raw: Dict) -> Dict:
        parsed      = parse_post_title(raw.get("raw_title", ""))
        title       = parsed["title"]
        company     = parsed["company"]
        deadline    = parsed["deadline"]
        positions   = parsed["positions_count"]
        description = raw.get("description", "") or ""
        location_raw = raw.get("location_raw", "") or ""
        source_url  = raw.get("source_url", "")
        
        # ✨ NEW: If no deadline in title, search in description
        if not deadline:
            deadline = extract_deadline_from_text(description)

        loc = infer_location(location_raw)
        sal = extract_salary(description)

        sector          = infer_field(description, SECTOR_KEYWORDS, title=title) or ""
        employment_type = infer_field(description, EMPLOYMENT_TYPE_KEYWORDS, title=title) or ""
        job_level       = infer_field(description, JOB_LEVEL_KEYWORDS, title=title) or ""
        education_level = infer_field(description, EDUCATION_KEYWORDS) or ""
        exp_years       = extract_experience_years(description)

        record: Dict[str, Any] = {
            # 1. BASIC INFO
            "id":          make_hash(source_url),
            "title":       title,
            "company":     company,
            "description": description,
            # 2. LOCATION
            "location_raw": loc["location_raw"] or location_raw,
            "district":     loc["district"],
            "country":      loc["country"],
            "is_remote":    loc["is_remote"],
            "is_hybrid":    loc["is_hybrid"],
            # 3. RWANDA ELIGIBILITY
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
            "salary_min":       sal["salary_min"],
            "salary_max":       sal["salary_max"],
            "currency":         sal["currency"],
            "salary_disclosed": sal["salary_disclosed"],
            # 6. DATES
            "posted_date": raw.get("posted_date", "") or "",
            "deadline":    deadline,
            "scraped_at":  now_iso(),
            # 7. SOURCE
            "source":        self.SOURCE_NAME,
            "source_url":    source_url,
            "source_job_id": raw.get("source_job_id", ""),
            # 8. SYSTEM CONTROL
            "is_active":      True,
            "last_checked":   now_iso(),
            "duplicate_hash": make_hash(source_url),
            # Bonus
            "positions_count":  positions,
            "application_link": raw.get("application_link", ""),
        }

        record.update(infer_rwanda_eligibility(record))
        return record

    # ── Main entry point ──────────────────────────────────────────────

    def scrape(self) -> pd.DataFrame:
        logger.info("=" * 55)
        logger.info("Starting scrape -- mucuruzi.com")
        t0 = time.time()

        # Step 1: collect all unique job post URLs
        post_urls = self._collect_all_post_urls()

        if not post_urls:
            logger.error("No job URLs found. Check selectors or site availability.")
            return pd.DataFrame()

        # Step 2: fetch and parse each post concurrently
        raw_posts: List[Dict] = []
        url_list = list(post_urls)

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self._parse_post, u): u for u in url_list}
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    result = future.result()
                    if result:
                        raw_posts.append(result)
                except Exception as e:
                    logger.warning(f"Post error: {e}")
                if i % 30 == 0:
                    logger.info(f"  Parsed {i}/{len(url_list)} posts...")

        logger.info(f"Parsed {len(raw_posts)} posts successfully")

        # Step 3: build records
        records = [self._build_record(r) for r in raw_posts]
        
        # ✨ Step 3.5: Smart position splitting (NEW!)
        logger.info("Checking for multi-position announcements...")
        splitter = SmartPositionSplitter()
        split_records = []
        
        for record in records:
            title = record.get('title', '')
            company = record.get('company', '')
            description = record.get('description', '')
            
            # Check if multi-position
            is_multi, expected_count = splitter.is_multi_position(title, description)
            
            if is_multi:
                # Try to extract individual positions
                positions = splitter.extract_positions(description, title, company)
                
                if positions and len(positions) > 1:
                    logger.info(f"  Split: '{title[:50]}...' → {len(positions)} positions")
                    
                    # Create a record for each position
                    for pos in positions:
                        new_record = record.copy()
                        new_record['title'] = pos['title']
                        new_record['company'] = pos.get('company', company)
                        
                        # Update IDs to make unique
                        source_url = record.get('source_url', '')
                        unique_key = f"{source_url}_{pos['title']}"
                        new_record['id'] = make_hash(unique_key)
                        new_record['duplicate_hash'] = make_hash(f"{pos['title']}_{company}")
                        
                        # Add metadata
                        new_record['is_multi_position_split'] = True
                        new_record['original_announcement'] = title
                        new_record['extraction_method'] = pos.get('extraction_method', 'unknown')
                        
                        split_records.append(new_record)
                else:
                    # Couldn't split, keep original
                    split_records.append(record)
            else:
                # Single position, keep as is
                split_records.append(record)
        
        records = split_records
        logger.info(f"After splitting: {len(records)} individual jobs")
        
        # Step 4: deduplicate records
        seen: Set[str] = set()
        unique = []
        for r in records:
            dup_key = r.get("duplicate_hash", r.get("source_url", ""))
            if dup_key not in seen:
                seen.add(dup_key)
                unique.append(r)

        removed = len(records) - len(unique)
        if removed:
            logger.info(f"Removed {removed} duplicate records")

        df = pd.DataFrame(unique)
        logger.info(f"Done in {time.time() - t0:.1f}s -- {len(df)} jobs")
        logger.info("=" * 55)
        return df

    # ── Save helpers ──────────────────────────────────────────────────

    def save_csv(self, df: pd.DataFrame, path: str = "mucuruzi_jobs.csv"):
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved -> {path}")

    def save_excel(self, df: pd.DataFrame, path: str = "mucuruzi_jobs.xlsx"):
        df.to_excel(path, index=False, engine="openpyxl")
        logger.info(f"Saved -> {path}")

    def save_json(self, df: pd.DataFrame, path: str = "mucuruzi_jobs.json"):
        df.to_json(path, orient="records", indent=2, force_ascii=False)
        logger.info(f"Saved -> {path}")


# ── Entry point ───────────────────────────────────────────────────────

def main():
    scraper = MucuruziScraper(
        request_delay=0.5,        # seconds between requests
        timeout=20,
        max_workers=6,            # concurrent threads for post fetching
        max_archive_pages=100,    # pages per category (10 posts each ≈ 1000 jobs)
    )

    df = scraper.scrape()

    if df.empty:
        logger.warning("No jobs scraped.")
        return

    print("\n" + "=" * 55)
    print("  SCRAPE SUMMARY -- mucuruzi.com")
    print("=" * 55)
    print(f"  Total jobs          : {len(df)}")
    print(f"  Rwanda eligible     : {df['rwanda_eligible'].sum()}")
    print(f"  With deadline       : {(df['deadline'] != '').sum()}")
    print(f"  Salary disclosed    : {df['salary_disclosed'].sum()}")
    print(f"\n  Top sectors:")
    for s, c in df["sector"].value_counts().head(6).items():
        print(f"    {str(s):<25} {c}")
    print(f"\n  Top companies:")
    for s, c in df["company"].value_counts().head(6).items():
        print(f"    {str(s):<40} {c}")
    print("=" * 55)

    scraper.save_csv(df)
    scraper.save_excel(df)
    scraper.save_json(df)


if __name__ == "__main__":
    main()