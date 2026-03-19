"""
Job Scraper for rw.jobskazi.com
Outputs a rich, structured schema for each job listing.
Features: 3-tier fetching (RSS -> AJAX -> static sidebar),
          concurrent detail-page fetching, deduplication,
          Rwanda eligibility inference.
"""

import re
import time
import hashlib
import logging
import xml.etree.ElementTree as ET
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
# Constants / lookup tables  (identical to other scrapers)
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
    "Internship":  ["intern", "attachment", "apprentice"],
    "Consultancy": ["consultant", "consultancy", "freelance"],
    "Volunteer":   ["volunteer", "voluntary"],
}

EDUCATION_KEYWORDS = {
    "PhD":         ["phd", "doctorate", "doctoral"],
    "Master's":    ["master", "msc", "mba", "ma ", "m.sc", "m.a."],
    "Bachelor's":  ["bachelor", "bsc", "ba ", "b.sc", "b.a.", "degree", "undergraduate", "university"],
    "Diploma":     ["diploma", "a-level", "advanced level", "hnd"],
    "Certificate": ["certificate", "o-level", "tvet", "vocational"],
}

CURRENCY_SYMBOLS = {
    "RWF": ["rwf", "frw", "francs rwandais"],
    "USD": ["usd", "$", "dollars"],
    "EUR": ["eur", "EUR", "euros"],
}


# ----------------------------------------------
# Helper functions  (identical signatures to other scrapers)
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
    1. Match against title first (stronger signal).
    2. Fall back to full text. Longer phrases preferred.
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
    Returns normalised strings: '2', '3-5', '5+', '0', or ''.
      'at least 3 years'  -> '3+'
      '3 to 5 years'      -> '3-5'
      '2+ years'          -> '2+'
      'no experience'     -> '0'
    """
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

    for district in RWANDA_DISTRICTS:
        if district in t:
            loc["district"] = district.capitalize()
            break

    if "kigali" in t and not loc["district"]:
        loc["district"] = "Kigali"

    return loc


def infer_rwanda_eligibility(job: Dict) -> Dict:
    """
    Rule-based eligibility check.
    Returns: rwanda_eligible, eligibility_reason, confidence_score.
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

    disqualifiers = [
        "us citizens only", "eu residents only",
        "must be based in", "authorized to work in the us",
        "work permit required (not rwanda)",
    ]
    for d in disqualifiers:
        if d in text:
            return {"rwanda_eligible": False,
                    "eligibility_reason": f"Disqualifying clause: '{d}'",
                    "confidence_score": 5}

    rwanda_signals = ["rwanda", "kigali", "rwandan", "based in kigali", "based in rwanda"]
    for sig in rwanda_signals:
        if sig in text:
            return {"rwanda_eligible": True,
                    "eligibility_reason": "Explicitly mentions Rwanda location",
                    "confidence_score": 5}

    # JobsKazi is a Rwanda-only jobs board - safe default
    return {"rwanda_eligible": True,
            "eligibility_reason": "Listed on jobskazi.com",
            "confidence_score": 3}


# ----------------------------------------------
# Scraper class
# ----------------------------------------------

class JobScraper:
    BASE_URL    = "https://rw.jobskazi.com"
    SOURCE_NAME = "jobskazi"

    def __init__(self, request_delay: float = 0.5,
                 timeout: int = 15,
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
            allowed_methods=["GET", "HEAD", "POST"],
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
    # Tier 1 - RSS feed  (?feed=job_feed)
    # WP Job Manager publishes all active listings as standard RSS 2.0.
    # This is the fastest and most complete source - no JS required.
    # ------------------------------------------------------------------

    def _get_stubs_via_rss(self) -> List[Dict]:
        """
        Fetches /?feed=job_feed and parses each <item> into a stub dict.
        Each stub has: title, source_url, source_job_id, company,
                       posted_date, location_raw.
        """
        logger.info("  [Tier 1] Trying RSS feed (?feed=job_feed)...")
        url = f"{self.BASE_URL}/?feed=job_feed"
        try:
            time.sleep(self.request_delay)
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"  [Tier 1] RSS request failed: {e}")
            return []

        ct = resp.headers.get("Content-Type", "")
        if "xml" not in ct and not resp.text.strip().startswith("<?xml"):
            logger.info("  [Tier 1] Response not XML - RSS unavailable.")
            return []

        stubs = []
        try:
            root    = ET.fromstring(resp.content)
            channel = root.find("channel")
            if channel is None:
                return []

            items = channel.findall("item")
            logger.info(f"  [Tier 1] {len(items)} items in RSS feed.")

            for item in items:
                def t(tag: str) -> str:
                    el = item.find(tag)
                    return el.text.strip() if el is not None and el.text else ""

                title   = t("title")
                link    = t("link")
                pub_raw = t("pubDate")

                if not link:
                    continue

                # pubDate -> "on DD-MM-YYYY"
                posted_date = ""
                for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S +0000"):
                    try:
                        dt = datetime.strptime(pub_raw, fmt)
                        posted_date = f"on {dt.strftime('%d-%m-%Y')}"
                        break
                    except ValueError:
                        pass

                # Derive company from URL slug:
                # e.g. "world-bank-kigali-rwanda-contract-full-time-{title}-..."
                company = ""
                slug    = link.rstrip("/").split("/")[-1]
                parts   = slug.split("-kigali-rwanda-")
                if parts:
                    company = parts[0].replace("-", " ").title()

                stubs.append({
                    "title":          clean(title) or "",
                    "source_url":     link,
                    "source_job_id":  slug,
                    "company":        company,
                    "posted_date":    posted_date,
                    "location_raw":   "Kigali (Rwanda)",
                    "employment_type_raw": "Contract",
                    "deadline":       "",
                })

        except ET.ParseError as e:
            logger.warning(f"  [Tier 1] XML parse error: {e}")
            return []

        return stubs

    # ------------------------------------------------------------------
    # Tier 2 - WP Job Manager AJAX endpoint
    # POST to /wp-admin/admin-ajax.php?action=get_listings
    # Returns HTML fragment of job cards - same markup as the page grid.
    # ------------------------------------------------------------------

    def _get_stubs_via_ajax(self) -> List[Dict]:
        logger.info("  [Tier 2] Trying WP Job Manager AJAX endpoint...")
        url     = f"{self.BASE_URL}/wp-admin/admin-ajax.php"
        payload = {
            "action":          "get_listings",
            "search_keywords": "",
            "search_location": "",
            "post_type":       "job_listing",
            "per_page":        50,
            "page":            1,
            "show_pagination": True,
        }
        try:
            time.sleep(self.request_delay)
            resp = self.session.post(url, data=payload, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"  [Tier 2] AJAX failed: {e}")
            return []

        if not data.get("found_jobs"):
            logger.info("  [Tier 2] AJAX returned no jobs.")
            return []

        soup = BeautifulSoup(data.get("html", ""), "html.parser")
        return self._parse_listing_cards(soup)

    # ------------------------------------------------------------------
    # Tier 3 - Static sidebar (always available, ~4 jobs)
    # The homepage server-renders a "Latest Jobs" sidebar even without JS.
    # ------------------------------------------------------------------

    def _get_stubs_via_sidebar(self) -> List[Dict]:
        logger.info("  [Tier 3] Falling back to static sidebar...")
        soup = self._get(self.BASE_URL)
        if not soup:
            return []
        return self._parse_listing_cards(soup)

    # ------------------------------------------------------------------
    # Parse listing cards  (shared by Tier 2 and Tier 3)
    #
    # WP Job Manager card HTML:
    #   <li class="job_listing">
    #     <a href="/job/.../">
    #       <img alt="Company Name">
    #       <h3>Job Title</h3>
    #       <ul>
    #         <li>Kigali (Rwanda)</li>   <- location
    #         <li>Company Name</li>      <- company text
    #         <li>Contract</li>          <- contract type
    #         <li>Full Time</li>         <- employment type
    #       </ul>
    #     </a>
    #   </li>
    # ------------------------------------------------------------------

    def _parse_listing_cards(self, soup: BeautifulSoup) -> List[Dict]:
        # Main JS-rendered grid
        cards = soup.select("li.job_listing")
        if not cards:
            # Sidebar: any <li> with an <h3> linking to /job/
            cards = [
                li for li in soup.find_all("li")
                if li.find("h3") and li.find("a", href=re.compile(r"/job/"))
            ]

        logger.info(f"  Found {len(cards)} job cards in HTML.")
        stubs = []

        for card in cards:
            try:
                link_el = card.find("a", href=True)
                if not link_el:
                    continue

                href = link_el["href"]
                url  = href if href.startswith("http") else urljoin(self.BASE_URL, href)
                slug = urlparse(url).path.rstrip("/").split("/")[-1]

                h3    = card.find("h3")
                title = clean(h3.get_text()) if h3 else ""

                img     = card.find("img")
                company = clean(img["alt"]) if img and img.get("alt") else ""

                items           = card.select("ul li")
                location_raw    = clean(items[0].get_text()) if len(items) > 0 else "Kigali (Rwanda)"
                company_text    = clean(items[1].get_text()) if len(items) > 1 else ""
                contract_type   = clean(items[2].get_text()) if len(items) > 2 else "Contract"
                employment_type = clean(items[3].get_text()) if len(items) > 3 else "Full Time"

                if company_text:
                    company = company_text

                # Relative date ("Posted 2 months ago")
                date_el     = card.find(string=re.compile(r"posted\s+\d+|ago", re.I))
                posted_date = self._relative_to_date_str(
                    date_el.strip() if date_el else ""
                )

                if not title:
                    continue

                stubs.append({
                    "title":               title,
                    "source_url":          url,
                    "source_job_id":       slug,
                    "company":             company,
                    "posted_date":         posted_date,
                    "location_raw":        location_raw or "Kigali (Rwanda)",
                    "employment_type_raw": f"{contract_type} {employment_type}".strip(),
                    "deadline":            "",
                })

            except Exception as e:
                logger.debug(f"Card parse error: {e}")
                continue

        return stubs

    # ------------------------------------------------------------------
    # Individual job detail page
    #
    # Detail page structure (fully static HTML):
    #   <h1>Job Title</h1>
    #   <ul class="job-listing-meta">
    #     <li class="job-type">Contract / Full Time</li>
    #     <li class="location">Kigali (Rwanda)</li>
    #   </ul>
    #   <strong>Company Name</strong>
    #   <a href="maps.google.com/...">Kigali (Rwanda)</a>  <- location link
    #   <p>Posted X months ago</p>
    #   <div class="job_description">... full text ...</div>
    #   <a class="apply_button">Apply for Job</a>
    # ------------------------------------------------------------------

    def _parse_job_detail(self, job_url: str) -> Dict:
        details: Dict[str, Any] = {}
        soup = self._get(job_url)
        if not soup:
            return details

        try:
            # Full description - largest text block
            desc_div = (
                soup.find("div", class_=re.compile(r"job.description|job.content|entry-content", re.I))
                or soup.find("section", class_=re.compile(r"job", re.I))
            )
            if desc_div:
                for tag in desc_div(["script", "style"]):
                    tag.decompose()
                details["description"] = clean(desc_div.get_text(" "))

            # Company - first <strong> that isn't a button label
            for s in soup.find_all("strong"):
                text = clean(s.get_text())
                if text and len(text) < 100 and "apply" not in text.lower():
                    details["company_raw"] = text
                    break

            # Location from Google Maps link
            maps_link = soup.find("a", href=re.compile(r"maps\.google\.com"))
            if maps_link:
                details["location_raw"] = clean(maps_link.get_text())

            # Posted date ("Posted 2 months ago")
            date_text = soup.find(string=re.compile(r"posted\s+\d+", re.I))
            if date_text:
                details["posted_date_raw"] = date_text.strip()

            # Employment / contract type from meta list
            for item in soup.select(
                "ul.job-listing-meta li, .job_listing_meta li, .job-meta li"
            ):
                text = item.get_text(strip=True).lower()
                if "full time" in text or "full-time" in text:
                    details["contract_type_raw"] = "Full-time"
                elif "part time" in text or "part-time" in text:
                    details["contract_type_raw"] = "Part-time"
                elif "contract" in text:
                    details["contract_type_raw"] = "Contract"

            # Salary - scan full page text for salary context
            page_text = clean(soup.get_text(" ")) or ""
            sal_pat   = re.compile(
                r"salary|compensation|remuneration|pay|stipend", re.IGNORECASE
            )
            m = sal_pat.search(page_text)
            if m:
                snippet = page_text[max(0, m.start() - 30): m.end() + 80]
                details["salary_snippet"] = snippet

            # Deadline - search description text
            desc_text = details.get("description") or ""
            deadline  = extract_regex(
                r"deadline[:\s]+(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})", desc_text
            ) or extract_regex(
                r"not later than[^,\n]*?(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})", desc_text
            ) or extract_regex(
                r"closing date[:\s]+(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})", desc_text
            )
            if deadline:
                details["deadline"] = deadline

        except Exception as e:
            logger.debug(f"Detail parse error [{job_url}]: {e}")

        return details

    # ------------------------------------------------------------------
    # Build final structured record  (mirrors _build_record in other scrapers)
    # ------------------------------------------------------------------

    def _build_record(self, stub: Dict, detail: Dict) -> Dict:
        title        = stub.get("title") or ""
        company      = detail.get("company_raw") or stub.get("company") or ""
        description  = detail.get("description") or ""
        contract_raw = (
            detail.get("contract_type_raw")
            or stub.get("employment_type_raw")
            or ""
        )
        location_raw = detail.get("location_raw") or stub.get("location_raw") or ""
        combo        = f"{title} {description} {contract_raw}".strip()

        # Location
        loc = infer_location(location_raw)

        # Salary
        salary_info = extract_salary(detail.get("salary_snippet") or "")

        # Posted date - prefer detail page parse (absolute), fall back to stub (relative)
        posted_date = ""
        if detail.get("posted_date_raw"):
            posted_date = self._relative_to_date_str(detail["posted_date_raw"])
        if not posted_date:
            posted_date = stub.get("posted_date") or ""

        # Deadline - detail page wins, stub fallback
        deadline = detail.get("deadline") or stub.get("deadline") or ""

        # Classification - title gets priority via infer_field
        sector          = infer_field(combo, SECTOR_KEYWORDS, title=title)          or ""
        employment_type = (
            infer_field(contract_raw, EMPLOYMENT_TYPE_KEYWORDS, title=title)
            or infer_field(combo, EMPLOYMENT_TYPE_KEYWORDS)
            or clean(contract_raw)
            or ""
        )
        job_level       = infer_field(combo, JOB_LEVEL_KEYWORDS, title=title)       or ""
        education_level = infer_field(description, EDUCATION_KEYWORDS)              or ""
        exp_years       = extract_experience_years(description)

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
            "posted_date": posted_date,
            "deadline":    deadline,
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

        # Rwanda eligibility
        eligibility = infer_rwanda_eligibility(record)
        record.update(eligibility)

        return record

    # ------------------------------------------------------------------
    # Public scrape method
    # ------------------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        logger.info("=" * 50)
        logger.info("Starting scrape - rw.jobskazi.com")
        t0 = time.time()

        # Step 1: collect stubs via 3-tier fallback
        stubs = self._get_stubs_via_rss()
        if stubs:
            logger.info(f"Tier 1 (RSS) -> {len(stubs)} stubs")
        else:
            stubs = self._get_stubs_via_ajax()
            if stubs:
                logger.info(f"Tier 2 (AJAX) -> {len(stubs)} stubs")
            else:
                stubs = self._get_stubs_via_sidebar()
                logger.info(f"Tier 3 (Sidebar) -> {len(stubs)} stubs")

        if not stubs:
            logger.error("No stubs found across all tiers - aborting.")
            return pd.DataFrame()

        # Step 2: deduplicate by URL
        seen: set = set()
        unique_stubs = []
        for s in stubs:
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
    # Save helpers  (identical interface to other scrapers)
    # ------------------------------------------------------------------

    def save_csv(self, df: pd.DataFrame, path: str = "jobskazi_jobs.csv"):
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved CSV -> {path}")

    def save_excel(self, df: pd.DataFrame, path: str = "jobskazi_jobs.xlsx"):
        df.to_excel(path, index=False, engine="openpyxl")
        logger.info(f"Saved Excel -> {path}")

    def save_json(self, df: pd.DataFrame, path: str = "jobskazi_jobs.json"):
        df.to_json(path, orient="records", indent=2, force_ascii=False)
        logger.info(f"Saved JSON -> {path}")

    # ------------------------------------------------------------------
    # Internal utility
    # ------------------------------------------------------------------

    @staticmethod
    def _relative_to_date_str(text: str) -> str:
        """
        Converts relative dates to "on DD-MM-YYYY" format.
          'Posted 2 months ago' -> 'on 18-01-2026'
          'Posted 3 days ago'   -> 'on 15-03-2026'
        """
        if not text:
            return ""
        t = text.lower().strip()
        now = datetime.now()
        try:
            from datetime import timedelta
            if "hour" in t or "minute" in t:
                dt = now
            elif "day" in t:
                n  = int(re.search(r"(\d+)", t).group(1))
                dt = now - timedelta(days=n)
            elif "week" in t:
                n  = int(re.search(r"(\d+)", t).group(1))
                dt = now - timedelta(weeks=n)
            elif "month" in t:
                n     = int(re.search(r"(\d+)", t).group(1))
                month = now.month - n
                year  = now.year
                while month <= 0:
                    month += 12
                    year  -= 1
                dt = datetime(year, month, min(now.day, 28))
            elif "year" in t:
                n  = int(re.search(r"(\d+)", t).group(1))
                dt = datetime(now.year - n, now.month, now.day)
            else:
                return ""
            return f"on {dt.strftime('%d-%m-%Y')}"
        except Exception:
            return ""


# ----------------------------------------------
# Entry point
# ----------------------------------------------

def main():
    scraper = JobScraper(
        request_delay=0.5,
        timeout=15,
        max_workers=8,
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

    scraper.save_csv(df, "jobskazi_jobs.csv")
    scraper.save_excel(df, "jobskazi_jobs.xlsx")
    scraper.save_json(df, "jobskazi_jobs.json")


if __name__ == "__main__":
    main()
