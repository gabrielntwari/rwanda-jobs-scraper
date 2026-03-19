"""
Job Scraper for jobs.newtimes.co.rw
----------------------------------------------------------

WHY 403?
  The site is behind Cloudflare bot protection. It requires JavaScript
  execution to set a `cf_clearance` cookie before allowing scraping.

SOLUTION - install cloudscraper (handles Cloudflare automatically):
  pip install cloudscraper

  cloudscraper is a drop-in replacement for requests.Session().
  It solves Cloudflare JS challenges transparently.

FALLBACK (if cloudscraper also gets blocked):
  Use Selenium or Playwright to get the cf_clearance cookie once,
  then pass it to requests. See get_cf_cookie() at the bottom.

Site structure (confirmed):
  Listing: /jobs/search?page=N          (jobs)
  Tenders: /jobs/search/tenders?page=N  (tenders)
  Job URL: /jobs/{id}-{slug}
  Each card: "Title * Company | Published on DD-MM-YYYY | Deadline DD-MM-YYYY"
"""

import re, time, hashlib, logging, random
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any, Set
from urllib.parse import urlparse

# -- Try cloudscraper first, fall back to requests ---------------------
try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry

from bs4 import BeautifulSoup
import pandas as pd
from schema import enforce

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("newtimes_scraper.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

BASE_URL = "https://jobs.newtimes.co.rw"

SEARCH_ENDPOINTS = [
    f"{BASE_URL}/jobs/search",
    f"{BASE_URL}/jobs/search/tenders",
]

# -- Lookup tables -----------------------------------------------------

RWANDA_DISTRICTS = [
    "kigali","gasabo","kicukiro","nyarugenge","musanze","rubavu","rusizi",
    "huye","rwamagana","muhanga","karongi","nyamasheke","nyagatare","gatsibo",
    "kayonza","kirehe","ngoma","bugesera","nyanza","gisagara","nyaruguru",
    "ruhango","kamonyi","gakenke","rulindo","gicumbi","burera","ngororero",
    "nyabihu","rutsiro",
]

SECTOR_KEYWORDS = {
    "Health": ["nurse","doctor","physician","surgeon","pharmacist","midwife","clinical",
               "epidemiolog","laboratory","radiology","dentist","public health","hospital",
               "nutrition","dietitian","health worker","community health","hiv","malaria"],
    "Finance": ["accountant","accounting","auditor","audit","tax consultant","financial analyst",
                "chief financial","cfo","treasurer","budget analyst","microfinance",
                "investment analyst","credit analyst","actuary","bookkeeper"],
    "Agriculture": ["agronomist","agronomy","horticulture","livestock","veterinary","crop",
                    "soil","irrigation","aquaculture","agri-business","agricultural extension",
                    "rural development","food security"],
    "Education": ["teacher","lecturer","professor","headmaster","principal","curriculum developer",
                  "academic","school","university","early childhood","education officer"],
    "Construction": ["architect","structural engineer","quantity surveyor","site engineer",
                     "construction manager","civil engineer","urban planner","land surveyor"],
    "Logistics": ["supply chain","logistics officer","warehouse","procurement officer",
                  "fleet manager","customs officer","freight","inventory manager","distribution"],
    "HR": ["human resource","hr officer","hr manager","recruiter","talent acquisition",
           "payroll officer","people operations","hr business partner"],
    "Marketing": ["marketing officer","brand manager","communications officer","digital marketer",
                  "social media manager","content creator","public relations","copywriter"],
    "NGO": ["ngo","ingo","unicef","undp","usaid","world bank","oxfam","save the children",
            "care international","irc","mercy corps","msf","humanitarian","monitoring and evaluation"],
    "Tender": ["tender","supply of","provision of","request for proposal","expression of interest",
               "eoi","rfp","rfq","bid","addendum","consultancy services","framework contract"],
    "IT": ["software engineer","software developer","web developer","mobile developer",
           "frontend developer","backend developer","fullstack developer","data engineer",
           "data scientist","machine learning","devops engineer","cloud engineer",
           "network engineer","cybersecurity","database administrator","systems administrator",
           "it support","it officer","it manager","ict officer","programmer","ui/ux designer"],
}

JOB_LEVEL_KEYWORDS = {
    "Internship": ["intern","attachment","apprentice"],
    "Entry":      ["entry","junior","fresh","graduate","trainee","0-1 year","1 year"],
    "Mid":        ["mid","2-3 year","3-5 year","2 years","3 years","4 years","5 years"],
    "Senior":     ["senior","lead","principal","expert","6+ year","7+ year","8+ year",
                   "10+ year","manager","head of","director"],
    "Executive":  ["ceo","cfo","cto","coo","vp ","vice president","executive director",
                   "country director","chief"],
}

EMPLOYMENT_TYPE_KEYWORDS = {
    "Full-time":   ["full-time","full time","permanent","indefinite"],
    "Part-time":   ["part-time","part time"],
    "Contract":    ["contract","fixed-term","fixed term","temporary"],
    "Internship":  ["intern","attachment","apprentice"],
    "Consultancy": ["consultant","consultancy","freelance","terms of reference","tor"],
    "Volunteer":   ["volunteer","voluntary"],
}

EDUCATION_KEYWORDS = {
    "PhD":         ["phd","doctorate","doctoral"],
    "Master's":    ["master","msc","mba","ma ","m.sc","m.a."],
    "Bachelor's":  ["bachelor","bsc","ba ","b.sc","b.a.","degree","undergraduate","university","a0"],
    "Diploma":     ["diploma","a-level","advanced level","hnd","a1","a2"],
    "Certificate": ["certificate","o-level","tvet","vocational"],
}

CURRENCY_SYMBOLS = {
    "RWF": ["rwf","frw","francs rwandais"],
    "USD": ["usd","$","dollars"],
    "EUR": ["eur","euros"],
}

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest":  "document",
    "Sec-Fetch-Mode":  "navigate",
    "Sec-Fetch-Site":  "none",
    "Sec-Fetch-User":  "?1",
    "Cache-Control":   "max-age=0",
}


# -- Helpers -----------------------------------------------------------

def clean(t):
    return " ".join(t.split()).strip() if t else None

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_hash(url):
    return hashlib.md5(url.encode()).hexdigest()

def infer_field(text, kmap, title=""):
    def best(h):
        h = h.lower(); bc, bl = None, 0
        for cat, kws in kmap.items():
            for kw in sorted(kws, key=len, reverse=True):
                if kw in h and len(kw) > bl:
                    bc, bl = cat, len(kw); break
        return bc
    if title:
        r = best(title)
        if r: return r
    return best(text) if text else None

def extract_exp(text):
    if not text: return ""
    t = text.lower()
    if re.search(r"no\s+(prior\s+)?experience", t): return "0"
    m = re.search(r"(\d+)\s*(?:to|-|-|-)\s*(\d+)\s*(?:years?|yrs?)", t)
    if m: return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(\d+)\s*\+\s*(?:years?|yrs?)", t)
    if m: return f"{m.group(1)}+"
    m = re.search(r"(?:at\s+least|minimum|over|more\s+than)\s+(\d+)\s*(?:years?|yrs?)", t)
    if m: return f"{m.group(1)}+"
    m = re.search(r"(\d+)\s*(?:years?|yrs?)", t)
    if m: return m.group(1)
    return ""

def extract_salary(text):
    r = {"salary_min": "", "salary_max": "", "currency": "", "salary_disclosed": False}
    if not text: return r
    t = text.lower()
    for cur, syms in CURRENCY_SYMBOLS.items():
        if any(s in t for s in syms): r["currency"] = cur; break
    m = re.search(r"(\d[\d,]*)\s*[-to]+\s*(\d[\d,]*)", text.replace(",",""))
    if m:
        try:
                    lo=float(m.group(1)); hi=float(m.group(2))
                    if lo <= 9_999_999_999 and hi <= 9_999_999_999:
                        r["salary_min"]=lo; r["salary_max"]=hi; r["salary_disclosed"]=True
        except ValueError: pass
    return r

def infer_location(text):
    loc = {"location_raw": clean(text) or "", "district": "", "country": "Rwanda",
           "is_remote": False, "is_hybrid": False}
    if not text: return loc
    t = text.lower()
    if "remote" in t: loc["is_remote"] = True
    if "hybrid" in t: loc["is_hybrid"] = True
    for d in RWANDA_DISTRICTS:
        if d in t: loc["district"] = d.capitalize(); break
    if "kigali" in t and not loc["district"]: loc["district"] = "Kigali"
    return loc

def infer_eligibility(job):
    text = " ".join(filter(None,[job.get("title",""),job.get("description",""),job.get("location_raw","")])).lower()
    if job.get("is_remote"):
        return {"rwanda_eligible":True,"eligibility_reason":"Remote - open globally","confidence_score":4}
    for d in ["us citizens only","eu residents only","authorized to work in the us"]:
        if d in text:
            return {"rwanda_eligible":False,"eligibility_reason":f"Disqualifying: '{d}'","confidence_score":5}
    for s in ["rwanda","kigali","rwandan"]:
        if s in text:
            return {"rwanda_eligible":True,"eligibility_reason":"Explicitly mentions Rwanda","confidence_score":5}
    return {"rwanda_eligible":True,"eligibility_reason":"Listed on jobs.newtimes.co.rw","confidence_score":3}

def parse_date_dmy(date_str):
    """Convert DD-MM-YYYY to YYYY-MM-DD."""
    if not date_str: return ""
    m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})", date_str.strip())
    if m:
        try: return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
        except: pass
    return date_str

def extract_dates(text):
    result = {"posted_date": "", "deadline": ""}
    if not text: return result
    pub = re.search(r"published\s+on\s+(\d{1,2}-\d{1,2}-\d{4})", text, re.IGNORECASE)
    if pub: result["posted_date"] = parse_date_dmy(pub.group(1))
    dl = re.search(r"deadline\s+(\d{1,2}-\d{1,2}-\d{4})", text, re.IGNORECASE)
    if dl: result["deadline"] = parse_date_dmy(dl.group(1))
    return result

def extract_id(url):
    slug = urlparse(url).path.strip("/").split("/")[-1]
    m = re.match(r"^(\d+)", slug)
    return m.group(1) if m else slug[:80]


# -- Session builder ---------------------------------------------------

def build_session(cf_cookie: str = ""):
    """
    Build a session that can bypass Cloudflare.

    Priority:
      1. cloudscraper  (pip install cloudscraper)  <- recommended
      2. requests with cf_clearance cookie          <- manual cookie from browser
      3. plain requests                              <- will 403 on Cloudflare sites

    To get cf_clearance manually:
      - Open jobs.newtimes.co.rw in Chrome DevTools -> Application -> Cookies
      - Copy the cf_clearance value and pass it as cf_cookie here
    """
    if HAS_CLOUDSCRAPER:
        logger.info("Using cloudscraper (Cloudflare bypass enabled)")
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        scraper.headers.update(BROWSER_HEADERS)
        return scraper

    # Fallback: requests with optional cf_clearance cookie
    logger.warning(
        "cloudscraper not installed - falling back to requests.\n"
        "  Install it: pip install cloudscraper\n"
        "  Or manually set cf_cookie from browser DevTools."
    )
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET"])
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update(BROWSER_HEADERS)

    if cf_cookie:
        logger.info("Using manual cf_clearance cookie")
        session.cookies.set("cf_clearance", cf_cookie, domain="jobs.newtimes.co.rw")

    return session


# -- Scraper -----------------------------------------------------------

class NewTimesScraper:
    SOURCE = "newtimes"

    def __init__(self, delay=0.5, timeout=20, workers=8, cf_cookie=""):
        """
        Args:
            delay:     Seconds between requests.
            timeout:   HTTP timeout.
            workers:   Concurrent threads for detail fetching.
            cf_cookie: Optional cf_clearance cookie value from browser DevTools.
                       Only needed if cloudscraper is not installed.
        """
        self.delay   = delay
        self.timeout = timeout
        self.workers = workers
        self.session = build_session(cf_cookie)

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        time.sleep(self.delay + random.uniform(0, 0.3))
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", "?")
            logger.warning(f"HTTP {status} -> {url}")
            return None

    # -- Parse listing page --------------------------------------------

    def _parse_cards(self, soup: BeautifulSoup, endpoint: str) -> List[Dict]:
        """
        Extract job stubs from a search results page.

        The site renders job cards as <div> or <li> elements.
        Each card text looks like:
          "Supply of fuel at Bank of Kigali | Published on 11-02-2026 | Deadline 25-02-2026"
        And has an <a href="/jobs/501302289-supply-of-fuel-...">

        We try multiple selectors since the site may update its classes.
        """
        stubs = []
        seen_urls: Set[str] = set()
        is_tender = "tenders" in endpoint

        # Strategy 1: standard job card containers
        cards = soup.select(
            "div.job-listing, div.job-item, li.job-item, "
            "div[class*='job-card'], article.job, div.listing-item, "
            "div[class*='listing'], div.card"
        )

        # Strategy 2: fallback - find all links to /jobs/{id}-
        if not cards:
            for a in soup.select("a[href]"):
                href = a.get("href","")
                if not re.search(r"/jobs/\d+", href):
                    continue
                full_url = href if href.startswith("http") else BASE_URL + href
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                # Walk up to find the containing block
                container = a.parent
                for _ in range(4):
                    if container and len(container.get_text()) > 30:
                        break
                    container = container.parent if container else None

                card_text = clean((container or a).get_text(" ")) or ""
                dates = extract_dates(card_text)
                tc = self._title_company(card_text)

                stubs.append({
                    "source_url":          full_url,
                    "source_job_id":       extract_id(full_url),
                    "title_raw":           tc["title"],
                    "company_raw":         tc["company"],
                    "raw_card_text":       card_text,
                    "posted_date":         dates["posted_date"],
                    "deadline":            dates["deadline"],
                    "location_card":       "",
                    "employment_type_raw": "Tender" if is_tender else "",
                    "listing_endpoint":    endpoint,
                })
            return stubs

        for card in cards:
            a = card.select_one("a[href*='/jobs/']")
            if not a: continue
            href = a.get("href","")
            full_url = href if href.startswith("http") else BASE_URL + href
            if full_url in seen_urls: continue
            seen_urls.add(full_url)

            card_text = clean(card.get_text(" ")) or ""
            dates = extract_dates(card_text)
            tc = self._title_company(card_text)

            loc_el = card.select_one(".location,.job-location,[class*='location']")
            type_el = card.select_one(".badge,.tag,.job-type,.type,[class*='type']")

            stubs.append({
                "source_url":          full_url,
                "source_job_id":       extract_id(full_url),
                "title_raw":           tc["title"],
                "company_raw":         tc["company"],
                "raw_card_text":       card_text,
                "posted_date":         dates["posted_date"],
                "deadline":            dates["deadline"],
                "location_card":       clean(loc_el.get_text()) if loc_el else "",
                "employment_type_raw": clean(type_el.get_text()) if type_el else ("Tender" if is_tender else ""),
                "listing_endpoint":    endpoint,
            })

        return stubs

    def _title_company(self, text: str) -> Dict[str, str]:
        """Extract title and company from card text."""
        core = re.sub(r"\|?\s*published\s+on.+", "", text, flags=re.IGNORECASE).strip()
        core = re.sub(r"\|?\s*deadline.+", "", core, flags=re.IGNORECASE).strip()
        m = re.match(r"^(.+?)\s+at\s+(.+)$", core, re.IGNORECASE)
        if m:
            return {"title": clean(m.group(1)) or "", "company": clean(m.group(2)) or ""}
        return {"title": clean(core) or "", "company": ""}

    def collect_stubs(self) -> List[Dict]:
        all_stubs: List[Dict] = []
        seen: Set[str] = set()

        for endpoint in SEARCH_ENDPOINTS:
            logger.info(f"Endpoint: {endpoint}")
            page = 1

            while True:
                url = f"{endpoint}?page={page}" if page > 1 else endpoint
                soup = self._get(url)
                if not soup:
                    break

                stubs = self._parse_cards(soup, endpoint)
                if not stubs:
                    logger.info(f"  Page {page}: no cards found - stopping")
                    break

                new = 0
                for s in stubs:
                    su = s.get("source_url","")
                    if su and su not in seen:
                        seen.add(su); all_stubs.append(s); new += 1

                logger.info(f"  Page {page}: {new} new stubs (total {len(all_stubs)})")

                # Detect end of pagination
                has_next = bool(
                    soup.select_one("a[rel='next'],a.next,li.next a,.pagination .next")
                    or soup.find("a", string=re.compile(r"^(next|>|>>)$", re.IGNORECASE))
                )
                if not has_next:
                    break
                page += 1

        logger.info(f"Total stubs: {len(all_stubs)}")
        return all_stubs

    # -- Parse detail page ---------------------------------------------

    def _parse_detail(self, url: str) -> Dict:
        detail: Dict[str, Any] = {}
        soup = self._get(url)
        if not soup: return detail
        try:
            # Get full page text for pattern matching
            page_text = clean(soup.get_text(" ")) or ""
            
            # TITLE - from H1
            h1 = soup.select_one("h1,.job-title,.listing-title")
            if h1: 
                detail["title_detail"] = clean(h1.get_text())

            # COMPANY - Extract from "at Company Name |" pattern in page text
            # Pattern: "Job Title at Company Name | Published" 
            # Example: "Driver at AHF Rwanda | Published on 10-03-2026"
            company_match = re.search(
                r'\bat\s+([A-Z][A-Za-z0-9\s&\.\(\)]+?)\s*\|',
                page_text[:1000],  # Search in first 1000 chars
                re.IGNORECASE
            )
            if company_match:
                company_text = company_match.group(1).strip()
                # Clean up: remove "Published" if it leaked in
                company_text = re.sub(r'\s+Published.*$', '', company_text, flags=re.IGNORECASE).strip()
                if len(company_text) > 2 and len(company_text) < 100:
                    detail["company_detail"] = company_text
            
            # Fallback: Try to get from page title
            if not detail.get("company_detail"):
                title_tag = soup.select_one("title")
                if title_tag:
                    title_text = clean(title_tag.get_text())
                    # Pattern: "Job at Company | Published"
                    title_match = re.search(r'\bat\s+([A-Za-z0-9\s&\.\(\)]+?)\s*\|', title_text)
                    if title_match:
                        detail["company_detail"] = clean(title_match.group(1))

            # LOCATION
            loc = soup.select_one(".location,.job-location,[class*='location']")
            if loc: detail["location_detail"] = clean(loc.get_text())

            # EMPLOYMENT TYPE
            etype = soup.select_one(".job-type,.employment-type,[class*='type'],[class*='employment']")
            if etype: detail["employment_type_detail"] = clean(etype.get_text())

            # DESCRIPTION - Get from paragraphs in main content
            # First try to find a specific content container
            main_content = soup.select_one("main, article, .content, .job-content")
            
            description_text = ""
            if main_content:
                # Remove navigation, sidebar, and other non-content elements
                for unwanted in main_content.select("nav, .navbar, .header, .footer, .sidebar, .siteSearch, aside, .headerV3-wrapper"):
                    unwanted.decompose()
                
                # Get all paragraphs
                paragraphs = main_content.select("p")
                if paragraphs:
                    desc_text = " ".join([clean(p.get_text()) for p in paragraphs if len(p.get_text().strip()) > 10])
                    # Filter out if it's just navigation text
                    nav_indicators = ["Category Announcement Internship", "All Category Distance", "5 Miles 10 Miles"]
                    is_nav = any(indicator in desc_text[:150] for indicator in nav_indicators)
                    
                    if len(desc_text) > 150 and not is_nav:
                        description_text = desc_text
                
                # Fallback: get all text from main content (skip first 500 chars which might be nav)
                if not description_text:
                    all_text = clean(main_content.get_text(" "))
                    if len(all_text) > 500:
                        # Try to find where actual content starts (after "Posted" or "Deadline")
                        content_start = 0
                        for keyword in ["Posted", "Deadline", "Published on"]:
                            idx = all_text.find(keyword)
                            if idx > 0:
                                # Find the next sentence after this keyword
                                next_sentence = all_text.find(".", idx)
                                if next_sentence > 0:
                                    content_start = next_sentence + 1
                                    break
                        
                        if content_start > 0 and content_start < len(all_text):
                            description_text = all_text[content_start:].strip()
            
            # Final fallback: use page_text but skip navigation at the start
            if not description_text and len(page_text) > 600:
                # Find where "Posted" or similar appears, content usually starts after that
                for marker in ["Posted", "Sign up for Job Alerts"]:
                    idx = page_text.find(marker)
                    if idx > 0 and idx < 800:
                        # Content likely starts after the date info
                        potential_content = page_text[idx+100:]
                        if len(potential_content) > 200:
                            description_text = potential_content
                            break
            
            detail["description"] = description_text

            # Dates from page text
            detail.update(extract_dates(page_text))

            # Apply link
            detail["application_link"] = ""
            for a in soup.select("a[href]"):
                href = a.get("href",""); txt = (a.get_text() or "").lower()
                if any(k in txt for k in ["apply","submit application"]) and not href.startswith(BASE_URL):
                    detail["application_link"] = href; break
                if href.startswith("mailto:"):
                    detail["application_link"] = href; break

        except Exception as e:
            logger.debug(f"Detail parse error [{url}]: {e}")
        return detail

    # -- Build record --------------------------------------------------

    def _build(self, stub: Dict, detail: Dict) -> Dict:
        card_text = stub.get("raw_card_text","") or ""
        is_tender = "tenders" in stub.get("listing_endpoint","")

        title = (detail.get("title_detail") or stub.get("title_raw",""))
        title = re.sub(r"\s*\|?\s*published\s+on.+","", title, flags=re.IGNORECASE).strip()

        company = detail.get("company_detail") or stub.get("company_raw","")
        if company:
            title = re.sub(r"\s+at\s+" + re.escape(company), "", title, flags=re.IGNORECASE).strip()

        posted = detail.get("posted_date") or stub.get("posted_date","")
        deadline = detail.get("deadline") or stub.get("deadline","")
        location_raw = detail.get("location_detail") or stub.get("location_card","")
        employment_raw = detail.get("employment_type_detail") or stub.get("employment_type_raw","")
        description = detail.get("description","") or ""
        source_url = stub.get("source_url","")

        loc = infer_location(location_raw or description[:400])
        sal = extract_salary(description)

        sector = infer_field(description, SECTOR_KEYWORDS, title=title) or (
            "Tender" if is_tender else ""
        )
        employment_type = (
            clean(employment_raw)
            or infer_field(description, EMPLOYMENT_TYPE_KEYWORDS, title=title)
            or ""
        )

        record = {
            "id":               make_hash(source_url),
            "title":            title,
            "company":          company,
            "description":      description,
            "location_raw":     loc["location_raw"] or location_raw,
            "district":         loc["district"],
            "country":          loc["country"],
            "is_remote":        loc["is_remote"],
            "is_hybrid":        loc["is_hybrid"],
            "rwanda_eligible":  True,
            "eligibility_reason": "",
            "confidence_score": 0,
            "sector":           sector,
            "job_level":        infer_field(description, JOB_LEVEL_KEYWORDS, title=title) or "",
            "experience_years": extract_exp(description),
            "employment_type":  employment_type,
            "education_level":  infer_field(description, EDUCATION_KEYWORDS) or "",
            "salary_min":       sal["salary_min"],
            "salary_max":       sal["salary_max"],
            "currency":         sal["currency"],
            "salary_disclosed": sal["salary_disclosed"],
            "posted_date":      posted,
            "deadline":         deadline,
            "scraped_at":       now_iso(),
            "source":           self.SOURCE,
            "source_url":       source_url,
            "source_job_id":    stub.get("source_job_id",""),
            "is_active":        True,
            "last_checked":     now_iso(),
            "duplicate_hash":   make_hash(source_url),
        }
        record.update(infer_eligibility(record))
        return record

    # -- Main ----------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        logger.info("=" * 60)
        logger.info(f"jobs.newtimes.co.rw | cloudscraper={HAS_CLOUDSCRAPER}")
        logger.info("=" * 60)
        t0 = time.time()

        stubs = self.collect_stubs()
        if not stubs:
            logger.error(
                "No stubs found.\n"
                "  -> Install cloudscraper:  pip install cloudscraper\n"
                "  -> Or pass cf_cookie from browser DevTools (see docstring)"
            )
            return pd.DataFrame()

        logger.info(f"Fetching {len(stubs)} detail pages ({self.workers} workers)...")
        details: Dict[str, Dict] = {}

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {ex.submit(self._parse_detail, s["source_url"]): s["source_url"]
                       for s in stubs if s.get("source_url")}
            for i, f in enumerate(as_completed(futures), 1):
                url = futures[f]
                try: details[url] = f.result()
                except Exception as e:
                    logger.warning(f"Detail error: {e}"); details[url] = {}
                if i % 50 == 0:
                    el = time.time()-t0; eta = (el/i)*(len(stubs)-i)
                    logger.info(f"  {i}/{len(stubs)} ({i/len(stubs)*100:.0f}%) elapsed={el:.0f}s ETA={eta:.0f}s")

        records = [self._build(s, details.get(s.get("source_url",""), {})) for s in stubs]
        seen: Set[str] = set()
        unique = [r for r in records if r["source_url"] not in seen and not seen.add(r["source_url"])]

        df = pd.DataFrame(unique)
        df = enforce(df)
        logger.info(f"Done in {time.time()-t0:.1f}s - {len(df)} records")
        return df

    def save_csv(self, df, path="newtimes_jobs.csv"):
        df.to_csv(path, index=False, encoding="utf-8-sig"); logger.info(f"Saved -> {path}")

    def save_excel(self, df, path="newtimes_jobs.xlsx"):
        df.to_excel(path, index=False, engine="openpyxl"); logger.info(f"Saved -> {path}")

    def save_json(self, df, path="newtimes_jobs.json"):
        df.to_json(path, orient="records", indent=2, force_ascii=False); logger.info(f"Saved -> {path}")


# -- Selenium helper (last resort) -------------------------------------

def get_cf_cookie_via_selenium(url=BASE_URL) -> str:
    """
    Use Selenium to solve Cloudflare challenge and extract cf_clearance cookie.
    Run this once, copy the value, then pass it as cf_cookie= to NewTimesScraper.

    Requirements: pip install selenium  +  ChromeDriver in PATH
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        import time as t

        opts = Options()
        # Remove headless so Cloudflare doesn't detect it
        # opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])

        driver = webdriver.Chrome(options=opts)
        driver.get(url)
        t.sleep(8)  # wait for CF challenge to complete

        cf_cookie = ""
        for cookie in driver.get_cookies():
            if cookie["name"] == "cf_clearance":
                cf_cookie = cookie["value"]
                break

        driver.quit()
        print(f"cf_clearance = {cf_cookie}")
        return cf_cookie
    except Exception as e:
        print(f"Selenium failed: {e}")
        return ""


# -- Entry point -------------------------------------------------------

def main():
    # -- Option 1: cloudscraper installed (recommended) ----------------
    # pip install cloudscraper
    # Then just run: python newtimes_scraper.py

    # -- Option 2: manual cf_clearance cookie -------------------------
    # 1. Open jobs.newtimes.co.rw in Chrome
    # 2. DevTools -> Application -> Cookies -> copy cf_clearance value
    # 3. Pass it below:
    CF_COOKIE = ""  # paste cf_clearance value here if needed

    scraper = NewTimesScraper(
        delay=0.5,
        timeout=20,
        workers=8,
        cf_cookie=CF_COOKIE,
    )

    df = scraper.scrape()
    if df.empty: return

    jobs_df = df[df.get("listing_type", pd.Series(["job"]*len(df))) == "job"] if "listing_type" in df.columns else df
    tenders_df = df[df.get("listing_type", pd.Series(["job"]*len(df))) == "tender"] if "listing_type" in df.columns else pd.DataFrame()

    print(f"\n{'='*60}")
    print("  SCRAPE SUMMARY -- jobs.newtimes.co.rw")
    print(f"{'='*60}")
    print(f"  Total records    : {len(df)}")
    print(f"  Rwanda eligible  : {df['rwanda_eligible'].sum()}")
    print(f"  With deadline    : {(df['deadline'] != '').sum()}")
    print(f"\n  Top sectors:")
    for s,c in df["sector"].value_counts().head(6).items():
        print(f"    {str(s):<25} {c}")
    print(f"{'='*60}")

    scraper.save_csv(df)
    scraper.save_excel(df)
    scraper.save_json(df)


if __name__ == "__main__":
    main()