"""
Job Scraper for www.greatrwandajobs.com
----------------------------------------------------------

Site facts (confirmed from live fetch):
  Total jobs : 36,172
  No Cloudflare - plain requests works
  CMS        : Joomla + JsJobs component

Listing URL pattern  (20 cards per page, Joomla offset):
  /jobs/?start=0   /jobs/?start=20   /jobs/?start=40  ...

Job detail URL:
  /jobs/job-detail/job-{slug}-{id}

Card text structure:
  <a href="/jobs/job-detail/job-...">Title</a>
  Company from <img alt="Company Name">  or company <a>
  "Job Category: ..."
  "Posted: Today"  /  "Posted: X Days Ago"
  "Deadline of this Job: 08th March 2026"
  "Duty Station: Kigali | Kigali | Rwanda"
"""

import re, time, hashlib, logging, random
from datetime import datetime, date, timezone, timedelta
from typing import List, Dict, Optional, Any, Set
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from schema import enforce

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("greatrwandajobs_scraper.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

BASE_URL  = "https://www.greatrwandajobs.com"
LIST_URL  = f"{BASE_URL}/jobs/"
PAGE_SIZE = 50

RWANDA_DISTRICTS = [
    "kigali","gasabo","kicukiro","nyarugenge","musanze","rubavu","rusizi",
    "huye","rwamagana","muhanga","karongi","nyamasheke","nyagatare","gatsibo",
    "kayonza","kirehe","ngoma","bugesera","nyanza","gisagara","nyaruguru",
    "ruhango","kamonyi","gakenke","rulindo","gicumbi","burera","ngororero",
    "nyabihu","rutsiro","kibagabaga","remera","kimironko","gikondo",
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
               "eoi","rfp","rfq","bid","framework contract","notice for supply","notice for provision",
               "terms of reference","tor","consultancy services"],
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

MONTH_MAP = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
    "jan":1,"feb":2,"mar":3,"apr":4,"jun":6,"jul":7,"aug":8,
    "sep":9,"oct":10,"nov":11,"dec":12,
}


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
    m = re.search(r"(\d+)\s*(?:to|-|-)\s*(\d+)\s*(?:years?|yrs?)", t)
    if m: return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(\d+)\s*\+\s*(?:years?|yrs?)", t)
    if m: return f"{m.group(1)}+"
    m = re.search(r"(?:at\s+least|minimum|over|more\s+than)\s+(\d+)\s*(?:years?|yrs?)", t)
    if m: return f"{m.group(1)}+"
    m = re.search(r"(\d+)\s*(?:years?|yrs?)", t)
    if m: return m.group(1)
    return ""

def extract_salary(text):
    r = {"salary_min":"","salary_max":"","currency":"","salary_disclosed":False}
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
    loc = {"location_raw":clean(text) or "","district":"","country":"Rwanda",
           "is_remote":False,"is_hybrid":False}
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
    return {"rwanda_eligible":True,"eligibility_reason":"Listed on greatrwandajobs.com","confidence_score":3}

def relative_to_date(text: str) -> str:
    """Convert 'Today', '1 Day Ago', '3 Days Ago' to YYYY-MM-DD."""
    if not text: return ""
    t = text.strip().lower()
    today = date.today()
    if "today" in t: return today.isoformat()
    m = re.search(r"(\d+)\s+day", t)
    if m: return (today - timedelta(days=int(m.group(1)))).isoformat()
    return text.strip()

def parse_deadline(text: str) -> str:
    """
    Normalise many deadline formats to YYYY-MM-DD.
    Falls back to returning the original text so no data is lost.
    Formats seen on site:
      08th March 2026
      10/03/2026
      28/02/2026
      19.03.2026 at 2:30 PM
      Thursday, March 12, 2026
      March 3, 2026, at 5:00 PM
    """
    if not text: return ""
    t = text.strip()

    # DD/MM/YYYY or DD.MM.YYYY
    m = re.search(r"(\d{1,2})[/\.](\d{1,2})[/\.](\d{4})", t)
    if m:
        try: return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
        except: pass

    # Month name variants
    tl = t.lower()
    for mname, mnum in MONTH_MAP.items():
        if mname in tl:
            day_m  = re.search(r"(\d{1,2})(?:st|nd|rd|th)?", t)
            year_m = re.search(r"(\d{4})", t)
            if day_m and year_m:
                try: return f"{year_m.group(1)}-{str(mnum).zfill(2)}-{day_m.group(1).zfill(2)}"
                except: pass
            break

    return t  # return as-is rather than lose the information

def extract_job_id(url: str) -> str:
    m = re.search(r"-(\d+)$", urlparse(url).path.rstrip("/"))
    return m.group(1) if m else urlparse(url).path.strip("/").split("/")[-1][:80]


class GreatRwandaJobsScraper:
    SOURCE = "greatrwandajobs"

    def __init__(self, delay=0.4, timeout=20, workers=10, max_pages=None):
        """
        Args:
            delay:     Seconds between requests.
            timeout:   HTTP timeout.
            workers:   Concurrent threads for detail fetches.
            max_pages: Cap on listing pages (None = all ~1800 pages for 36k jobs).
                       Set to 50 for a quick test (~1000 recent jobs).
        """
        self.delay     = delay
        self.timeout   = timeout
        self.workers   = workers
        self.max_pages = max_pages
        self.session   = self._build_session()

    def _build_session(self):
        s = requests.Session()
        retry = Retry(total=4, backoff_factor=1,
                      status_forcelist=[429,500,502,503,504],
                      allowed_methods=["GET"])
        adp = HTTPAdapter(max_retries=retry)
        s.mount("http://", adp); s.mount("https://", adp)
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection":      "keep-alive",
            "Referer":         BASE_URL,
        })
        return s

    def _get(self, url: str) -> Optional[BeautifulSoup]:
        time.sleep(self.delay + random.uniform(0, 0.2))
        try:
            resp = self.session.get(url, timeout=self.timeout)
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", "?")
            logger.warning(f"HTTP {status} -> {url}")
            return None

    # -- Step 1: parse listing page cards -----------------------------

    def _parse_listing(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Walk every job-detail link on the page.
        For each link, walk UP the DOM to find the enclosing card block,
        then extract: company (img alt), category, posted, deadline, location,
        employment type (badge text before the link).
        """
        stubs = []
        seen: Set[str] = set()

        for a in soup.select("a[href*='/jobs/job-detail/']"):
            href = a.get("href","")
            full_url = href if href.startswith("http") else BASE_URL + href
            if full_url in seen: continue
            seen.add(full_url)

            title = clean(a.get_text()) or ""
            # Skip navigation links (very short text or just whitespace)
            if not title or len(title) < 3: continue

            # Walk up to find a block with enough context (~200+ chars)
            block = a.parent
            for _ in range(6):
                if block is None: break
                bt = block.get_text(" ")
                if len(bt) > 120: break
                block = block.parent

            block_text = clean(block.get_text(" ")) if block else ""

            # -- Company: from <img alt> nearest to this link ----------
            company = ""
            if block:
                img = block.find("img", alt=True)
                if img:
                    alt = img.get("alt","").strip()
                    # Skip generic/icon images
                    if alt and len(alt) > 2 and "logo" not in alt.lower():
                        company = alt
                # Fallback: company link (Joomla JsJobs uses /jobs/company-detail/)
                if not company:
                    comp_a = block.find("a", href=re.compile(r"/jobs/company-detail/"))
                    if comp_a:
                        company = clean(comp_a.get("title","") or comp_a.get_text()) or ""

            # -- Category ----------------------------------------------
            category = ""
            m = re.search(r"Job Category:\s*(.+?)(?:\n|Posted|Deadline)", block_text, re.IGNORECASE)
            if m: category = clean(m.group(1)) or ""

            # -- Employment type badge (appears before job link in card) -
            emp_type = ""
            m = re.search(r"(Full-time|Part-time|Contract|Internship|Volunteer|Consultancy)",
                          block_text, re.IGNORECASE)
            if m: emp_type = m.group(1)

            # -- Posted date -------------------------------------------
            posted = ""
            m = re.search(r"Posted:\s*(.+?)(?:\n|Deadline)", block_text, re.IGNORECASE)
            if m: posted = relative_to_date(clean(m.group(1)) or "")

            # -- Deadline ---------------------------------------------
            deadline = ""
            m = re.search(r"Deadline of this Job:\s*(.+?)(?:\n|Duty Station|$)",
                          block_text, re.IGNORECASE | re.DOTALL)
            if m: deadline = parse_deadline(clean(m.group(1)) or "")

            # -- Location (Duty Station) -------------------------------
            location_raw = ""
            m = re.search(r"Duty Station:\s*(.+?)(?:\n|$)", block_text, re.IGNORECASE)
            if m: location_raw = clean(m.group(1)) or ""

            stubs.append({
                "source_url":          full_url,
                "source_job_id":       extract_job_id(full_url),
                "title":               title,
                "company":             company,
                "category":            category,
                "employment_type_raw": emp_type,
                "posted_date":         posted,
                "deadline":            deadline,
                "location_raw":        location_raw,
            })

        return stubs

    def _get_total_jobs(self, soup: BeautifulSoup) -> int:
        """Extract 'Total jobs: 36172' from listing page."""
        m = re.search(r"Total jobs:\s*([\d,]+)", soup.get_text())
        if m:
            try: return int(m.group(1).replace(",",""))
            except: pass
        return 0

    # -- Collect all stubs via pagination -----------------------------

    def collect_stubs(self) -> List[Dict]:
        all_stubs: List[Dict] = []
        seen: Set[str] = set()

        # Fetch page 1 to get total count
        soup = self._get(LIST_URL)
        if not soup:
            logger.error("Cannot reach listing page")
            return []

        total = self._get_total_jobs(soup)
        logger.info(f"Site reports {total} total jobs")

        # Process page 1
        for s in self._parse_listing(soup):
            if s["source_url"] not in seen:
                seen.add(s["source_url"]); all_stubs.append(s)

        # Calculate pages
        total_pages = (total // PAGE_SIZE) + 1 if total else 2000
        if self.max_pages:
            total_pages = min(total_pages, self.max_pages)

        logger.info(f"Scraping {total_pages} listing pages (~{total_pages * PAGE_SIZE} jobs)")

        for page_num in range(1, total_pages):
            start = page_num * PAGE_SIZE
            url   = f"{LIST_URL}?start={start}"
            soup  = self._get(url)

            if not soup:
                logger.info(f"  Stopped at offset {start} (no response)")
                break

            new_stubs = self._parse_listing(soup)
            if not new_stubs:
                logger.info(f"  No cards at offset {start} - end of listings")
                break

            new = 0
            for s in new_stubs:
                if s["source_url"] not in seen:
                    seen.add(s["source_url"]); all_stubs.append(s); new += 1

            if page_num % 50 == 0:
                logger.info(f"  Page {page_num}/{total_pages} | offset {start} | total stubs: {len(all_stubs)}")

            # If 0 new results, site probably wrapped around - stop
            if new == 0 and page_num > 5:
                logger.info("  No new results - stopping pagination")
                break

        logger.info(f"Total stubs collected: {len(all_stubs)}")
        return all_stubs

    # -- Step 2: fetch detail page -------------------------------------

    def _parse_detail(self, url: str) -> Dict:
        detail: Dict[str, Any] = {}
        soup = self._get(url)
        if not soup: return detail
        try:
            # -- Company Name Extraction -------------------------------
            company = ""
            
            # Try multiple selectors for company name
            company_selectors = [
                ("a", {"href": re.compile(r"/jobs/company-detail/")}),
                ("div", {"class": re.compile(r"company", re.I)}),
                ("span", {"class": re.compile(r"company", re.I)}),
            ]
            
            for tag, attrs in company_selectors:
                comp_el = soup.find(tag, attrs)
                if comp_el:
                    company = clean(comp_el.get("title","") or comp_el.get_text())
                    if company and len(company) > 2:
                        break
            
            # Try regex pattern in page text
            if not company:
                page_text = soup.get_text()
                match = re.search(r'Company(?:\s+Name)?:\s*([^\n]{3,100})', page_text, re.IGNORECASE)
                if match:
                    company = clean(match.group(1))
            
            # Try image alt text
            if not company:
                for img in soup.find_all("img", alt=True):
                    alt = img.get("alt", "").strip()
                    if alt and len(alt) > 2 and "logo" not in alt.lower() and "icon" not in alt.lower():
                        company = alt
                        break
            
            if company:
                detail["company"] = company
            
            # Full description - Joomla JsJobs puts it in div.jsjobsview or similar
            desc_el = soup.select_one(
                "div.jsjobsview, div.job-description, div[class*='jobdesc'], "
                "div.jsjobs_job_description, div#jsjobs_job_detail_block, "
                "div.item-page, div.jd_inner, div[itemprop='description']"
            )
            if not desc_el:
                # Fallback: largest text block in main content
                desc_el = soup.select_one("div#content, div.jd_container, main, article")
            detail["description"] = clean(desc_el.get_text(" ")) if desc_el else ""

            # Employment type from detail (more reliable than card badge)
            etype_el = soup.select_one(
                "span[class*='type'], div[class*='job-type'], "
                "td:-soup-contains('Employment'), span:-soup-contains('Full-time'), "
                "span:-soup-contains('Contract')"
            )
            if etype_el: detail["employment_type_detail"] = clean(etype_el.get_text())

            # Application link or email
            detail["application_link"] = ""
            for a in soup.select("a[href]"):
                href = a.get("href",""); txt = (a.get_text() or "").lower()
                if href.startswith("mailto:"): detail["application_link"] = href; break
                if any(k in txt for k in ["apply now","apply here","submit application","click to apply"]) \
                   and not href.startswith(BASE_URL):
                    detail["application_link"] = href; break

        except Exception as e:
            logger.debug(f"Detail parse error [{url}]: {e}")
        return detail

    # -- Step 3: build canonical record -------------------------------

    def _build(self, stub: Dict, detail: Dict) -> Dict:
        title    = stub.get("title","")
        company  = detail.get("company") or stub.get("company","")  # Prioritize detail page company
        desc     = detail.get("description","") or ""
        loc_raw  = stub.get("location_raw","")
        cat      = stub.get("category","") or ""
        emp_raw  = detail.get("employment_type_detail") or stub.get("employment_type_raw","")
        src_url  = stub.get("source_url","")

        loc = infer_location(loc_raw or desc[:400])
        sal = extract_salary(desc)

        # Sector: use job category first (site already labelled it), then infer
        sector = ""
        if "tender" in cat.lower():
            sector = "Tender"
        else:
            sector = infer_field(desc, SECTOR_KEYWORDS, title=title) or ""

        employment_type = (
            clean(emp_raw)
            or infer_field(desc, EMPLOYMENT_TYPE_KEYWORDS, title=title)
            or ""
        )

        record = {
            "id":               make_hash(src_url),
            "title":            title,
            "company":          company,
            "description":      desc,
            "location_raw":     loc["location_raw"] or loc_raw,
            "district":         loc["district"],
            "country":          loc["country"],
            "is_remote":        loc["is_remote"],
            "is_hybrid":        loc["is_hybrid"],
            "rwanda_eligible":  True,
            "eligibility_reason": "",
            "confidence_score": 0,
            "sector":           sector,
            "job_level":        infer_field(desc, JOB_LEVEL_KEYWORDS, title=title) or "",
            "experience_years": extract_exp(desc),
            "employment_type":  employment_type,
            "education_level":  infer_field(desc, EDUCATION_KEYWORDS) or "",
            "salary_min":       sal["salary_min"],
            "salary_max":       sal["salary_max"],
            "currency":         sal["currency"],
            "salary_disclosed": sal["salary_disclosed"],
            "posted_date":      stub.get("posted_date",""),
            "deadline":         stub.get("deadline",""),
            "scraped_at":       now_iso(),
            "source":           self.SOURCE,
            "source_url":       src_url,
            "source_job_id":    stub.get("source_job_id",""),
            "is_active":        True,
            "last_checked":     now_iso(),
            "duplicate_hash":   make_hash(src_url),
        }
        record.update(infer_eligibility(record))
        return record

    # -- Main ----------------------------------------------------------

    def scrape(self) -> pd.DataFrame:
        logger.info("=" * 60)
        logger.info(f"greatrwandajobs.com scraper | max_pages={self.max_pages} workers={self.workers}")
        logger.info("=" * 60)
        t0 = time.time()

        stubs = self.collect_stubs()
        if not stubs:
            logger.error("No stubs found.")
            return pd.DataFrame()

        logger.info(f"Fetching {len(stubs)} detail pages ({self.workers} workers)...")
        details: Dict[str, Dict] = {}

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futures = {ex.submit(self._parse_detail, s["source_url"]): s["source_url"]
                       for s in stubs}
            for i, f in enumerate(as_completed(futures), 1):
                url = futures[f]
                try: details[url] = f.result()
                except Exception as e:
                    logger.warning(f"Detail error: {e}"); details[url] = {}
                if i % 100 == 0:
                    el = time.time()-t0; eta = (el/i)*(len(stubs)-i)
                    logger.info(f"  {i}/{len(stubs)} ({i/len(stubs)*100:.0f}%) "
                                f"elapsed={el:.0f}s ETA={eta:.0f}s")

        records = [self._build(s, details.get(s["source_url"],{})) for s in stubs]
        seen: Set[str] = set()
        unique = [r for r in records if r["source_url"] not in seen and not seen.add(r["source_url"])]

        df = pd.DataFrame(unique)
        df = enforce(df)
        logger.info(f"Done in {time.time()-t0:.1f}s - {len(df)} jobs")
        return df

    def save_csv(self, df, path="greatrwandajobs_jobs.csv"):
        df.to_csv(path, index=False, encoding="utf-8-sig"); logger.info(f"Saved -> {path}")

    def save_excel(self, df, path="greatrwandajobs_jobs.xlsx"):
        df.to_excel(path, index=False, engine="openpyxl"); logger.info(f"Saved -> {path}")

    def save_json(self, df, path="greatrwandajobs_jobs.json"):
        df.to_json(path, orient="records", indent=2, force_ascii=False); logger.info(f"Saved -> {path}")


def main():
    scraper = GreatRwandaJobsScraper(
        delay=0.4,
        timeout=20,
        workers=20,
        max_pages=200,
    )

    df = scraper.scrape()
    if df.empty: return

    tenders = (df["sector"] == "Tender").sum()
    jobs    = len(df) - tenders

    print(f"\n{'='*60}")
    print("  SCRAPE SUMMARY -- greatrwandajobs.com")
    print(f"{'='*60}")
    print(f"  Total records    : {len(df)}")
    print(f"  Jobs             : {jobs}")
    print(f"  Tenders          : {tenders}")
    print(f"  Rwanda eligible  : {df['rwanda_eligible'].sum()}")
    print(f"  With deadline    : {(df['deadline'] != '').sum()}")
    print(f"  Salary disclosed : {df['salary_disclosed'].sum()}")
    print(f"\n  Top sectors:")
    for s,c in df["sector"].value_counts().head(8).items():
        print(f"    {str(s):<25} {c}")
    print(f"\n  Top companies:")
    for s,c in df["company"].value_counts().head(6).items():
        print(f"    {str(s):<40} {c}")
    print(f"{'='*60}")

    scraper.save_csv(df)
    scraper.save_excel(df)
    scraper.save_json(df)


if __name__ == "__main__":
    main()