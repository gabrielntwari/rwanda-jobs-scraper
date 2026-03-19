"""
MIFOTRA Scraper - Complete Dataset Version
===========================================
Gets basic job info + detailed qualifications by clicking each job
"""

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


class MifotraScraper:
    
    def __init__(self, headless=True):
        self.headless = headless
        self.SOURCE = "mifotra"
    
    def scrape(self) -> pd.DataFrame:
        """Main scrape function"""
        logger.info("="*60)
        logger.info("MIFOTRA Scraper - Complete Dataset")
        logger.info("="*60)
        
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        
        # Try to get ChromeDriver - handle offline/download errors gracefully
        driver = None
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logger.warning(f"webdriver_manager failed ({e}), trying system chromedriver...")
            try:
                # Fall back to system-installed chromedriver (no download needed)
                driver = webdriver.Chrome(options=options)
            except Exception as e2:
                logger.error(f"Could not start Chrome: {e2}")
                logger.error("MIFOTRA requires Chrome/Chromium installed. Skipping.")
                return pd.DataFrame()
        
        jobs = []
        
        try:
            logger.info(f"Loading: {BASE_URL}")
            driver.get(BASE_URL)
            time.sleep(12)
            
            page_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Parse basic job info
            basic_jobs = self._parse_jobs(page_text)
            logger.info(f"Found {len(basic_jobs)} jobs")
            
            # Now click each job to get details
            logger.info("Fetching detailed information...")
            
            for i, job in enumerate(basic_jobs, 1):
                logger.info(f"  [{i}/{len(basic_jobs)}] {job['title'][:50]}...")
                
                try:
                    # Find the job title on the page
                    job_links = driver.find_elements(By.TAG_NAME, "a")
                    
                    clicked = False
                    for link in job_links:
                        link_text = link.text.strip()
                        if link_text and job['title'] in link_text:
                            # Scroll to it
                            driver.execute_script("arguments[0].scrollIntoView(true);", link)
                            time.sleep(0.5)
                            # Click
                            link.click()
                            clicked = True
                            time.sleep(2)  # Wait for popup
                            break
                    
                    if clicked:
                        # Get popup text
                        popup_text = driver.find_element(By.TAG_NAME, "body").text
                        
                        # Extract details
                        details = self._extract_details(popup_text)
                        job.update(details)
                        
                        # Close popup (press ESC)
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
            return pd.DataFrame()
        
        # Convert to DataFrame
        records = []
        for job in jobs:
            records.append({
                'id': hashlib.md5(f"{self.SOURCE}_{job['title']}".encode()).hexdigest(),
                'title': job['title'],
                'company': job['company'],
                'description': job.get('description', ''),
                'responsibilities': job.get('responsibilities', ''),
                'qualifications': job.get('qualifications', ''),
                'experience_required': job.get('experience', ''),
                'education_level': job.get('education', ''),
                'location_raw': '',
                'district': 'Kigali',
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
        logger.info("="*60)
        
        return df
    
    def _parse_jobs(self, text: str) -> List[Dict]:
        """Parse jobs from text using regex"""
        jobs = []
        
        pattern = r'([^\n]+)\n([^\n]+\([A-Z\-]+\))\nAPPLY\n(Level:[^\n]+)\n(Posts?:\d+)\n(Under (?:Contract|Statute))\nPosted on\n([A-Z][a-z]{2} \d{1,2}, \d{4})\nDeadline\n([A-Z][a-z]{2} \d{1,2}, \d{4})'
        
        matches = re.findall(pattern, text)
        logger.info(f"Regex found {len(matches)} job matches")
        
        for match in matches:
            title, company, level, posts, contract, posted, deadline = match
            
            jobs.append({
                'title': title.strip(),
                'company': company.strip(),
                'level': f"{level} {posts}",
                'contract_type': 'Contract' if 'Contract' in contract else 'Permanent',
                'posted_date': self._parse_date(posted),
                'deadline': self._parse_date(deadline),
            })
        
        return jobs
    
    def _extract_details(self, popup_text: str) -> Dict:
        """Extract details from popup text"""
        details = {}
        
        # Job responsibilities (description)
        if "Job responsibilities" in popup_text:
            resp_start = popup_text.find("Job responsibilities")
            resp_end = popup_text.find("Qualifications", resp_start)
            if resp_end > resp_start:
                resp = popup_text[resp_start + len("Job responsibilities"):resp_end]
                resp = resp.replace("Duties and Responsibilities:", "").strip()
                details['description'] = resp[:500]  # First 500 chars
                details['responsibilities'] = resp[:1000]  # First 1000 chars
        
        # Qualifications
        if "Qualifications" in popup_text:
            qual_start = popup_text.find("Qualifications")
            # Get next 1000 chars after "Qualifications"
            qual_text = popup_text[qual_start + len("Qualifications"):qual_start + 1500]
            
            # Extract numbered qualifications
            qual_lines = []
            for line in qual_text.split('\n')[:15]:
                line = line.strip()
                if line and (line[0].isdigit() or 'degree' in line.lower() or 'bachelor' in line.lower() or 'master' in line.lower()):
                    qual_lines.append(line)
            
            details['qualifications'] = ' | '.join(qual_lines)
            
            # Extract education
            qual_lower = qual_text.lower()
            if 'phd' in qual_lower or 'doctorate' in qual_lower:
                details['education'] = 'PhD'
            elif 'master' in qual_lower:
                details['education'] = 'Master'
            elif 'bachelor' in qual_lower:
                details['education'] = 'Bachelor'
            
            # Extract experience (e.g., "5 years of relevant experience")
            exp_match = re.search(r'(\d+)\s*years?\s*of\s*(?:relevant\s*)?experience', qual_lower)
            if exp_match:
                details['experience'] = f"{exp_match.group(1)} years"
        
        return details
    
    def _parse_date(self, text: str) -> str:
        """Parse date like 'Mar 14, 2026' to '2026-03-14'"""
        months = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        
        match = re.search(r'([A-Z][a-z]{2}) (\d{1,2}), (\d{4})', text)
        if match:
            month_str, day, year = match.groups()
            month = months.get(month_str.lower())
            if month:
                return f"{year}-{month}-{day.zfill(2)}"
        return ""


def main():
    print("\n" + "="*60)
    print("MIFOTRA Complete Dataset Scraper")
    print("="*60)
    print("This will take 2-3 minutes to click through 23 jobs")
    print("Don't close the browser!")
    print("="*60 + "\n")
    
    scraper = MifotraScraper(headless=False)
    df = scraper.scrape()
    
    if not df.empty:
        print(f"\n[OK] SUCCESS! Scraped {len(df)} complete jobs!\n")
        print(df[['title', 'company', 'education', 'experience', 'deadline']].to_string(index=False))
        print(f"\n[INFO] Total: {len(df)} jobs with complete details")
        
        # Show sample of qualifications
        print("\n[INFO] Sample qualification:")
        if not df['qualifications'].isna().all():
            print(df['qualifications'].iloc[0][:200] + "...")
    else:
        print("\n[ERROR] No jobs found")


if __name__ == "__main__":
    main()
