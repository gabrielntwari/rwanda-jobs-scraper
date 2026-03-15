"""
JobWebRwanda Scraper - Standalone Version
Scrapes job listings from https://jobwebrwanda.com/jobs/
No base_scraper.py required
"""

import hashlib
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup


class JobWebRwandaScraper:
    """Scraper for JobWebRwanda website"""
    
    def __init__(self):
        self.source_name = "jobwebrwanda"
        self.base_url = "https://jobwebrwanda.com"
        self.jobs_url = f"{self.base_url}/jobs/"
        
        # Setup session with headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        
        # Setup logging
        self.logger = logging.getLogger(f"{__name__}.{self.source_name}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Rate limiting
        self.request_delay = 1.0
        self.last_request_time = 0
    
    def rate_limit(self):
        """Implement rate limiting between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_delay:
            sleep_time = self.request_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_base_job_dict(self) -> Dict:
        """Get base job dictionary with all required fields"""
        now = datetime.utcnow().isoformat() + '+00:00'
        
        return {
            'id': '',
            'title': '',
            'company': '',
            'description': '',
            'location_raw': '',
            'district': '',
            'country': 'Rwanda',
            'is_remote': False,
            'is_hybrid': False,
            'rwanda_eligible': True,
            'eligibility_reason': '',
            'confidence_score': 5,
            'sector': '',
            'job_level': '',
            'experience_years': '',
            'employment_type': '',
            'education_level': '',
            'salary_min': '',
            'salary_max': '',
            'currency': '',
            'salary_disclosed': False,
            'posted_date': '',
            'deadline': '',
            'scraped_at': now,
            'source': self.source_name,
            'source_url': '',
            'source_job_id': '',
            'is_active': True,
            'last_checked': now,
            'duplicate_hash': ''
        }
    
    def generate_job_id(self, job_url: str) -> str:
        """Generate unique job ID from URL"""
        return hashlib.md5(job_url.encode()).hexdigest()
    
    def extract_source_job_id(self, job_url: str) -> str:
        """Extract job ID from URL slug"""
        parts = job_url.rstrip('/').split('/')
        if parts and parts[-1]:
            return parts[-1]
        return ""
    
    def parse_location(self, location_str: str) -> tuple:
        """Parse location string to extract district"""
        if not location_str:
            return "", "Rwanda"
        
        location_str = location_str.strip()
        
        # Common districts in Rwanda
        districts = [
            'Kigali', 'Butaro', 'Byumba', 'Cyangugu', 'Gisenyi', 'Gitarama',
            'Kayonza', 'Kibuye', 'Nyanza', 'Rubengera', 'Ruhengeri', 'Rusumo',
            'Rwamagana', 'Musanze', 'Huye', 'Ngoma', 'Nyarugenge', 'Gasabo',
            'Kicukiro', 'Bugesera', 'Gatsibo', 'Karongi', 'Nyabihu', 'Nyagatare',
            'Nyamagabe', 'Nyamasheke', 'Nyaruguru', 'Rubavu', 'Ruhango',
            'Rulindo', 'Rusizi', 'Rutsiro', 'Gisagara', 'Kirehe', 'Muhanga',
            'Ngororero', 'Gicumbi'
        ]
        
        for district in districts:
            if district.lower() in location_str.lower():
                return district, "Rwanda"
        
        if location_str.lower() == 'rwanda':
            return "", "Rwanda"
        
        return location_str, "Rwanda"
    
    def extract_job_type(self, job_type_str: str) -> str:
        """Extract employment type from job type string"""
        if not job_type_str:
            return 'Full-time'
        
        job_type_lower = job_type_str.lower().strip()
        
        if 'full-time' in job_type_lower or 'full time' in job_type_lower:
            return 'Full-time'
        elif 'part-time' in job_type_lower or 'part time' in job_type_lower:
            return 'Part-time'
        elif 'temporary' in job_type_lower:
            return 'Temporary'
        elif 'freelance' in job_type_lower:
            return 'Freelance'
        elif 'internship' in job_type_lower:
            return 'Internship'
        elif 'contract' in job_type_lower or 'consultancy' in job_type_lower:
            return 'Contract'
        else:
            return 'Full-time'
    
    def categorize_sector(self, category: str, title: str = "") -> str:
        """Map job category to standardized sector"""
        if not category and not title:
            return 'Other'
        
        search_text = f"{category} {title}".lower()
        
        sector_mapping = {
            'it': 'IT',
            'telecom': 'IT',
            'technology': 'IT',
            'computer': 'IT',
            'software': 'IT',
            'developer': 'IT',
            'programmer': 'IT',
            'engineering': 'Engineering',
            'health': 'Healthcare',
            'medicine': 'Healthcare',
            'medical': 'Healthcare',
            'healthcare': 'Healthcare',
            'nursing': 'Healthcare',
            'clinical': 'Healthcare',
            'education': 'Education',
            'teaching': 'Education',
            'academic': 'Education',
            'teacher': 'Education',
            'finance': 'Finance',
            'accounting': 'Finance',
            'banking': 'Finance',
            'accountant': 'Finance',
            'hr': 'HR',
            'human resource': 'HR',
            'marketing': 'Marketing',
            'sales': 'Sales',
            'agriculture': 'Agriculture',
            'agricultural': 'Agriculture',
            'farming': 'Agriculture',
            'construction': 'Construction',
            'real estate': 'Construction',
            'legal': 'Legal',
            'law': 'Legal',
            'procurement': 'Procurement',
            'purchasing': 'Procurement',
            'ngo': 'NGO',
            'development': 'Development',
            'logistics': 'Logistics',
            'transport': 'Logistics',
            'warehouse': 'Logistics',
            'hospitality': 'Hospitality',
            'hotel': 'Hospitality',
            'tourism': 'Hospitality',
            'customer service': 'Customer Service',
            'client service': 'Customer Service',
            'administrative': 'Administration',
            'secretarial': 'Administration',
            'office': 'Administration',
            'manufacturing': 'Manufacturing',
            'production': 'Manufacturing',
            'retail': 'Retail',
            'consulting': 'Consulting',
            'security': 'Security',
        }
        
        for key, value in sector_mapping.items():
            if key in search_text:
                return value
        
        return 'Other'
    
    def determine_job_level(self, title: str) -> str:
        """Determine job level from title keywords"""
        if not title:
            return 'Mid-Level'
        
        title_lower = title.lower()
        
        senior_keywords = [
            'senior', 'lead', 'principal', 'head', 'director', 'chief',
            'manager', 'supervisor', 'executive', 'coordinator'
        ]
        junior_keywords = [
            'junior', 'assistant', 'associate', 'entry', 'graduate',
            'intern', 'trainee', 'fresh'
        ]
        
        if any(word in title_lower for word in senior_keywords):
            return 'Senior'
        elif any(word in title_lower for word in junior_keywords):
            return 'Junior'
        else:
            return 'Mid-Level'
    
    def parse_date(self, date_str: str) -> str:
        """Parse and standardize date"""
        if not date_str:
            return ""
        
        date_str = date_str.strip()
        
        # Try to extract date in DD/MMM/YYYY format
        match = re.search(r'(\d{1,2})/(\w{3})/(\d{4})', date_str)
        if match:
            day, month_abbr, year = match.groups()
            
            months = {
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
                'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
            }
            
            month = months.get(month_abbr.lower(), '01')
            return f"{year}-{month}-{day.zfill(2)}"
        
        return date_str
    
    def parse_job_element(self, job_element) -> Optional[Dict]:
        """Parse individual job listing element"""
        try:
            job_data = self.get_base_job_dict()
            
            # Extract job title and URL - look for the main link with the job title
            # Structure: li > strong > a (with job title)
            title_strong = job_element.find('strong')
            if not title_strong:
                return None
            
            link_tag = title_strong.find('a')
            if not link_tag:
                return None
            
            job_data['title'] = link_tag.get_text(strip=True)
            job_data['source_url'] = link_tag.get('href', '')
            if not job_data['source_url']:
                return None
                
            job_data['source_job_id'] = self.extract_source_job_id(job_data['source_url'])
            job_data['id'] = self.generate_job_id(job_data['source_url'])
            job_data['duplicate_hash'] = job_data['id']
            
            # Extract company from title (format: "Job Title at Company Name")
            if ' at ' in job_data['title']:
                parts = job_data['title'].split(' at ', 1)
                actual_title = parts[0].strip()
                job_data['company'] = parts[1].strip()
                job_data['title'] = actual_title
            
            # Extract job type - look for spans with job type info
            job_type_spans = job_element.find_all('span')
            for span in job_type_spans:
                span_text = span.get_text(strip=True)
                # Job types are repeated, just get first one
                if span_text in ['Full-Time', 'Part-Time', 'Internship', 'Temporary', 'Freelance']:
                    job_data['employment_type'] = self.extract_job_type(span_text)
                    break
            
            # Extract location - look for strong tag with "Location:"
            text_content = job_element.get_text()
            location_match = re.search(r'Location:\s*([^\n]+)', text_content)
            if location_match:
                location_str = location_match.group(1).strip()
                job_data['location_raw'] = location_str
                district, country = self.parse_location(location_str)
                job_data['district'] = district
                job_data['country'] = country
            
            # Extract date - look for strong tag with "Date" or span with date class
            date_match = re.search(r'(\d{1,2}/\w{3}/\d{4})', text_content)
            if date_match:
                date_str = date_match.group(1)
                job_data['posted_date'] = self.parse_date(date_str)
            
            # Determine sector and job level
            job_data['sector'] = self.categorize_sector('', job_data['title'])
            job_data['job_level'] = self.determine_job_level(job_data['title'])
            
            # Set default values
            job_data['rwanda_eligible'] = True
            job_data['eligibility_reason'] = 'Listed on jobwebrwanda.com'
            job_data['confidence_score'] = 5
            
            return job_data
        
        except Exception as e:
            self.logger.error(f"Error parsing job element: {e}")
            return None
    
    def scrape_page(self, page_num: int = 1) -> List[Dict]:
        """Scrape a single page of job listings"""
        jobs = []
        
        if page_num == 1:
            url = self.jobs_url
        else:
            url = f"{self.jobs_url}page/{page_num}/"
        
        try:
            self.logger.info(f"Scraping page {page_num}: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find job containers - jobs are in numbered list items
            # Look for the ordered list containing jobs
            job_list = soup.find('ol')
            if job_list:
                job_containers = job_list.find_all('li', recursive=False)
            else:
                # Fallback to article tags
                job_containers = soup.find_all('article')
            
            self.logger.info(f"Found {len(job_containers)} job containers")
            
            for container in job_containers:
                job_data = self.parse_job_element(container)
                if job_data and job_data.get('title'):
                    jobs.append(job_data)
            
            self.logger.info(f"Successfully parsed {len(jobs)} jobs from page {page_num}")
        
        except Exception as e:
            self.logger.error(f"Error scraping page {page_num}: {e}")
        
        return jobs
    
    def scrape(self, max_pages: int = 1) -> List[Dict]:
        """Main scraping method"""
        all_jobs = []
        
        self.logger.info(f"Starting scrape of JobWebRwanda (max {max_pages} pages)")
        
        for page_num in range(1, max_pages + 1):
            page_jobs = self.scrape_page(page_num)
            
            if not page_jobs:
                self.logger.warning(f"No jobs found on page {page_num}, stopping")
                break
            
            all_jobs.extend(page_jobs)
            
            if page_num < max_pages:
                self.rate_limit()
        
        self.logger.info(f"Scraping complete. Total jobs: {len(all_jobs)}")
        return all_jobs
    
    def close(self):
        """Clean up resources"""
        if self.session:
            self.session.close()


def main():
    """Test the scraper"""
    scraper = JobWebRwandaScraper()
    
    try:
        jobs = scraper.scrape(max_pages=1)
        
        print(f"\nScraped {len(jobs)} jobs")
        
        if jobs:
            print("\nSample jobs:")
            for i, sample in enumerate(jobs[:3], 1):
                print(f"\n{i}. {sample['title']}")
                print(f"   Company: {sample['company']}")
                print(f"   Sector: {sample['sector']}")
                print(f"   Level: {sample['job_level']}")
                print(f"   Type: {sample['employment_type']}")
                print(f"   Location: {sample['location_raw']} ({sample['district']})")
                print(f"   Posted: {sample['posted_date']}")
                print(f"   URL: {sample['source_url']}")
    
    finally:
        scraper.close()


if __name__ == "__main__":
    main()