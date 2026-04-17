"""
Database Adapter for Rwanda Jobs Scrapers
==========================================
"""

import os
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)


class SupabaseAdapter:
    """Adapter to save scraped jobs to Supabase database"""
    
    def __init__(self, database_url: str = None):
        """Initialize with Supabase connection"""
        self.database_url = database_url or os.getenv('DATABASE_URL')
        
        if not self.database_url:
            raise ValueError("DATABASE_URL not set")
        
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Connect to Supabase"""
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(self.database_url)
            self.cursor = self.conn.cursor()
            logger.info("Connected to Supabase")
    
    def close(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def start_run(self, source: str) -> int:
        """Start scraper run tracking"""
        self.cursor.execute("""
            INSERT INTO scraper_runs (source, status)
            VALUES (%s, 'running')
            RETURNING id;
        """, (source,))
        self.conn.commit()
        run_id = self.cursor.fetchone()[0]
        logger.info(f"📊 Started scraper run #{run_id} for {source}")
        return run_id
    
    def complete_run(self, run_id: int, found: int, new: int, updated: int, error: str = None):
        """Complete scraper run"""
        status = 'failed' if error else 'completed'
        self.cursor.execute("""
            UPDATE scraper_runs
            SET completed_at = CURRENT_TIMESTAMP,
                status = %s,
                jobs_found = %s,
                jobs_new = %s,
                jobs_updated = %s,
                error_message = %s
            WHERE id = %s;
        """, (status, found, new, updated, error, run_id))
        self.conn.commit()
        logger.info(f"Completed run #{run_id}: {new} new, {updated} updated")
    
    def save_jobs_from_dataframe(self, df: pd.DataFrame, source: str) -> Dict[str, int]:
        """
        Save jobs from pandas DataFrame (your schema format) to Supabase
        
        Returns: {'found': X, 'new': Y, 'updated': Z, 'skipped': W}
        """
        stats = {'found': len(df), 'new': 0, 'updated': 0, 'skipped': 0}
        
        if df.empty:
            logger.warning("Empty DataFrame, nothing to save")
            return stats
        
        # Start run tracking
        run_id = self.start_run(source)
        
        try:
            # Prepare data for insertion
            jobs_to_insert = []
            
            for _, row in df.iterrows():
                # Check if job already exists
                if self._job_exists(row['id']):
                    stats['skipped'] += 1
                    continue
                
                # Prepare job data
                job_data = self._prepare_job_data(row)
                jobs_to_insert.append(job_data)
            
            # Batch insert
            if jobs_to_insert:
                self._batch_insert_jobs(jobs_to_insert)
                stats['new'] = len(jobs_to_insert)
                logger.info(f"Inserted {stats['new']} new jobs")
            
            # Update source statistics
            self._update_source_stats(source, stats['new'])
            
            # Complete run
            self.complete_run(run_id, stats['found'], stats['new'], stats['updated'])
            
        except Exception as e:
            logger.error(f"Error saving jobs: {e}")
            self.complete_run(run_id, stats['found'], 0, 0, str(e))
            raise
        
        return stats
    
    def _job_exists(self, job_id: str) -> bool:
        """Check if job ID already exists"""
        self.cursor.execute("SELECT 1 FROM jobs WHERE id = %s LIMIT 1;", (job_id,))
        return self.cursor.fetchone() is not None
    
    def _prepare_job_data(self, row: pd.Series) -> tuple:
        """Convert DataFrame row to database tuple"""
        return (
            row['id'],
            row['title'],
            row.get('company', ''),
            row.get('description', ''),
            row.get('location_raw', ''),
            row.get('district', ''),
            row.get('country', 'Rwanda'),
            bool(row.get('is_remote', False)),
            bool(row.get('is_hybrid', False)),
            bool(row.get('rwanda_eligible', True)),
            row.get('eligibility_reason', ''),
            int(row.get('confidence_score', 0)),
            row.get('sector', ''),
            row.get('job_level', ''),
            row.get('experience_years', ''),
            row.get('employment_type', ''),
            row.get('education_level', ''),
            self._to_decimal(row.get('salary_min')),
            self._to_decimal(row.get('salary_max')),
            row.get('currency', 'RWF'),
            bool(row.get('salary_disclosed', False)),
            self._to_date(row.get('posted_date')),
            self._to_date(row.get('deadline')),
            row.get('source', ''),
            row.get('source_url', ''),
            row.get('source_job_id', ''),
            row.get('duplicate_hash', ''),
        )
    
    def _to_decimal(self, value) -> Optional[float]:
        """Convert to decimal, handling empty strings"""
        if value == '' or value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except:
            return None
    
    def _to_date(self, value) -> Optional[str]:
        """
        Enhanced date parser that handles ALL date formats from Rwanda job sites.
        Returns YYYY-MM-DD format or None.
        
        Handles:
        - "30 March 2026" (Mucuruzi)
        - "19/03/2026" (Mucuruzi)
        - "05/03/2026" (Mucuruzi)
        - "March 30, 2026"
        - "2026-03-30" (ISO)
        - "Ongoing", "ASAP" (returns None)
        """
        if not value or value in (None, "", "N/A", "Not specified"):
            return None
        
        # If pandas Timestamp
        if hasattr(value, 'strftime'):
            return value.strftime('%Y-%m-%d')
        
        # Convert to string
        if not isinstance(value, str):
            value = str(value)
        
        s = value.strip()
        
        # Remove common prefixes
        import re
        s = re.sub(r'^(on|by|before|until|deadline:?)\s+', '', s, flags=re.IGNORECASE).strip()
        
        # Handle special cases (non-date values)
        special_cases = {
            'ongoing': None,
            'continuous': None,
            'rolling': None,
            'asap': None,
            'immediate': None,
            'open': None,
            'n/a': None,
            'tbd': None,
            'to be determined': None,
        }
        if s.lower() in special_cases:
            return special_cases[s.lower()]
        
        # Month name mapping
        months = {
            'jan': '01', 'january': '01',
            'feb': '02', 'february': '02',
            'mar': '03', 'march': '03',
            'apr': '04', 'april': '04',
            'may': '05',
            'jun': '06', 'june': '06',
            'jul': '07', 'july': '07',
            'aug': '08', 'august': '08',
            'sep': '09', 'september': '09',
            'oct': '10', 'october': '10',
            'nov': '11', 'november': '11',
            'dec': '12', 'december': '12',
        }
        
        # Pattern 1: "30 March 2026" or "30 Mar 2026" (Mucuruzi format)
        match = re.match(r'(\d{1,2})\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{4})', s, re.IGNORECASE)
        if match:
            day = match.group(1).zfill(2)
            month = months[match.group(2).lower()]
            year = match.group(3)
            result = f"{year}-{month}-{day}"
            if self._is_valid_date(result):
                return result
        
        # Pattern 2: "March 30, 2026" or "Mar 30 2026"
        match = re.match(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+(\d{1,2}),?\s+(\d{4})', s, re.IGNORECASE)
        if match:
            month = months[match.group(1).lower()]
            day = match.group(2).zfill(2)
            year = match.group(3)
            result = f"{year}-{month}-{day}"
            if self._is_valid_date(result):
                return result
        
        # Pattern 3: "19/03/2026" or "05/03/2026" (DD/MM/YYYY - Mucuruzi format)
        match = re.match(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', s)
        if match:
            day = match.group(1).zfill(2)
            month = match.group(2).zfill(2)
            year = match.group(3)
            result = f"{year}-{month}-{day}"
            if self._is_valid_date(result):
                return result
        
        # Pattern 4: "2026-03-30" or "2026/03/30" (Already correct YYYY-MM-DD format)
        match = re.match(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})', s)
        if match:
            year = match.group(1)
            month = match.group(2).zfill(2)
            day = match.group(3).zfill(2)
            result = f"{year}-{month}-{day}"
            if self._is_valid_date(result):
                return result
        
        # Pattern 5: ISO timestamp "2026-03-30T00:00:00" or "2026-02-19T17:24:02+02:00"
        if 'T' in s:
            s = s.split('T')[0]
            if self._is_valid_date(s):
                return s
        
        # If we got here, format not recognized
        return None
    
    def _is_valid_date(self, date_str: str) -> bool:
        """
        Validate that a date string in YYYY-MM-DD format is valid.
        Also checks year is reasonable (2020-2030).
        """
        try:
            from datetime import datetime
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            year = dt.year
            # Only accept dates between 2020 and 2030
            if 2020 <= year <= 2030:
                return True
            return False
        except (ValueError, AttributeError):
            return False
    
    def _batch_insert_jobs(self, jobs_data: List[tuple]):
        """Batch insert jobs for better performance"""
        insert_sql = """
            INSERT INTO jobs (
                id, title, company, description,
                location_raw, district, country,
                is_remote, is_hybrid,
                rwanda_eligible, eligibility_reason, confidence_score,
                sector, job_level, experience_years,
                employment_type, education_level,
                salary_min, salary_max, currency, salary_disclosed,
                posted_date, deadline,
                source, source_url, source_job_id,
                duplicate_hash
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        execute_batch(self.cursor, insert_sql, jobs_data, page_size=100)
        self.conn.commit()
    
    def _update_source_stats(self, source: str, jobs_added: int):
        """Update job source statistics"""
        self.cursor.execute("""
            UPDATE job_sources
            SET last_scraped = CURRENT_TIMESTAMP,
                total_jobs_scraped = total_jobs_scraped + %s
            WHERE name = %s;
        """, (jobs_added, source))
        self.conn.commit()
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        stats = {}
        
        # Total jobs
        self.cursor.execute("SELECT COUNT(*) FROM jobs;")
        stats['total_jobs'] = self.cursor.fetchone()[0]
        
        # Active jobs
        self.cursor.execute("SELECT COUNT(*) FROM jobs WHERE is_active = TRUE;")
        stats['active_jobs'] = self.cursor.fetchone()[0]
        
        # Jobs by source
        self.cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM jobs
            WHERE is_active = TRUE
            GROUP BY source
            ORDER BY count DESC;
        """)
        stats['by_source'] = {row[0]: row[1] for row in self.cursor.fetchall()}
        
        # Recent scraper runs
        self.cursor.execute("""
            SELECT source, status, jobs_new, completed_at
            FROM scraper_runs
            ORDER BY started_at DESC
            LIMIT 10;
        """)
        stats['recent_runs'] = self.cursor.fetchall()
        
        return stats


def save_scraper_output(df: pd.DataFrame, source: str, database_url: str = None) -> Dict[str, int]:
    """
    Convenience function to save scraper output to Supabase
    
    Usage in your scrapers:
        from db_adapter import save_scraper_output
        
        # After scraping
        df = scrape_jobs()  # Your existing scraper
        stats = save_scraper_output(df, source="jobinrwanda")
        print(f"Saved {stats['new']} new jobs!")
    """
    with SupabaseAdapter(database_url) as db:
        return db.save_jobs_from_dataframe(df, source)
