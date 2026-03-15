"""
Rwanda Jobs - Master Scraper Runner with Auto-Deduplication
============================================================
Runs all 5 scrapers and automatically removes duplicate jobs.

Features:
- Runs all scrapers in sequence
- Saves results to database
- Automatically detects and removes duplicate jobs
- Shows summary statistics

Usage:
    set DATABASE_URL=postgresql://...
    python run_all_5_scrapers.py
"""

import os
import sys
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
import difflib

# Add scrapers directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scrapers'))

from jobinrwanda_scraper import JobScraper
from newtimesjobs_scraper import NewTimesScraper
from greatrwandajobs_scraper import GreatRwandaJobsScraper
from mucuruzi_scraper import MucuruziScraper
from mifotra_scraper import MifotraScraper
from db_adapter import save_scraper_output


def normalize_text(text):
    """Normalize text for comparison"""
    if not text:
        return ""
    return " ".join(text.lower().split())


def normalize_company(company):
    """Normalize company name for matching"""
    if not company:
        return ""
    
    import re
    
    # Extract acronym if present: "Full Name (ACRONYM) Location" → "ACRONYM"
    acronym_match = re.search(r'\(([A-Z]{2,10})\)', company, re.IGNORECASE)
    if acronym_match:
        return acronym_match.group(1).upper()
    
    return " ".join(company.lower().split())


def remove_duplicates(database_url):
    """
    Automatically remove duplicate jobs after scraping.
    Keeps the most recent version of each job.
    NO confirmation required - runs automatically.
    """
    print("\n" + "="*70)
    print("🧹 STEP 6: Auto-Removing Duplicate Jobs")
    print("="*70)
    
    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()
        
        # Get all active jobs
        cur.execute("""
            SELECT 
                id, title, company, deadline, source, scraped_at
            FROM jobs
            WHERE is_active = true
            ORDER BY scraped_at DESC;
        """)
        
        jobs = []
        for row in cur.fetchall():
            jobs.append({
                'id': row[0],
                'title': row[1],
                'company': row[2],
                'deadline': row[3],
                'source': row[4],
                'scraped_at': row[5]
            })
        
        print(f"📊 Analyzing {len(jobs)} jobs for duplicates...")
        
        # Find duplicates
        to_delete = []
        processed = set()
        duplicate_groups = []
        
        for i, job1 in enumerate(jobs):
            if job1['id'] in processed:
                continue
            
            group = [job1]
            
            for job2 in jobs[i+1:]:
                if job2['id'] in processed:
                    continue
                
                # Check if duplicate
                title1 = normalize_text(job1['title'])
                title2 = normalize_text(job2['title'])
                company1 = normalize_company(job1['company'])
                company2 = normalize_company(job2['company'])
                
                is_duplicate = False
                
                # Rule 1: Same title + same company
                if title1 == title2 and company1 and company2 and company1 == company2:
                    is_duplicate = True
                
                # Rule 2: Very similar titles (>90%) + same company
                elif company1 and company2 and company1 == company2:
                    similarity = difflib.SequenceMatcher(None, title1, title2).ratio()
                    if similarity > 0.9:
                        is_duplicate = True
                
                # Rule 3: Same title + same deadline
                elif title1 == title2 and job1['deadline'] and job2['deadline']:
                    if job1['deadline'] == job2['deadline']:
                        is_duplicate = True
                
                if is_duplicate:
                    group.append(job2)
                    processed.add(job2['id'])
            
            # If duplicates found, mark older ones for deletion
            if len(group) > 1:
                duplicate_groups.append(group)
                
                # Sort by scraped_at descending (keep newest)
                group_sorted = sorted(group, key=lambda x: x['scraped_at'], reverse=True)
                
                # Delete all except the first (newest)
                for job in group_sorted[1:]:
                    to_delete.append(job['id'])
                
                processed.add(job1['id'])
        
        # Show what we found (preview only - no confirmation needed)
        if duplicate_groups:
            print(f"\n🔍 Found {len(duplicate_groups)} groups with duplicates")
            
            # Show a few examples
            for i, group in enumerate(duplicate_groups[:3], 1):
                title_preview = group[0]['title'][:45] + "..." if len(group[0]['title']) > 45 else group[0]['title']
                company_preview = group[0]['company'][:25] if group[0]['company'] else "N/A"
                print(f"   {i}. '{title_preview}' at {company_preview} ({len(group)} copies)")
            
            if len(duplicate_groups) > 3:
                print(f"   ... and {len(duplicate_groups) - 3} more")
        
        # Delete duplicates automatically (NO confirmation)
        if to_delete:
            print(f"\n🗑️  Removing {len(to_delete)} older duplicates (keeping newest versions)...")
            
            # Delete in batches for better performance
            batch_size = 100
            for i in range(0, len(to_delete), batch_size):
                batch = to_delete[i:i+batch_size]
                cur.execute(
                    "DELETE FROM jobs WHERE id = ANY(%s);",
                    (batch,)
                )
            
            conn.commit()
            print(f"✅ Successfully removed {len(to_delete)} duplicates")
        else:
            print("✅ No duplicates found - database is clean!")
        
        # Get final count
        cur.execute("SELECT COUNT(*) FROM jobs WHERE is_active = true;")
        final_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        return len(to_delete), final_count
        
    except Exception as e:
        print(f"❌ Error during deduplication: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0


def main():
    print("\n" + "="*70)
    print("🇷🇼 RWANDA JOBS - COMPLETE SCRAPING PIPELINE")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Check database URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("\n❌ DATABASE_URL environment variable not set!")
        print("Run: set DATABASE_URL=postgresql://...")
        return
    
    scrapers = [
        ("JobInRwanda", JobScraper),
        ("NewTimes", NewTimesScraper),
        ("GreatRwandaJobs", GreatRwandaJobsScraper),
        ("Mucuruzi", MucuruziScraper),
        ("MIFOTRA", MifotraScraper),
    ]
    
    results = {}
    total_found = 0
    total_new = 0
    total_skipped = 0
    
    # Run each scraper
    for i, (name, ScraperClass) in enumerate(scrapers, 1):
        print(f"\n{'='*70}")
        print(f"STEP {i}/5: Scraping {name}")
        print("="*70)
        
        try:
            start_time = time.time()
            
            # Create scraper and run
            scraper = ScraperClass()
            df = scraper.scrape()
            
            # Save to database
            if not df.empty:
                stats = save_scraper_output(df, source=name.lower().replace(' ', ''))
                results[name] = stats
                total_found += stats.get('found', 0)
                total_new += stats.get('new', 0)
                total_skipped += stats.get('skipped', 0)
                
                elapsed = time.time() - start_time
                print(f"✅ {name}: {stats['found']} found, {stats['new']} new, {stats['skipped']} duplicates ({elapsed:.1f}s)")
            else:
                print(f"⚠️  {name}: No jobs found")
                results[name] = {'found': 0, 'new': 0, 'skipped': 0}
                
        except Exception as e:
            print(f"❌ {name} failed: {e}")
            results[name] = {'found': 0, 'new': 0, 'skipped': 0, 'error': str(e)}
    
    # Remove duplicates
    duplicates_removed, final_count = remove_duplicates(database_url)
    
    # Final summary
    print("\n" + "="*70)
    print("📊 FINAL SUMMARY")
    print("="*70)
    print(f"\nScraping Results:")
    for name, stats in results.items():
        if 'error' in stats:
            print(f"  ❌ {name:20} - Failed: {stats['error'][:50]}")
        else:
            print(f"  ✅ {name:20} - Found: {stats['found']:3}, New: {stats['new']:3}, Skipped: {stats['skipped']:3}")
    
    print(f"\nTotals:")
    print(f"  📥 Jobs found:         {total_found}")
    print(f"  ✨ New jobs added:     {total_new}")
    print(f"  ⏭️  Already existed:    {total_skipped}")
    print(f"  🗑️  Duplicates removed: {duplicates_removed}")
    print(f"  📊 Final active jobs:  {final_count}")
    
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Suggest next steps
    print("\n💡 Next Steps:")
    print("   1. Run: python multi_page_dashboard.py")
    print("   2. Open: http://localhost:8050")
    print("   3. View your clean, deduplicated job data!")
    print("\n" + "="*70)


if __name__ == "__main__":
    main()