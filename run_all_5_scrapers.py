"""
Rwanda Jobs - Master Scraper Runner with Auto-Deduplication
============================================================
Runs all 9 scrapers and automatically removes duplicate jobs.

Features:
- Runs all scrapers in sequence
- Saves results to database
- Automatically detects and removes duplicate jobs
- Shows summary statistics

Usage:
    set DATABASE_URL=postgresql://...
    python run_all_scrapers.py
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
from jobskazi_scraper import JobScraper as JobsKaziScraper
from unjobs_scraper import JobScraper as UNJobsScraper
from impactpool_scraper import JobScraper as ImpactPoolScraper
from jobnziza_scraper import JobScraper as JobNzizaScraper
from db_adapter import save_scraper_output


def normalize_text(text):
    """Normalize text for comparison"""
    if not text:
        return ""
    return " ".join(text.lower().split())


def normalize_company(company):
    """
    Normalise company name for cross-source duplicate matching.
    Strategy:
      1. Extract acronym if present: "Catholic Relief Services (CRS)" -> "CRS"
      2. Strip common suffixes (Rwanda, Ltd, Plc, etc.) and normalise
    """
    if not company:
        return ""

    import re

    # Extract acronym if present: "Full Name (ACRONYM)" -> "ACRONYM"
    acronym_match = re.search(r'\(([A-Z]{2,10})\)', company, re.IGNORECASE)
    if acronym_match:
        return acronym_match.group(1).upper()

    # Strip common suffixes that vary between scrapers
    cleaned = re.sub(
        r'\b(ltd|plc|inc|llc|limited|rwanda|plc|formerly.*)\b.*$',
        '', company, flags=re.IGNORECASE
    ).strip().rstrip('.,- ')

    return " ".join(cleaned.lower().split())


def companies_match(c1_raw, c2_raw):
    """
    Return True if two company name strings refer to the same organisation.
    Handles:
      - Exact normalised match
      - Acronym extraction  ("GIZ Rwanda" vs "Deutsche...(GIZ)")
      - Substring containment ("Alight" vs "Alight (formerly...)")
      - Fuzzy match for minor spelling differences
    """
    if not c1_raw or not c2_raw:
        return False

    n1 = normalize_company(c1_raw)
    n2 = normalize_company(c2_raw)

    # Exact normalised match (includes acronym-based)
    if n1 and n2 and n1 == n2:
        return True

    # Substring containment - handles "Alight" vs "Alight (formerly...)"
    # Use the base name (before any parenthesis)
    b1 = c1_raw.lower().split('(')[0].strip()
    b2 = c2_raw.lower().split('(')[0].strip()
    if b1 and b2 and len(b1) >= 4 and len(b2) >= 4:
        if b1 in b2 or b2 in b1:
            return True

    # Fuzzy match on normalised names (>85% similarity)
    if n1 and n2 and len(n1) >= 4 and len(n2) >= 4:
        sim = difflib.SequenceMatcher(None, n1, n2).ratio()
        if sim > 0.85:
            return True

    return False


def normalize_deadline(deadline):
    """
    Normalise deadline for cross-scraper duplicate comparison.
    Handles ALL types returned by the DB or scrapers:
      - datetime.date / datetime.datetime objects (from psycopg2)
      - 'on 27-03-2026', '27/03/2026', '27-03-2026 17:00' (scraper strings)
    Returns: 'YYYY-MM-DD' string, or '' if empty/unparseable.
    """
    if not deadline:
        return ""
    import re
    from datetime import date, datetime

    # Handle datetime.date / datetime.datetime objects from psycopg2
    if isinstance(deadline, (date, datetime)):
        return deadline.strftime('%Y-%m-%d')

    d = str(deadline).strip().lower()
    d = d.removeprefix("on ").strip()   # strip "on " prefix
    d = re.sub(r'[/.]', '-', d)         # normalise separators to -
    d = d.split()[0]                     # strip time component
    return d


def remove_duplicates(database_url):
    """
    Automatically remove cross-source duplicate jobs after scraping.
    Keeps the most recently scraped version of each job.
    NO confirmation required - runs automatically.

    Duplicate detection rules (in order):
      Rule 1: Same normalised title + matching company name
              (handles: exact, acronym, substring, fuzzy company variants)
      Rule 2: Very similar title (>90%) + matching company name
      Rule 3: Same title + same normalised deadline
              (catches cases where company names differ too much to match,
               e.g. 'GIZ Rwanda' vs full German name)
    """
    print("\n" + "="*70)
    print("[CLEAN] STEP 10: Auto-Removing Duplicate Jobs")
    print("="*70)

    try:
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        # Get all active jobs
        cur.execute("""
            SELECT id, title, company, deadline, source, scraped_at
            FROM jobs
            WHERE is_active = true
            ORDER BY scraped_at DESC;
        """)

        jobs = []
        for row in cur.fetchall():
            jobs.append({
                'id':         row[0],
                'title':      row[1],
                'company':    row[2],
                'deadline':   row[3],
                'source':     row[4],
                'scraped_at': row[5]
            })

        print(f"[STATS] Analyzing {len(jobs)} jobs for cross-source duplicates...")

        to_delete        = []
        processed        = set()
        duplicate_groups = []

        for i, job1 in enumerate(jobs):
            if job1['id'] in processed:
                continue

            group = [job1]

            for job2 in jobs[i+1:]:
                if job2['id'] in processed:
                    continue

                title1 = normalize_text(job1['title'])
                title2 = normalize_text(job2['title'])
                dl1    = normalize_deadline(job1.get('deadline', ''))
                dl2    = normalize_deadline(job2.get('deadline', ''))

                is_duplicate = False

                # Rule 1: Same title + matching company
                if title1 == title2 and companies_match(job1['company'], job2['company']):
                    is_duplicate = True

                # Rule 2: Very similar title (>90%) + matching company
                elif companies_match(job1['company'], job2['company']):
                    similarity = difflib.SequenceMatcher(None, title1, title2).ratio()
                    if similarity > 0.9:
                        is_duplicate = True

                # Rule 3: Same title + same deadline
                # (catches cross-source where company names are too different to match)
                elif title1 == title2 and dl1 and dl2 and dl1 == dl2:
                    is_duplicate = True

                if is_duplicate:
                    group.append(job2)
                    processed.add(job2['id'])

            if len(group) > 1:
                duplicate_groups.append(group)

                # Keep newest scraped version
                group_sorted = sorted(group, key=lambda x: x['scraped_at'], reverse=True)
                for job in group_sorted[1:]:
                    to_delete.append(job['id'])

                processed.add(job1['id'])

        # Preview
        if duplicate_groups:
            print(f"\n[SEARCH] Found {len(duplicate_groups)} duplicate groups")
            for i, group in enumerate(duplicate_groups[:3], 1):
                title_preview   = group[0]['title'][:45] + "..." if len(group[0]['title']) > 45 else group[0]['title']
                company_preview = group[0]['company'][:25] if group[0]['company'] else "N/A"
                sources         = " + ".join(set(j['source'] for j in group))
                print(f"   {i}. '{title_preview}' at {company_preview}")
                print(f"      Sources: {sources} ({len(group)} copies)")
            if len(duplicate_groups) > 3:
                print(f"   ... and {len(duplicate_groups) - 3} more")

        # Delete
        if to_delete:
            print(f"\n[DELETE]  Removing {len(to_delete)} duplicates (keeping newest per job)...")
            batch_size = 100
            for i in range(0, len(to_delete), batch_size):
                batch = to_delete[i:i+batch_size]
                cur.execute("DELETE FROM jobs WHERE id = ANY(%s);", (batch,))
            conn.commit()
            print(f"[OK] Successfully removed {len(to_delete)} duplicates")
        else:
            print("[OK] No duplicates found - database is clean!")

        cur.execute("SELECT COUNT(*) FROM jobs WHERE is_active = true;")
        final_count = cur.fetchone()[0]

        cur.close()
        conn.close()

        return len(to_delete), final_count

    except Exception as e:
        print(f"[ERROR] Error during deduplication: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0


def main():
    print("\n" + "="*70)
    print("[RW] RWANDA JOBS - COMPLETE SCRAPING PIPELINE")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Check database URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("\n[ERROR] DATABASE_URL environment variable not set!")
        print("Run: set DATABASE_URL=postgresql://...")
        return
    
    scrapers = [
        ("JobInRwanda",     JobScraper),
        ("NewTimes",        NewTimesScraper),
        ("GreatRwandaJobs", GreatRwandaJobsScraper),
        ("Mucuruzi",        MucuruziScraper),
        ("MIFOTRA",         MifotraScraper),
        ("JobsKazi",        JobsKaziScraper),
        ("UNJobs",          UNJobsScraper),
        ("ImpactPool",      ImpactPoolScraper),
        ("JobNziza",        JobNzizaScraper),
    ]
    
    results = {}
    total_found = 0
    total_new = 0
    total_skipped = 0
    
    # Run each scraper
    for i, (name, ScraperClass) in enumerate(scrapers, 1):
        print(f"\n{'='*70}")
        print(f"STEP {i}/{len(scrapers)}: Scraping {name}")
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
                print(f"[OK] {name}: {stats['found']} found, {stats['new']} new, {stats['skipped']} duplicates ({elapsed:.1f}s)")
            else:
                print(f"[WARN]  {name}: No jobs found")
                results[name] = {'found': 0, 'new': 0, 'skipped': 0}
                
        except Exception as e:
            print(f"[ERROR] {name} failed: {e}")
            results[name] = {'found': 0, 'new': 0, 'skipped': 0, 'error': str(e)}
    
    # Remove duplicates
    duplicates_removed, final_count = remove_duplicates(database_url)
    
    # Final summary
    print("\n" + "="*70)
    print("[STATS] FINAL SUMMARY")
    print("="*70)
    print(f"\nScraping Results:")
    for name, stats in results.items():
        if 'error' in stats:
            print(f"  [ERROR] {name:20} - Failed: {stats['error'][:50]}")
        else:
            print(f"  [OK] {name:20} - Found: {stats['found']:3}, New: {stats['new']:3}, Skipped: {stats['skipped']:3}")
    
    print(f"\nTotals:")
    print(f"  [IN] Jobs found:         {total_found}")
    print(f"  [NEW] New jobs added:     {total_new}")
    print(f"  [SKIP]  Already existed:    {total_skipped}")
    print(f"  [DELETE]  Duplicates removed: {duplicates_removed}")
    print(f"  [STATS] Final active jobs:  {final_count}")
    
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Suggest next steps
    print("\n[TIP] Next Steps:")
    print("   1. Run: python multi_page_dashboard.py")
    print("   2. Open: http://localhost:8050")
    print("   3. View your clean, deduplicated job data!")
    print("\n" + "="*70)


if __name__ == "__main__":
    main()
