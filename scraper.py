# -*- coding: utf-8 -*-
import os
import re
import unicodedata
import asyncio
import aiohttp
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from pathlib import Path
import subprocess
import sys

# =========================
# Config
# =========================
BASE = "https://www.zeturf.fr"
DATE_URL_TPL = BASE + "/fr/resultats-et-rapports/{date}"
REPO_ROOT = "resultats-et-rapports"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
}

# Limites pour ne pas surcharger GitHub et zeturf.fr
MAX_CONCURRENT_DATES = 3
MAX_CONCURRENT_REUNIONS = 4
MAX_CONCURRENT_COURSES = 8

# =========================
# Helpers
# =========================
def slugify(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text

def get_date_directory(date_str: str) -> Path:
    """Returns: resultats-et-rapports/2025/11/2025-11-10/"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str

def file_exists_and_valid(filepath: Path) -> bool:
    """Check if file exists and has content (>100 bytes to avoid empty/error pages)"""
    try:
        return filepath.exists() and filepath.stat().st_size > 100
    except:
        return False

def date_range_asc(start_date: str, end_date: str):
    """Generate dates in ascending order"""
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_date, "%Y-%m-%d").date()
    if d0 > d1:
        d0, d1 = d1, d0
    days = (d1 - d0).days
    return [(d0 + timedelta(days=i)).isoformat() for i in range(days + 1)]

def group_by_year(dates):
    """Group dates by year"""
    years = {}
    for date_str in dates:
        year = date_str[:4]
        if year not in years:
            years[year] = []
        years[year].append(date_str)
    return years

# =========================
# Async scrapers
# =========================
async def fetch_html(session: aiohttp.ClientSession, url: str, retries=3) -> str:
    """Fetch HTML with retries and error handling"""
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                resp.raise_for_status()
                text = await resp.text()
                # Validation minimale : vérifier que c'est du HTML
                if len(text) < 100 or '<html' not in text.lower():
                    raise ValueError(f"Invalid HTML content (too short or not HTML)")
                return text
        except Exception as e:
            if attempt == retries - 1:
                print(f"    ✗ Failed after {retries} attempts: {url} - {e}")
                raise
            await asyncio.sleep(1.5 * (attempt + 1))

def save_html(filepath: Path, html: str):
    """Save HTML to file with atomic write"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    # Write to temp file first, then rename (atomic operation)
    temp_file = filepath.with_suffix('.tmp')
    temp_file.write_text(html, encoding="utf-8")
    temp_file.replace(filepath)

async def scrape_date(session: aiohttp.ClientSession, date_str: str):
    """Scrape one date and return reunions FR"""
    date_dir = get_date_directory(date_str)
    date_file = date_dir / f"{date_str}.html"
    
    # Check if already exists and valid
    if file_exists_and_valid(date_file):
        print(f"  ⏭ Skip date {date_str} (already exists)")
        html = date_file.read_text(encoding="utf-8")
    else:
        url = DATE_URL_TPL.format(date=date_str)
        try:
            html = await fetch_html(session, url)
            save_html(date_file, html)
            print(f"  ✓ Downloaded date {date_str}")
        except Exception as e:
            print(f"  ✗ Error date {date_str}: {e}")
            return []
    
    soup = BeautifulSoup(html, "lxml")
    container = soup.select_one("div#list-reunion")
    if not container:
        return []
    
    reunions = []
    for tr in container.select("table.programme tbody tr.item"):
        a = tr.select_one('td.numero a[data-tc-pays="FR"]')
        if not a:
            continue
        
        href = a.get("href", "").strip()
        if not href:
            continue
        
        reunion_url = urljoin(BASE, href)
        m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
        reunion_code = m.group(1) if m else (a.get_text(strip=True).replace("FR", "R"))
        
        hippo_el = tr.select_one("td.nom h2 span span")
        hippodrome = hippo_el.get_text(strip=True) if hippo_el else ""
        reunion_slug = f"{reunion_code}-{slugify(hippodrome)}"
        
        reunions.append({
            "date": date_str,
            "reunion_code": reunion_code,
            "hippodrome": hippodrome,
            "url": reunion_url,
            "reunion_slug": reunion_slug,
            "date_dir": date_dir,
        })
    
    return reunions

async def scrape_reunion(session: aiohttp.ClientSession, reunion: dict):
    """Scrape one reunion and return courses"""
    reunion_dir = reunion["date_dir"] / reunion["reunion_slug"]
    reunion_file = reunion_dir / f"{reunion['reunion_slug']}.html"
    
    # Check if already exists and valid
    if file_exists_and_valid(reunion_file):
        print(f"    ⏭ Skip reunion {reunion['reunion_slug']} (exists)")
        html = reunion_file.read_text(encoding="utf-8")
    else:
        try:
            html = await fetch_html(session, reunion["url"])
            save_html(reunion_file, html)
            print(f"    ✓ Downloaded reunion {reunion['reunion_slug']}")
        except Exception as e:
            print(f"    ✗ Error reunion {reunion['reunion_slug']}: {e}")
            return []
    
    soup = BeautifulSoup(html, "lxml")
    frise = soup.select_one("#frise-course .strip2.active") or soup.select_one("#frise-course .strip2")
    if not frise:
        return []
    
    courses = []
    for a in frise.select("ul.scroll-content li.scroll-element a"):
        href = a.get("href", "")
        if not href:
            continue
        
        url = urljoin(BASE, href)
        
        numero_txt = a.select_one("span.numero")
        numero_txt = numero_txt.get_text(strip=True) if numero_txt else ""
        mC = re.search(r"C(\d+)", href)
        numero = int(numero_txt) if numero_txt.isdigit() else (int(mC.group(1)) if mC else None)
        
        title = a.get("title", "").strip()
        heure, intitule = None, None
        if " - " in title:
            heure, intitule = title.split(" - ", 1)
        else:
            intitule = title or None
        
        code = f"C{numero}" if numero is not None else (mC.group(0) if mC else None)
        
        # Build filename
        slug = slugify(intitule) if intitule else ""
        if not slug:
            slug = "course"
        code_part = f"{reunion['reunion_code']}{(code or '').upper()}"
        filename = f"{code_part}-{slug}.html"
        
        courses.append({
            "url": url,
            "filename": filename,
            "reunion_dir": reunion_dir,
        })
    
    return courses

async def scrape_course(session: aiohttp.ClientSession, course: dict):
    """Scrape one course page"""
    filepath = course["reunion_dir"] / course["filename"]
    
    # Skip if already exists and valid
    if file_exists_and_valid(filepath):
        return f"⏭ {course['filename']}"
    
    try:
        html = await fetch_html(session, course["url"])
        save_html(filepath, html)
        return f"✓ {course['filename']}"
    except Exception as e:
        return f"✗ {course['filename']}: {e}"

# =========================
# Main orchestrator
# =========================
async def scrape_year(year: str, dates: list):
    """Scrape all dates for one year with parallelization"""
    print(f"\n{'='*60}")
    print(f"ANNÉE {year} - {len(dates)} dates")
    print(f"{'='*60}\n")
    
    stats = {"dates": 0, "reunions": 0, "courses": 0, "skipped": 0, "errors": 0}
    
    connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Process dates in batches
        for i in range(0, len(dates), MAX_CONCURRENT_DATES):
            date_batch = dates[i:i + MAX_CONCURRENT_DATES]
            
            print(f"Processing dates: {', '.join(date_batch)}")
            
            # Fetch all reunions for this batch of dates
            date_tasks = [scrape_date(session, date_str) for date_str in date_batch]
            all_reunions_lists = await asyncio.gather(*date_tasks, return_exceptions=True)
            
            # Flatten reunions
            all_reunions = []
            for result in all_reunions_lists:
                if isinstance(result, Exception):
                    print(f"  ✗ Error fetching date: {result}")
                    stats["errors"] += 1
                else:
                    all_reunions.extend(result)
                    stats["dates"] += 1
            
            if not all_reunions:
                continue
            
            # Process reunions in batches
            for j in range(0, len(all_reunions), MAX_CONCURRENT_REUNIONS):
                reunion_batch = all_reunions[j:j + MAX_CONCURRENT_REUNIONS]
                
                reunion_tasks = [scrape_reunion(session, reunion) for reunion in reunion_batch]
                all_courses_lists = await asyncio.gather(*reunion_tasks, return_exceptions=True)
                
                # Flatten courses
                all_courses = []
                for result in all_courses_lists:
                    if isinstance(result, Exception):
                        print(f"    ✗ Error fetching reunion: {result}")
                        stats["errors"] += 1
                    else:
                        all_courses.extend(result)
                        if result:
                            stats["reunions"] += 1
                
                if not all_courses:
                    continue
                
                # Process courses in batches
                for k in range(0, len(all_courses), MAX_CONCURRENT_COURSES):
                    course_batch = all_courses[k:k + MAX_CONCURRENT_COURSES]
                    course_tasks = [scrape_course(session, course) for course in course_batch]
                    results = await asyncio.gather(*course_tasks, return_exceptions=True)
                    
                    for result in results:
                        if isinstance(result, Exception):
                            stats["errors"] += 1
                        elif "✓" in str(result):
                            stats["courses"] += 1
                        elif "⏭" in str(result):
                            stats["skipped"] += 1
            
            # Small delay between date batches
            await asyncio.sleep(0.5)
    
    print(f"\n{'='*60}")
    print(f"Stats année {year}:")
    print(f"  Dates: {stats['dates']}")
    print(f"  Réunions: {stats['reunions']}")
    print(f"  Courses: {stats['courses']} downloaded, {stats['skipped']} skipped")
    print(f"  Errors: {stats['errors']}")
    print(f"{'='*60}\n")
    
    return stats

def git_commit_push(year: str):
    """Commit and push changes for the year"""
    print(f"\n{'='*60}")
    print(f"Git commit & push pour l'année {year}")
    print(f"{'='*60}\n")
    
    try:
        subprocess.run(["git", "config", "user.name", "GitHub Actions"], check=True)
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        
        # Add only this year's folder
        year_path = f"{REPO_ROOT}/{year}"
        subprocess.run(["git", "add", year_path], check=True)
        
        # Check if there are changes to commit
        result = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"⚠ Aucun changement pour l'année {year} (déjà scrapée)\n")
            return False
        
        # Commit and push
        subprocess.run(
            ["git", "commit", "-m", f"Add ZEturf data for year {year}"],
            check=True
        )
        subprocess.run(["git", "push"], check=True)
        print(f"✓ Année {year} committée et pushée avec succès\n")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Erreur Git: {e}")
        print(f"  stdout: {e.stdout if hasattr(e, 'stdout') else 'N/A'}")
        print(f"  stderr: {e.stderr if hasattr(e, 'stderr') else 'N/A'}\n")
        return False

async def main():
    """Main function - process one year at a time via env var"""
    
    # Get year from environment or command line
    target_year = os.environ.get("SCRAPE_YEAR")
    if not target_year and len(sys.argv) > 1:
        target_year = sys.argv[1]
    
    if not target_year:
        print("❌ ERREUR: Variable SCRAPE_YEAR manquante")
        print("Usage: SCRAPE_YEAR=2005 python scraper.py")
        print("   ou: python scraper.py 2005")
        sys.exit(1)
    
    start_date = "2005-04-27"
    end_date = "2025-11-11"
    
    print(f"\n{'='*60}")
    print(f"ZEturf Scraper - Année {target_year}")
    print(f"Période totale: {start_date} → {end_date}")
    print(f"{'='*60}\n")
    
    # Generate all dates and filter by target year
    all_dates = date_range_asc(start_date, end_date)
    years_dict = group_by_year(all_dates)
    
    if target_year not in years_dict:
        print(f"❌ Aucune date trouvée pour l'année {target_year}")
        sys.exit(1)
    
    dates = years_dict[target_year]
    stats = await scrape_year(target_year, dates)
    
    # Only commit if there were actual downloads
    if stats["courses"] > 0 or stats["reunions"] > 0:
        git_commit_push(target_year)
    else:
        print(f"⚠ Aucune nouvelle donnée pour {target_year}, pas de commit")
    
    print("\n" + "="*60)
    print(f"✅ SCRAPING TERMINÉ POUR {target_year}")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())
