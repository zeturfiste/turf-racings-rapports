# -*- coding: utf-8 -*-
import os
import re
import unicodedata
import asyncio
import aiohttp
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from pathlib import Path
import subprocess

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
}

# Limites plus agressives pour re-scraping
MAX_CONCURRENT_ITEMS = 10

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
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str

async def fetch_html(session: aiohttp.ClientSession, url: str, retries=3) -> str:
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 * (attempt + 1))

def save_html(filepath: Path, html: str):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(html, encoding="utf-8")

# =========================
# Parse verification report
# =========================
def parse_verification_report(report_path: Path = Path("verification_report.txt")):
    """
    Parse le rapport de vérification pour extraire les éléments manquants
    Returns: dict with structure of missing items
    """
    if not report_path.exists():
        print(f"❌ Fichier {report_path} introuvable")
        return {}
    
    missing = {}
    current_date = None
    
    with open(report_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            
            # Detect date header
            if line.startswith("DATE:") and "STATUS:" in line:
                match = re.search(r"DATE:\s*(\d{4}-\d{2}-\d{2})", line)
                if match:
                    current_date = match.group(1)
                    missing[current_date] = {
                        "date_missing": False,
                        "reunions": {}
                    }
            
            # Detect missing items
            elif current_date and line.startswith("❌"):
                # Dossier absent complet
                if "Dossier absent:" in line:
                    match = re.search(r"Dossier absent:\s*(.+)", line)
                    if match:
                        path = match.group(1)
                        if "resultats-et-rapports" in path:
                            missing[current_date]["date_missing"] = True
                
                # Fichier date absent
                elif "Fichier date absent" in line:
                    missing[current_date]["date_missing"] = True
                
                # Dossier réunion absent
                elif "Dossier réunion absent:" in line:
                    match = re.search(r"Dossier réunion absent:\s*(.+)/", line)
                    if match:
                        reunion_slug = match.group(1)
                        if reunion_slug not in missing[current_date]["reunions"]:
                            missing[current_date]["reunions"][reunion_slug] = {
                                "reunion_missing": True,
                                "courses": []
                            }
                
                # Fichier réunion absent
                elif "Fichier réunion absent:" in line:
                    match = re.search(r"Fichier réunion absent:\s*([^/]+)/", line)
                    if match:
                        reunion_slug = match.group(1)
                        if reunion_slug not in missing[current_date]["reunions"]:
                            missing[current_date]["reunions"][reunion_slug] = {
                                "reunion_missing": True,
                                "courses": []
                            }
                
                # Course manquante
                elif "/" in line and ".html" in line:
                    match = re.search(r"❌\s*([^/]+)/([^/]+\.html)", line)
                    if match:
                        reunion_slug = match.group(1)
                        course_file = match.group(2)
                        if reunion_slug not in missing[current_date]["reunions"]:
                            missing[current_date]["reunions"][reunion_slug] = {
                                "reunion_missing": False,
                                "courses": []
                            }
                        missing[current_date]["reunions"][reunion_slug]["courses"].append(course_file)
    
    return missing

# =========================
# Re-scraping functions
# =========================
async def rescrape_date(session: aiohttp.ClientSession, date_str: str):
    """Re-scrape une date complète"""
    print(f"  📅 Re-scraping date {date_str}...")
    date_dir = get_date_directory(date_str)
    date_file = date_dir / f"{date_str}.html"
    
    url = DATE_URL_TPL.format(date=date_str)
    try:
        html = await fetch_html(session, url)
        save_html(date_file, html)
        print(f"    ✓ Date {date_str} récupérée")
        return True
    except Exception as e:
        print(f"    ✗ Erreur date {date_str}: {e}")
        return False

async def rescrape_reunion(session: aiohttp.ClientSession, date_str: str, reunion_slug: str):
    """Re-scrape une réunion complète"""
    print(f"    🏇 Re-scraping réunion {reunion_slug}...")
    
    # First ensure date file exists to get reunion URL
    date_dir = get_date_directory(date_str)
    date_file = date_dir / f"{date_str}.html"
    
    if not date_file.exists():
        await rescrape_date(session, date_str)
    
    # Parse date file to find reunion URL
    try:
        html = date_file.read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")
        container = soup.select_one("div#list-reunion")
        
        if not container:
            print(f"      ✗ Impossible de trouver les réunions pour {date_str}")
            return False
        
        # Find matching reunion
        for tr in container.select("table.programme tbody tr.item"):
            a = tr.select_one('td.numero a[data-tc-pays="FR"]')
            if not a:
                continue
            
            href = a.get("href", "").strip()
            m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
            reunion_code = m.group(1) if m else ""
            
            hippo_el = tr.select_one("td.nom h2 span span")
            hippodrome = hippo_el.get_text(strip=True) if hippo_el else ""
            current_slug = f"{reunion_code}-{slugify(hippodrome)}"
            
            if current_slug == reunion_slug:
                reunion_url = urljoin(BASE, href)
                reunion_dir = date_dir / reunion_slug
                reunion_file = reunion_dir / f"{reunion_slug}.html"
                
                reunion_html = await fetch_html(session, reunion_url)
                save_html(reunion_file, reunion_html)
                print(f"      ✓ Réunion {reunion_slug} récupérée")
                return True
        
        print(f"      ✗ Réunion {reunion_slug} non trouvée dans la date")
        return False
    
    except Exception as e:
        print(f"      ✗ Erreur réunion {reunion_slug}: {e}")
        return False

async def rescrape_course(session: aiohttp.ClientSession, date_str: str, reunion_slug: str, course_file: str):
    """Re-scrape une course spécifique"""
    print(f"      🐎 Re-scraping course {course_file}...")
    
    date_dir = get_date_directory(date_str)
    reunion_dir = date_dir / reunion_slug
    reunion_file = reunion_dir / f"{reunion_slug}.html"
    
    # Ensure reunion file exists
    if not reunion_file.exists():
        await rescrape_reunion(session, date_str, reunion_slug)
    
    # Parse reunion file to find course URL
    try:
        html = reunion_file.read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")
        frise = soup.select_one("#frise-course .strip2.active") or soup.select_one("#frise-course .strip2")
        
        if not frise:
            print(f"        ✗ Impossible de trouver les courses pour {reunion_slug}")
            return False
        
        # Extract reunion code from slug
        reunion_code = reunion_slug.split("-")[0]
        
        # Find matching course
        for a in frise.select("ul.scroll-content li.scroll-element a"):
            href = a.get("href", "")
            if not href:
                continue
            
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
            slug = slugify(intitule) if intitule else "course"
            code_part = f"{reunion_code}{(code or '').upper()}"
            filename = f"{code_part}-{slug}.html"
            
            if filename == course_file:
                course_url = urljoin(BASE, href)
                course_path = reunion_dir / course_file
                
                course_html = await fetch_html(session, course_url)
                save_html(course_path, course_html)
                print(f"        ✓ Course {course_file} récupérée")
                return True
        
        print(f"        ✗ Course {course_file} non trouvée dans la réunion")
        return False
    
    except Exception as e:
        print(f"        ✗ Erreur course {course_file}: {e}")
        return False

# =========================
# Main orchestrator
# =========================
async def rescrape_missing():
    """Re-scrape tous les éléments manquants du rapport"""
    print("="*80)
    print("RE-SCRAPING DES DONNÉES MANQUANTES")
    print("="*80 + "\n")
    
    # Parse report
    missing = parse_verification_report()
    
    if not missing:
        print("✓ Aucune donnée manquante détectée ou rapport introuvable")
        return
    
    print(f"📊 {len(missing)} dates avec des problèmes détectés\n")
    
    stats = {
        "dates_fixed": 0,
        "reunions_fixed": 0,
        "courses_fixed": 0,
        "errors": 0
    }
    
    async with aiohttp.ClientSession() as session:
        for date_str, issues in sorted(missing.items()):
            print(f"\n{'='*80}")
            print(f"DATE: {date_str}")
            print(f"{'='*80}")
            
            # Re-scrape date if missing
            if issues["date_missing"]:
                success = await rescrape_date(session, date_str)
                if success:
                    stats["dates_fixed"] += 1
                else:
                    stats["errors"] += 1
                await asyncio.sleep(0.5)
            
            # Re-scrape reunions and courses
            if issues["reunions"]:
                tasks = []
                
                for reunion_slug, reunion_issues in issues["reunions"].items():
                    if reunion_issues["reunion_missing"]:
                        tasks.append(rescrape_reunion(session, date_str, reunion_slug))
                        stats["reunions_fixed"] += 1
                    elif reunion_issues["courses"]:
                        for course_file in reunion_issues["courses"]:
                            tasks.append(rescrape_course(session, date_str, reunion_slug, course_file))
                            stats["courses_fixed"] += 1
                
                # Execute in batches
                for i in range(0, len(tasks), MAX_CONCURRENT_ITEMS):
                    batch = tasks[i:i + MAX_CONCURRENT_ITEMS]
                    await asyncio.gather(*batch, return_exceptions=True)
                    await asyncio.sleep(0.5)
    
    # Print summary
    print("\n" + "="*80)
    print("RÉSUMÉ DU RE-SCRAPING")
    print("="*80)
    print(f"✓ Dates récupérées:     {stats['dates_fixed']}")
    print(f"✓ Réunions récupérées:  {stats['reunions_fixed']}")
    print(f"✓ Courses récupérées:   {stats['courses_fixed']}")
    print(f"✗ Erreurs:              {stats['errors']}")
    print("="*80 + "\n")

def git_commit_push():
    """Commit and push changes"""
    print("📤 Git commit & push...")
    try:
        subprocess.run(["git", "config", "user.name", "GitHub Actions Bot"], check=True)
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "add", REPO_ROOT], check=True)
        subprocess.run(["git", "commit", "-m", f"Re-scrape missing data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("✓ Changements committés et pushés\n")
    except subprocess.CalledProcessError as e:
        print(f"✗ Erreur Git: {e}\n")

async def main():
    await rescrape_missing()
    git_commit_push()

if __name__ == "__main__":
    asyncio.run(main())
