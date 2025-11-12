# scraper.py
# -*- coding: utf-8 -*-
import os
import re
import unicodedata
import asyncio
import aiohttp
from datetime import datetime, timedelta, date
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from pathlib import Path
import subprocess
import random

# =========================
# Config (paramétrable par env)
# =========================
BASE = "https://www.zeturf.fr"
DATE_URL_TPL = BASE + "/fr/resultats-et-rapports/{date}"

# Dossier *dans le repo* (le repo s'appelle visuellement "ZEturf" sur GitHub,
# mais le dossier racine ici EST ce repo — ne pas préfixer par "ZEturf/").
DATA_DIRNAME = os.environ.get("DATA_DIRNAME", "resultats-et-rapports")

# Fenêtre de dates
ENV_START = os.environ.get("START_DATE")  # ex: "2024-01-01"
ENV_END = os.environ.get("END_DATE")      # ex: "2024-01-31"

# Defaults malins : scrape depuis le 2005-04-27 jusqu'à hier (UTC)
DEFAULT_START = "2005-04-27"
DEFAULT_END = (date.today() - timedelta(days=1)).isoformat()

START_DATE = ENV_START or DEFAULT_START
END_DATE = ENV_END or DEFAULT_END

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
}

# Limites pour ne pas surcharger
MAX_CONCURRENT_DATES = int(os.environ.get("MAX_CONCURRENT_DATES", "2"))
MAX_CONCURRENT_REUNIONS = int(os.environ.get("MAX_CONCURRENT_REUNIONS", "20"))
MAX_CONCURRENT_COURSES = int(os.environ.get("MAX_CONCURRENT_COURSES", "200"))

# =========================
# Helpers
# =========================
REPO_ROOT = Path.cwd()
DATA_ROOT = REPO_ROOT / DATA_DIRNAME

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
    return DATA_ROOT / year / month / date_str

def file_exists(filepath: Path) -> bool:
    return filepath.exists() and filepath.stat().st_size > 0

def date_range_asc(start_date: str, end_date: str):
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_date, "%Y-%m-%d").date()
    if d0 > d1:
        d0, d1 = d1, d0
    days = (d1 - d0).days
    return [(d0 + timedelta(days=i)).isoformat() for i in range(days + 1)]

def group_by_year(dates):
    years = {}
    for date_str in dates:
        year = date_str[:4]
        years.setdefault(year, []).append(date_str)
    return years

def jitter_sleep_base():
    # Petit jitter pour être poli avec le site
    return 0.25 + random.random() * 0.5

# =========================
# Async scrapers
# =========================
async def fetch_html(session: aiohttp.ClientSession, url: str, retries=3) -> str:
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=40)) as resp:
                resp.raise_for_status()
                return await resp.text()
        except Exception as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep((attempt + 1) * 1.5 + jitter_sleep_base())

def save_html(filepath: Path, html: str):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(html, encoding="utf-8")

async def scrape_date(session: aiohttp.ClientSession, date_str: str):
    date_dir = get_date_directory(date_str)
    date_file = date_dir / f"{date_str}.html"

    if file_exists(date_file):
        html = date_file.read_text(encoding="utf-8")
    else:
        url = DATE_URL_TPL.format(date=date_str)
        html = await fetch_html(session, url)
        save_html(date_file, html)

    soup = BeautifulSoup(html, "lxml")
    container = soup.select_one("div#list-reunion")
    if not container:
        return []

    reunions = []
    for tr in container.select("table.programme tbody tr.item"):
        a = tr.select_one('td.numero a[data-tc-pays="FR"]')
        if not a:
            continue

        href = (a.get("href") or "").strip()
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

    await asyncio.sleep(jitter_sleep_base())
    return reunions

async def scrape_reunion(session: aiohttp.ClientSession, reunion: dict):
    reunion_dir = reunion["date_dir"] / reunion["reunion_slug"]
    reunion_file = reunion_dir / f"{reunion['reunion_slug']}.html"

    if file_exists(reunion_file):
        html = reunion_file.read_text(encoding="utf-8")
    else:
        html = await fetch_html(session, reunion["url"])
        save_html(reunion_file, html)

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

        title = (a.get("title") or "").strip()
        heure, intitule = None, None
        if " - " in title:
            heure, intitule = title.split(" - ", 1)
        else:
            intitule = title or None

        code = f"C{numero}" if numero is not None else (mC.group(0) if mC else None)

        slug = slugify(intitule) if intitule else "course"
        code_part = f"{reunion['reunion_code']}{(code or '').upper()}"
        filename = f"{code_part}-{slug}.html"

        courses.append({
            "url": url,
            "filename": filename,
            "reunion_dir": reunion_dir,
        })

    await asyncio.sleep(jitter_sleep_base())
    return courses

async def scrape_course(session: aiohttp.ClientSession, course: dict):
    filepath = course["reunion_dir"] / course["filename"]

    if file_exists(filepath):
        return f"SKIP: {course['filename']}"

    try:
        html = await fetch_html(session, course["url"])
        save_html(filepath, html)
        await asyncio.sleep(jitter_sleep_base())
        return f"OK: {course['filename']}"
    except Exception as e:
        return f"ERROR: {course['filename']} - {e}"

# =========================
# Orchestrateur
# =========================
async def scrape_year(year: str, dates: list, session: aiohttp.ClientSession):
    print(f"\n{'='*60}\nANNÉE {year} - {len(dates)} dates\n{'='*60}\n")

    # Batch dates
    for i in range(0, len(dates), MAX_CONCURRENT_DATES):
        date_batch = dates[i:i + MAX_CONCURRENT_DATES]
        print(f"Processing dates: {', '.join(date_batch)}")

        date_tasks = [scrape_date(session, d) for d in date_batch]
        all_reunions_lists = await asyncio.gather(*date_tasks, return_exceptions=True)

        all_reunions = []
        for result in all_reunions_lists:
            if isinstance(result, Exception):
                print(f"  Error fetching date: {result}")
            else:
                all_reunions.extend(result)

        if not all_reunions:
            continue

        # Batch reunions
        for j in range(0, len(all_reunions), MAX_CONCURRENT_REUNIONS):
            reunion_batch = all_reunions[j:j + MAX_CONCURRENT_REUNIONS]
            print(f"  Processing {len(reunion_batch)} reunions...")

            reunion_tasks = [scrape_reunion(session, r) for r in reunion_batch]
            all_courses_lists = await asyncio.gather(*reunion_tasks, return_exceptions=True)

            all_courses = []
            for result in all_courses_lists:
                if isinstance(result, Exception):
                    print(f"    Error fetching reunion: {result}")
                else:
                    all_courses.extend(result)

            if not all_courses:
                continue

            # Batch courses
            print(f"    Processing {len(all_courses)} courses...")
            for k in range(0, len(all_courses), MAX_CONCURRENT_COURSES):
                course_batch = all_courses[k:k + MAX_CONCURRENT_COURSES]
                course_tasks = [scrape_course(session, c) for c in course_batch]
                results = await asyncio.gather(*course_tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        print(f"      Error: {result}")
                    elif "OK:" in str(result):
                        print(f"      {result}")

        await asyncio.sleep(1.0)

def print_repo_context():
    print("\n" + "="*60)
    print("Contexte repo (sanity check)")
    print("="*60)
    print(f"Working dir: {REPO_ROOT}")
    print(f"DATA_ROOT  : {DATA_ROOT}")
    git_dir = (REPO_ROOT / ".git")
    print(f".git présent: {git_dir.exists()}")
    try:
        # Récupérer la branche courante si possible
        res = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=True)
        print(f"Branche locale détectée: {res.stdout.strip()}")
    except Exception as _:
        print("Branche locale: (non déterminée)")

def local_sanity_check(processed_years):
    print("\n" + "="*60)
    print("Vérification locale des fichiers générés")
    print("="*60)
    if not DATA_ROOT.exists():
        print(f"✗ Le dossier {DATA_ROOT} n'existe pas (attendu: créé pendant le scrape).")
        return

    print(f"✓ Dossier présent: {DATA_ROOT}")

    # Afficher un échantillon
    sample = []
    for year in sorted(processed_years):
        ypath = DATA_ROOT / year
        if ypath.exists():
            sample.append(str(ypath))
    if sample:
        print("✓ Dossiers d'année trouvés:")
        for p in sample[:10]:
            print("  -", p)
    else:
        print("⚠ Aucun dossier d'année trouvé dans", DATA_ROOT)

    # Lister jusqu'à 50 fichiers pour feedback
    listed = 0
    for path in DATA_ROOT.rglob("*.html"):
        print("  file:", path.relative_to(REPO_ROOT))
        listed += 1
        if listed >= 50:
            print("  ... (limite d'affichage)")
            break

async def main():
    print("ZEturf Scraper - GitHub Actions")
    print(f"Période: {START_DATE} → {END_DATE}\n")

    print_repo_context()

    all_dates = date_range_asc(START_DATE, END_DATE)
    years_dict = group_by_year(all_dates)
    processed_years = set()

    # Connector avec limite raisonnable
    connector = aiohttp.TCPConnector(limit=256, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=40)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for year in sorted(years_dict.keys()):
            dates = years_dict[year]
            await scrape_year(year, dates, session)
            processed_years.add(year)

    # Vérification locale (les commits & push sont gérés par le workflow YAML)
    local_sanity_check(processed_years)

    print("\n" + "="*60)
    print("SCRAPING TERMINÉ")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())
