# -*- coding: utf-8 -*-
"""
Re-scraping ultra-optimisé des courses manquantes ZEturf.
- Pas de batchs configurables → uniquement concurrency fixe.
- Pause dynamique légère basée sur le % de 429.
- Aucun log par course → seulement macros.
- Avance strictement dans l’ordre chronologique.
"""

import os
import re
import asyncio
import aiohttp
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import shutil
import subprocess
import time

# ============================================================================
# Configuration
# ============================================================================
BASE = "https://www.zeturf.fr"
REPO_ROOT = "resultats-et-rapports"
CONCURRENCY = 100          # Nombre réel de requêtes simultanées
INITIAL_PAUSE = 1.0        # Pause entre lots (secondes)
MIN_PAUSE = 0.5            # Pause min
MAX_PAUSE = 15.0           # Pause max

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

# ============================================================================
# Helpers
# ============================================================================
def get_disk_space_gb():
    stat = shutil.disk_usage('/')
    return stat.free / (1024**3)

def get_date_directory(date_str: str) -> Path:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return Path(REPO_ROOT) / dt.strftime("%Y") / dt.strftime("%m") / date_str

def save_html(filepath: Path, html: str):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(html, encoding="utf-8")

# ============================================================================
# Parse verification report
# ============================================================================
def parse_missing_courses(report_path=Path("verification_report.txt")):
    if not report_path.exists():
        print("❌ Rapport introuvable.")
        return {}

    missing = defaultdict(lambda: defaultdict(list))
    current_date = None

    with open(report_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if line.startswith("DATE:") and "STATUS:" in line:
                m = re.search(r"DATE:\s*(\d{4}-\d{2}-\d{2})", line)
                if m:
                    current_date = m.group(1)

            elif current_date and line.startswith("❌") and ".html" in line:
                m = re.search(r"❌\s*([^/]+)/([^/]+\.html)", line)
                if m:
                    reunion = m.group(1)
                    course = m.group(2)
                    year = current_date[:4]
                    missing[year][current_date].append((reunion, course))

    return {y: d for y, d in missing.items()}

# ============================================================================
# URL reconstruction
# ============================================================================
def build_course_url(date_str, reunion_slug, course_file):
    hipp = reunion_slug.split("-", 1)[1]
    slug = course_file.replace(".html", "")
    code = slug.split("-")[0]
    title_part = slug[len(code) + 1:]
    return f"{BASE}/fr/course/{date_str}/{code}-{hipp}-{title_part}"

# ============================================================================
# Fetch with concurrency
# ============================================================================
async def fetch_one(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=30) as resp:
            html = await resp.text()
            return html, resp.status
    except Exception as e:
        return None, 0

async def scrape_lot(session, todo):
    """
    Traite un lot de courses limité par CONCURRENCY.
    Retourne (succès, erreurs429, erreurs_autres, [courses à retenter])
    """
    sem = asyncio.Semaphore(CONCURRENCY)

    results = []
    tasks = []

    start = time.time()

    async def worker(item):
        date_str, reunion, course_file, filepath = item
        url = build_course_url(date_str, reunion, course_file)

        async with sem:
            html, status = await fetch_one(session, url)
            return (item, html, status)

    for item in todo:
        tasks.append(worker(item))

    finished = await asyncio.gather(*tasks)

    ok = 0
    err429 = 0
    err_other = 0
    retry_list = []

    for (item, html, status) in finished:
        date_str, reunion, course_file, filepath = item

        if status == 200:
            save_html(filepath, html)
            ok += 1

        elif status == 429:
            err429 += 1
            retry_list.append(item)

        else:
            err_other += 1
            retry_list.append(item)

    elapsed = time.time() - start
    return ok, err429, err_other, retry_list, elapsed

# ============================================================================
# Scrape one year
# ============================================================================
async def scrape_year(year, dates_courses):
    print(f"\n{'='*80}")
    print(f"ANNÉE {year}")
    print(f"{'='*80}")

    free = get_disk_space_gb()
    print(f"💾 Espace disque disponible: {free:.2f} GB")

    # Aplatir les courses dans l’ordre
    todo = []
    for date_str in sorted(dates_courses.keys()):
        for reunion, course_file in dates_courses[date_str]:
            filepath = get_date_directory(date_str) / reunion / course_file
            todo.append((date_str, reunion, course_file, filepath))

    total = len(todo)
    print(f"📊 {total} courses à récupérer pour {year}")

    pause = INITIAL_PAUSE

    async with aiohttp.ClientSession() as session:
        lot_id = 1

        while todo:
            free = get_disk_space_gb()
            if free < 2:
                print(f"🚨 Espace disque critique ({free:.2f} GB). Arrêt de l’année.")
                break

            print(f"\n🧩 Lot #{lot_id}: {min(CONCURRENCY, len(todo))} requêtes (restantes: {len(todo)})")

            batch = todo[:CONCURRENCY]

            ok, e429, eoth, retry, elapsed = await scrape_lot(session, batch)

            print(f"⏱️  Durée lot: {elapsed:.2f}s | ✓ {ok} | 429: {e429} | erreurs: {eoth}")

            # Retirer les courses traitées
            todo = retry + todo[CONCURRENCY:]

            # PAUSE DYNAMIQUE
            if e429 == 0:
                pause = max(MIN_PAUSE, pause - 0.2)
            else:
                pause = min(MAX_PAUSE, pause + 1.0)

            print(f"⏸️  Pause {pause:.1f}s...")
            await asyncio.sleep(pause)

            lot_id += 1

    # Résumé
    print(f"\n{'='*80}")
    print(f"RÉSUMÉ ANNÉE {year}")
    print(f"{'='*80}")
    print(f"Courses restantes (non scrapées): {len(todo)}")
    print(f"💾 Espace final: {get_disk_space_gb():.2f} GB")

    # Git commit
    print(f"\n📤 Commit année {year}...")
    subprocess.run(["git", "add", f"{REPO_ROOT}/{year}"], check=False)
    subprocess.run(["git", "commit", "-m", f"Scraping {year}", "-m", datetime.utcnow().isoformat()], check=False)
    subprocess.run(["git", "push"], check=False)

# ============================================================================
# MAIN
# ============================================================================
async def main():
    print("="*80)
    print(f"LANCEMENT DU RE-SCRAPING (concurrency {CONCURRENCY})")
    print("="*80)

    missing = parse_missing_courses()
    if not missing:
        print("✓ Aucune course manquante.")
        return

    total_missing = sum(len(v2) for v in missing.values() for v2 in v.values())
    print(f"📊 {len(missing)} années avec courses manquantes")
    print(f"📊 {total_missing} courses manquantes au total")

    for year in sorted(missing.keys()):
        await scrape_year(year, missing[year])

    print("\n🎉 Re-scraping terminé.")

if __name__ == "__main__":
    asyncio.run(main())
