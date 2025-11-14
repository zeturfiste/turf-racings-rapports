# -*- coding: utf-8 -*-
"""
Script de re-scraping intelligent des courses manquantes ZEturf

- Parse verification_report.txt pour identifier les courses manquantes
- Reconstruit les URLs directement depuis les noms de fichiers
- Scraping CONCURRENT (asyncio + aiohttp) avec une concurrency fixe
- Travail par lots de N courses (N = concurrency)
- A chaque lot : stats (succès, 429, temps, restantes)
- Pause dynamique entre les lots en fonction du taux de 429
- Les 429 sont retentées au lot suivant, sans jamais sauter une course
- Commit par année, années traitées l'une après l'autre
"""

import re
import time
import asyncio
import aiohttp
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import subprocess
import shutil

# =========================
# Configuration
# =========================
BASE = "https://www.zeturf.fr"
REPO_ROOT = "resultats-et-rapports"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

# Concurrency fixe par défaut : nombre de requêtes HTTP en parallèle
DEFAULT_CONCURRENCY = 100  # utilisé par défaut, ajustable via --concurrency si tu le lances à la main

# Gestion de la pause entre lots
INITIAL_SLEEP = 5.0        # pause initiale entre les lots (en secondes)
SLEEP_STEP_SMALL = 5.0     # +5s si un peu de 429
SLEEP_STEP_MEDIUM = 10.0   # +10s si pas mal de 429
SLEEP_STEP_LARGE = 20.0    # +20s si beaucoup de 429
SLEEP_DECAY = 5.0          # -5s si aucun 429
SLEEP_MAX = 120.0          # max 2 minutes de pause
SLEEP_MIN = 0.0

# Seuils disque
WARN_DISK_GB = 5
CRITICAL_DISK_GB = 2
YEAR_SKIP_DISK_GB = 3

# =========================
# Disk monitoring
# =========================
def get_disk_space_gb() -> float:
    """Retourne l'espace disque disponible en GB."""
    stat = shutil.disk_usage("/")
    return stat.free / (1024 ** 3)


def check_disk_space_critical() -> bool:
    """Vérifie si l'espace disque est critique (< CRITICAL_DISK_GB)."""
    free_gb = get_disk_space_gb()
    if free_gb < CRITICAL_DISK_GB:
        print(f"\n⚠️  ALERTE: Espace disque critique: {free_gb:.2f} GB restants")
        print("Arrêt du scraping pour éviter saturation...")
        return True
    return False

# =========================
# Path helpers
# =========================
def get_date_directory(date_str: str) -> Path:
    """Retourne le chemin du dossier de la date: YYYY/MM/YYYY-MM-DD/."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str


def save_html(filepath: Path, html: str) -> None:
    """Sauvegarde le HTML dans le fichier, en créant les dossiers au besoin."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(html, encoding="utf-8")

# =========================
# Parse verification report
# =========================
def parse_missing_courses(report_path: Path = Path("verification_report.txt")):
    """
    Parse le rapport de vérification pour extraire les courses manquantes.

    Format attendu dans le rapport:
        DATE: 2006-04-16 - STATUS: INCOMPLETE
        ❌ R1-auteuil/R1C2-prix-du-president-de-la-republique.html

    Returns:
        dict[year][date] = [(reunion_slug, course_file), ...]
    """
    if not report_path.exists():
        print(f"❌ Fichier {report_path} introuvable")
        return {}

    missing = defaultdict(lambda: defaultdict(list))
    current_date = None

    with report_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()

            # En-tête de date
            if line.startswith("DATE:") and "STATUS:" in line:
                match = re.search(r"DATE:\s*(\d{4}-\d{2}-\d{2})", line)
                if match:
                    current_date = match.group(1)

            # Lignes de courses manquantes
            elif current_date and line.startswith("❌") and "/" in line and ".html" in line:
                # Format: ❌ R1-auteuil/R1C2-prix-xxx.html
                match = re.search(r"❌\s*([^/]+)/([^/]+\.html)", line)
                if match:
                    reunion_slug = match.group(1)
                    course_file = match.group(2)
                    year = current_date[:4]
                    missing[year][current_date].append((reunion_slug, course_file))

    return dict(missing)

# =========================
# URL reconstruction
# =========================
def build_course_url(date_str: str, reunion_slug: str, course_file: str) -> str:
    """
    Reconstruit l'URL de la course depuis le nom de fichier.

    Ex:
      date = 2006-04-16
      reunion_slug = "R1-auteuil"
      course_file  = "R1C2-prix-du-president-de-la-republique.html"

    → https://www.zeturf.fr/fr/course/2006-04-16/R1C2-auteuil-prix-du-president-de-la-republique
    """
    hippodrome = reunion_slug.split("-", 1)[1] if "-" in reunion_slug else reunion_slug
    course_slug = course_file[:-5] if course_file.endswith(".html") else course_file

    if "-" in course_slug:
        code_part, title_part = course_slug.split("-", 1)
    else:
        code_part, title_part = course_slug, ""

    if title_part:
        url_suffix = f"{code_part}-{hippodrome}-{title_part}"
    else:
        url_suffix = f"{code_part}-{hippodrome}"

    return f"{BASE}/fr/course/{date_str}/{url_suffix}"

# =========================
# HTTP fetching (concurrent)
# =========================
async def fetch_course(
    session: aiohttp.ClientSession,
    url: str,
    retries: int = 3,
) -> tuple[str | None, int | None]:
    """Récupère le HTML d'une course. Retourne (html, status_code)."""
    last_exc = None
    for attempt in range(retries):
        try:
            async with session.get(
                url,
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                html = await resp.text()
                return html, resp.status
        except asyncio.TimeoutError as e:
            last_exc = e
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 * (attempt + 1))
        except Exception as e:
            last_exc = e
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 * (attempt + 1))

    if last_exc:
        raise last_exc
    return None, None


async def _scrape_one_course(
    sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    date_str: str,
    reunion_slug: str,
    course_file: str,
    filepath: Path,
) -> tuple[int | None, str | None]:
    """
    Tâche individuelle pour une course.
    Retourne (status, error_msg | None).
    """
    async with sem:
        url = build_course_url(date_str, reunion_slug, course_file)
        try:
            html, status = await fetch_course(session, url)
            if status == 200 and html is not None:
                save_html(filepath, html)
                return status, None
            else:
                msg = f"{date_str} {reunion_slug}/{course_file} (HTTP {status})"
                return status, msg
        except Exception as e:
            msg = f"{date_str} {reunion_slug}/{course_file} ({str(e)[:80]})"
            return None, msg

# =========================
# Scraping d'une année, en lots successifs
# =========================
async def scrape_year(year: str, dates_courses: dict, concurrency: int) -> None:
    """
    Scrape toutes les courses manquantes pour une année, en respectant l'ordre
    date → réunion → course, avec une concurrency fixe et des lots successifs.

    - On ne mélange jamais plusieurs années.
    - On ne saute jamais une course :
        * succès → course terminée
        * 429 → la course est remise au début de la file pour le lot suivant
        * autres erreurs → comptées comme échecs définitifs
    """
    print(f"\n{'=' * 80}")
    print(f"ANNÉE {year}")
    print(f"{'=' * 80}\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque disponible: {free_gb:.2f} GB")

    if free_gb < YEAR_SKIP_DISK_GB:
        print("⚠️  Espace insuffisant pour traiter cette année, on saute.")
        return

    # Aplatir toutes les courses de l'année dans l'ordre
    all_courses = []
    for date_str in sorted(dates_courses.keys()):
        for reunion_slug, course_file in dates_courses[date_str]:
            date_dir = get_date_directory(date_str)
            reunion_dir = date_dir / reunion_slug
            filepath = reunion_dir / course_file
            all_courses.append((date_str, reunion_slug, course_file, filepath))

    total_courses = len(all_courses)
    print(f"📊 {total_courses} courses à récupérer pour {year}")

    if total_courses == 0:
        return

    pending = list(all_courses)

    stats = {
        "total": total_courses,
        "success": 0,
        "failed_429": 0,
        "failed_other": 0,
        "lots": 0,
    }

    lot_index = 0
    current_sleep = INITIAL_SLEEP  # pause entre lots

    async with aiohttp.ClientSession() as session:
        while pending:
            if check_disk_space_critical():
                print("⚠️  Arrêt pour manque d'espace disque.")
                break

            lot_index += 1
            stats["lots"] += 1

            free_gb = get_disk_space_gb()
            lot_size = min(concurrency, len(pending))
            current_lot = pending[:lot_size]
            pending = pending[lot_size:]

            first_idx = total_courses - len(pending) - lot_size + 1
            last_idx = total_courses - len(pending)
            remaining_year = len(pending)

            print(
                f"\n  🧩 Lot #{lot_index}: courses {first_idx}-{last_idx}/{total_courses} "
                f"(taille lot: {lot_size}, concurrency: {concurrency})"
            )
            print(f"  💾 Espace libre avant lot: {free_gb:.2f} GB")
            print(f"  📉 Courses restantes pour l'année {year}: {remaining_year}")

            sem = asyncio.Semaphore(concurrency)
            tasks = [
                _scrape_one_course(sem, session, date_str, reunion_slug, course_file, filepath)
                for (date_str, reunion_slug, course_file, filepath) in current_lot
            ]

            lot_start = time.time()
            results = await asyncio.gather(*tasks)

            lot_success = 0
            lot_429 = 0
            lot_other_errors = 0
            retry_429 = []

            for (date_str, reunion_slug, course_file, filepath), (status, err) in zip(
                current_lot, results
            ):
                if status == 200:
                    lot_success += 1
                elif status == 429:
                    lot_429 += 1
                    retry_429.append((date_str, reunion_slug, course_file, filepath))
                else:
                    if err is not None:
                        lot_other_errors += 1

            lot_duration = time.time() - lot_start

            stats["success"] += lot_success
            stats["failed_429"] += lot_429
            stats["failed_other"] += lot_other_errors

            if retry_429:
                pending = retry_429 + pending

            remaining_year_after = len(pending)

            print(
                f"  ⏱️  Lot #{lot_index} terminé en {lot_duration:.2f}s "
                f"(succès: {lot_success}, 429: {lot_429}, autres erreurs: {lot_other_errors})"
            )
            print(
                f"  📊 Courses restantes pour l'année {year} après ce lot: "
                f"{remaining_year_after}"
            )
            print(f"  💾 Espace libre après lot: {get_disk_space_gb():.2f} GB")

            # Ajuster la pause entre lots en fonction du taux de 429
            if lot_429 > 0:
                ratio_429 = lot_429 / lot_size
                if ratio_429 > 0.5:
                    current_sleep = min(SLEEP_MAX, current_sleep + SLEEP_STEP_LARGE)
                elif ratio_429 > 0.2:
                    current_sleep = min(SLEEP_MAX, current_sleep + SLEEP_STEP_MEDIUM)
                else:
                    current_sleep = min(SLEEP_MAX, current_sleep + SLEEP_STEP_SMALL)
                print(
                    f"  ⚠️  Taux de 429 dans ce lot: {ratio_429:.2%} → "
                    f"nouvelle pause entre lots: {current_sleep:.1f}s"
                )
            else:
                if current_sleep > 0:
                    current_sleep = max(SLEEP_MIN, current_sleep - SLEEP_DECAY)
                    print(
                        f"  ✅ Aucun 429 dans ce lot → réduction de la pause à "
                        f"{current_sleep:.1f}s"
                    )

            if pending and current_sleep > 0:
                print(
                    f"  ⏸️  Pause {current_sleep:.1f}s avant le prochain lot "
                    f"pour laisser respirer le site..."
                )
                await asyncio.sleep(current_sleep)

    print(f"\n{'=' * 80}")
    print(f"RÉSUMÉ ANNÉE {year}")
    print(f"{'=' * 80}")
    print(f"  Total prévu:      {stats['total']}")
    print(f"  ✓ Succès:         {stats['success']}")
    print(f"  ✗ 429 (non OK):   {stats['failed_429']}")
    print(f"  ✗ Autres erreurs: {stats['failed_other']}")
    print(f"  🔁 Nombre de lots: {stats['lots']}")
    print(f"  💾 Espace final:   {get_disk_space_gb():.2f} GB")
    print(f"{'=' * 80}\n")

# =========================
# Git operations
# =========================
def git_commit_year(year: str) -> None:
    """Commit et push les changements pour l'année donnée."""
    print(f"\n📤 Git commit pour l'année {year}...")
    try:
        subprocess.run(
            ["git", "config", "user.name", "GitHub Actions Bot"],
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "actions@github.com"],
            check=True,
        )

        status = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=True,
        )

        if not status.stdout.strip():
            print("  ℹ️  Aucun changement pour cette année")
            return

        subprocess.run(
            ["git", "add", f"{REPO_ROOT}/{year}"],
            check=True,
        )

        files_changed = status.stdout.count("\n")
        commit_msg = f"Re-scrape: {year} - {files_changed} fichiers modifiés/ajoutés"
        timestamp_msg = f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        subprocess.run(
            ["git", "commit", "-m", commit_msg, "-m", timestamp_msg],
            check=True,
        )

        subprocess.run(["git", "push"], check=True)

        print(f"  ✓ Année {year} committée et pushée ({files_changed} fichiers)\n")

    except subprocess.CalledProcessError as e:
        print(f"  ✗ Erreur Git: {e}\n")

# =========================
# Main orchestrator
# =========================
async def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Re-scrape intelligent des courses ZEturf manquantes"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Nombre de requêtes en parallèle (défaut: {DEFAULT_CONCURRENCY})",
    )
    args = parser.parse_args()

    concurrency = max(1, args.concurrency)

    print("=" * 80)
    print("RE-SCRAPING DIRECT DES COURSES MANQUANTES")
    print("=" * 80 + "\n")

    print(f"⚙️  Concurrency utilisée: {concurrency}")
    print(f"⏱️  Pause initiale entre les lots: {INITIAL_SLEEP:.1f}s\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque initial: {free_gb:.2f} GB\n")

    if free_gb < WARN_DISK_GB:
        print("⚠️  WARNING: Espace disque faible! Recommandé: > 5GB")
        print("Continuation avec prudence...\n")

    missing_by_year = parse_missing_courses()

    if not missing_by_year:
        print("✓ Aucune course manquante détectée\n")
        return

    total_courses = sum(
        len(courses)
        for year_data in missing_by_year.values()
        for courses in year_data.values()
    )
    print(f"📊 {len(missing_by_year)} années avec courses manquantes")
    print(f"📊 {total_courses} courses manquantes au total\n")

    for year in sorted(missing_by_year.keys()):
        year_courses = sum(len(c) for c in missing_by_year[year].values())
        print(f"➡️  Traitement de l'année {year} ({year_courses} courses prévues)")
        await scrape_year(year, missing_by_year[year], concurrency)
        git_commit_year(year)

    print("\n" + "=" * 80)
    print("SCRAPING TERMINÉ")
    print(f"💾 Espace disque final: {get_disk_space_gb():.2f} GB")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
