# -*- coding: utf-8 -*-
"""
Script de re-scraping intelligent des courses manquantes ZEturf

- Parse verification_report.txt pour identifier les courses manquantes
- Reconstruit les URLs directement depuis les noms de fichiers
- Scraping CONCURRENT (asyncio + aiohttp) avec une concurrency dynamique
- Travail par lots de N courses (N = concurrency courante)
- A chaque lot :
    * stats (succès, 429, temps)
    * sleep entre lots FIXE à 30s
    * concurrency dynamique :
        - on part de 100 (plancher initial)
        - si un lot a au moins 1 erreur 429 :
              -> prochain lot à concurrency = plancher
        - si un lot a 0 erreur 429 :
              -> si concurrency > plancher : nouveau plancher = concurrency
              -> prochain lot concurrency += 20 (jusqu'à MAX_CONCURRENCY)
- Les 429 sont retentées au lot suivant, sans jamais sauter une course
- Commit par année, années traitées l'une après l'autre
- Option --start-year pour ignorer les années plus anciennes
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

# Concurrency dynamique
MIN_CONCURRENCY = 100
CONCURRENCY_STEP = 20
MAX_CONCURRENCY = 120  # garde une borne haute pour éviter les folies

# Sleep fixe entre lots (en secondes)
SLEEP_BETWEEN_LOTS = 30  # 30s entre chaque lot

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

    # On renvoie un dict classique pour figer l'ordre
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
async def fetch_course(session: aiohttp.ClientSession, url: str, retries: int = 3) -> tuple[str, int]:
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
    raise RuntimeError("fetch_course failed without exception")


async def _scrape_one_course(
    sem: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    date_str: str,
    reunion_slug: str,
    course_file: str,
    filepath: Path,
):
    """
    Tâche individuelle pour une course.
    Retourne (status, error_msg | None).
    """
    async with sem:
        url = build_course_url(date_str, reunion_slug, course_file)
        try:
            html, status = await fetch_course(session, url)

            if status == 200:
                save_html(filepath, html)
                # LOG avec date + réunion + fichier
                print(f"      ✓ {date_str} {reunion_slug}/{course_file}")
                return status, None
            else:
                msg = f"{date_str} {reunion_slug}/{course_file} (HTTP {status})"
                print(f"      ✗ {msg}")
                return status, msg

        except Exception as e:
            msg = f"{date_str} {reunion_slug}/{course_file} ({str(e)[:80]})"
            print(f"      ✗ {msg}")
            return None, msg

# =========================
# Scraping d'une année, en lots successifs
# =========================
async def scrape_year(year: str, dates_courses: dict) -> None:
    """
    Scrape toutes les courses manquantes pour une année, en respectant l'ordre
    date → réunion → course, avec une concurrency dynamique et des lots successifs.

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

    # File de "pending" dans l'ordre
    pending = list(all_courses)

    stats = {
        "total": total_courses,
        "success": 0,
        "failed_429": 0,
        "failed_other": 0,
        "lots": 0,
    }

    lot_index = 0
    concurrency_floor = MIN_CONCURRENCY
    concurrency_current = MIN_CONCURRENCY

    async with aiohttp.ClientSession() as session:
        while pending:
            if check_disk_space_critical():
                print("⚠️  Arrêt pour manque d'espace disque.")
                break

            lot_index += 1
            stats["lots"] += 1

            free_gb = get_disk_space_gb()
            lot_size = min(concurrency_current, len(pending))
            current_lot = pending[:lot_size]
            pending = pending[lot_size:]

            first_idx = total_courses - len(pending) - lot_size + 1
            last_idx = total_courses - len(pending)

            print(
                f"\n  🧩 Lot #{lot_index}: courses {first_idx}-{last_idx}/{total_courses} "
                f"(taille lot: {lot_size}, concurrency: {concurrency_current}, plancher: {concurrency_floor})"
            )
            print(f"  💾 Espace libre avant lot: {free_gb:.2f} GB")
            print(f"  ⏱️  Sleep fixe entre lots: {SLEEP_BETWEEN_LOTS}s")

            # Lancer le lot en parallèle
            sem = asyncio.Semaphore(concurrency_current)
            tasks = [
                _scrape_one_course(sem, session, date_str, reunion_slug, course_file, filepath)
                for (date_str, reunion_slug, course_file, filepath) in current_lot
            ]

            lot_start = time.time()
            lot_success = 0
            lot_429 = 0
            lot_other_errors = 0
            retry_429 = []

            for (date_str, reunion_slug, course_file, filepath), coro in zip(
                current_lot, asyncio.as_completed(tasks)
            ):
                status, err = await coro
                if status == 200:
                    lot_success += 1
                elif status == 429:
                    lot_429 += 1
                    retry_429.append((date_str, reunion_slug, course_file, filepath))
                else:
                    if err is not None:
                        lot_other_errors += 1

            lot_duration = time.time() - lot_start

            # Stats globales
            stats["success"] += lot_success
            stats["failed_429"] += lot_429
            stats["failed_other"] += lot_other_errors

            # Les 429 repartent en tête de file, dans le même ordre
            if retry_429:
                print(f"  🔁 {lot_429} courses avec 429 seront retentées au lot suivant.")
                pending = retry_429 + pending

            print(
                f"  ⏱️  Lot #{lot_index} terminé en {lot_duration:.2f}s "
                f"(succès: {lot_success}, 429: {lot_429}, autres erreurs: {lot_other_errors})"
            )
            print(f"  💾 Espace libre après lot: {get_disk_space_gb():.2f} GB")

            # Ajustement de la concurrency
            if lot_429 > 0:
                # On a vu des 429 → retour au plancher
                if concurrency_current > concurrency_floor:
                    print(
                        f"  ⚠️  {lot_429} erreurs 429 avec concurrency={concurrency_current}, "
                        f"retour au plancher {concurrency_floor}"
                    )
                else:
                    print(
                        f"  ⚠️  {lot_429} erreurs 429 au plancher {concurrency_floor}, "
                        f"on reste au plancher"
                    )
                concurrency_current = concurrency_floor
            else:
                # Aucun 429 sur ce lot
                if concurrency_current > concurrency_floor:
                    old_floor = concurrency_floor
                    concurrency_floor = concurrency_current
                    print(
                        f"  📌 Nouveau plancher de concurrency: {old_floor} → {concurrency_floor}"
                    )

                proposed = concurrency_current + CONCURRENCY_STEP
                if proposed > MAX_CONCURRENCY:
                    proposed = MAX_CONCURRENCY

                if proposed != concurrency_current:
                    print(
                        f"  🔼 Augmentation de concurrency: {concurrency_current} → {proposed}"
                    )
                concurrency_current = proposed

            # Pause fixe entre lots s'il reste des courses
            if pending:
                print(f"  ⏳ Pause de {SLEEP_BETWEEN_LOTS}s avant le lot suivant...")
                await asyncio.sleep(SLEEP_BETWEEN_LOTS)

    # Résumé par année
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
        "--max-courses",
        type=int,
        default=None,
        help="(Optionnel) Limite globale de courses à traiter (approx, par années entières)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="(Optionnel) Année de départ (inclus), ex: 2010",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("RE-SCRAPING DIRECT DES COURSES MANQUANTES")
    print("=" * 80 + "\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque initial: {free_gb:.2f} GB\n")

    if free_gb < WARN_DISK_GB:
        print("⚠️  WARNING: Espace disque faible! Recommandé: > 5GB")
        print("Continuation avec prudence...\n")

    missing_by_year = parse_missing_courses()

    if not missing_by_year:
        print("✓ Aucune course manquante détectée\n")
        return

    # Filtre par start_year si fourni
    if args.start_year is not None:
        print(f"➡️  Filtrage: on garde uniquement les années >= {args.start_year}")
        missing_by_year = {
            year: data
            for year, data in missing_by_year.items()
            if int(year) >= args.start_year
        }
        if not missing_by_year:
            print(f"ℹ️  Aucune année >= {args.start_year} avec des courses manquantes.")
            return

    total_courses = sum(
        len(courses)
        for year_data in missing_by_year.values()
        for courses in year_data.values()
    )
    print(f"📊 {len(missing_by_year)} années avec courses manquantes (après filtre éventuel)")
    print(f"📊 {total_courses} courses manquantes au total\n")

    courses_planned = 0
    for year in sorted(missing_by_year.keys()):
        # Limite globale (approx, par années complètes)
        year_courses = sum(len(c) for c in missing_by_year[year].values())
        if args.max_courses and courses_planned >= args.max_courses:
            print(f"⚠️  Limite globale atteinte (~{courses_planned} courses planifiées).")
            break

        print(f"➡️  Traitement de l'année {year} ({year_courses} courses prévues)")
        await scrape_year(year, missing_by_year[year])

        # Commit par année
        git_commit_year(year)

        courses_planned += year_courses

    print("\n" + "=" * 80)
    print("SCRAPING TERMINÉ")
    print(f"💾 Espace disque final: {get_disk_space_gb():.2f} GB")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
