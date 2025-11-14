# -*- coding: utf-8 -*-
"""
Script de re-scraping intelligent des courses manquantes ZEturf

- Parse verification_report.txt pour identifier les courses manquantes
- Reconstruit les URLs directement depuis les noms de fichiers
- Scraping CONCURRENT (asyncio + aiohttp) avec limite de parallélisme dynamique
- Auto-ajustement du concurrency en fonction des 429 (rate limit)
- Monitoring de l'espace disque
- Commit par année
"""

import re
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

# Batch & concurrency (dynamiques)
INITIAL_BATCH_SIZE = 200        # nombre de courses logiques dans un batch
MIN_BATCH_SIZE = 10            # (on ne le touche pas ici, on agit surtout sur la concurrency)

INITIAL_CONCURRENCY = 200       # point de départ : 100 requêtes en parallèle
MIN_CONCURRENCY = 10
CONCURRENCY_STEP = 10           # +10 / -10
CONSECUTIVE_THRESHOLD = 2       # nb de batches OK avant tentative d'augmentation

# Limite "interdite" après 429 : on ne remonte jamais à la concurrency qui a causé 429
last_rate_limit_concurrency = None  # ex : 240 → on ne dépassera jamais 230

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

            # Détecter l'en-tête de date
            if line.startswith("DATE:") and "STATUS:" in line:
                match = re.search(r"DATE:\s*(\d{4}-\d{2}-\d{2})", line)
                if match:
                    current_date = match.group(1)

            # Détecter les courses manquantes uniquement
            elif current_date and line.startswith("❌") and "/" in line and ".html" in line:
                # Format: ❌ R1-auteuil/R1C2-prix-du-president-de-la-republique.html
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
# Batch scraping (concurrent) avec détection des 429
# =========================
async def scrape_courses_batch(
    session: aiohttp.ClientSession,
    courses,
    concurrency: int,
):
    """
    Scrape un batch de courses de manière CONCURRENTE.

    Args:
        courses: [(date, reunion_slug, course_file, filepath), ...]
        concurrency: nombre max de requêtes HTTP en parallèle

    Returns:
        (success_count, rate_limited, errors)
    """
    sem = asyncio.Semaphore(concurrency)

    tasks = [
        _scrape_one_course(sem, session, date_str, reunion_slug, course_file, filepath)
        for (date_str, reunion_slug, course_file, filepath) in courses
    ]

    success = 0
    errors = []
    rate_limited = False

    for coro in asyncio.as_completed(tasks):
        status, err = await coro
        if status == 200:
            success += 1
        elif status == 429:
            rate_limited = True
            if err:
                errors.append(err)
        else:
            if err:
                errors.append(err)

    return success, rate_limited, errors

# =========================
# Scraping par année avec auto-ajustement du concurrency
# =========================
async def scrape_year(year: str, dates_courses: dict, initial_batch_size: int) -> None:
    """
    Scrape toutes les courses manquantes pour une année avec concurrency adaptatif.

    Args:
        year: Année à traiter
        dates_courses: dict[date] = [(reunion_slug, course_file), ...]
        initial_batch_size: nombre max de courses dans un batch logique
    """
    global last_rate_limit_concurrency

    print(f"\n{'=' * 80}")
    print(f"ANNÉE {year}")
    print(f"{'=' * 80}\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque disponible: {free_gb:.2f} GB")

    if free_gb < YEAR_SKIP_DISK_GB:
        print("⚠️  Espace insuffisant pour traiter cette année, on saute.")
        return

    # Aplatir toutes les courses
    all_courses = []
    for date_str, courses_list in sorted(dates_courses.items()):
        for reunion_slug, course_file in courses_list:
            date_dir = get_date_directory(date_str)
            reunion_dir = date_dir / reunion_slug
            filepath = reunion_dir / course_file
            all_courses.append((date_str, reunion_slug, course_file, filepath))

    total_courses = len(all_courses)
    print(f"📊 {total_courses} courses à récupérer pour {year}")

    if total_courses == 0:
        return

    stats = {
        "success": 0,
        "failed": 0,
        "rate_limits": 0,
        "concurrency_increases": 0,
        "concurrency_decreases": 0,
    }

    batch_size = max(initial_batch_size, MIN_BATCH_SIZE)
    position = 0
    concurrency = INITIAL_CONCURRENCY
    consecutive_successes = 0

    async with aiohttp.ClientSession() as session:
        while position < total_courses:
            if check_disk_space_critical():
                print(f"⚠️  Arrêt à la position {position}/{total_courses}")
                break

            remaining = total_courses - position
            current_batch_size = min(batch_size, remaining)

            free_gb = get_disk_space_gb()
            print(
                f"\n  📦 Batch: courses {position+1}-"
                f"{position+current_batch_size}/{total_courses} "
                f"(batch size: {current_batch_size}, concurrency: {concurrency})"
            )
            print(f"  💾 Espace libre: {free_gb:.2f} GB")

            batch = all_courses[position:position + current_batch_size]

            # Scraping concurrent de ce batch
            success, rate_limited, errors = await scrape_courses_batch(
                session, batch, concurrency
            )

            stats["success"] += success
            stats["failed"] += len(errors)

            if rate_limited:
                # 429 détecté → on baisse la concurrency et on RETENTE le même batch
                stats["rate_limits"] += 1
                consecutive_successes = 0

                print(f"      ⚠️  Rate limit détecté avec concurrency={concurrency}")

                # On note la concurrency qui a causé un 429
                if last_rate_limit_concurrency is None or concurrency < last_rate_limit_concurrency:
                    last_rate_limit_concurrency = concurrency

                # Nouvelle concurrency : -10, mais jamais en dessous de MIN_CONCURRENCY
                new_concurrency = max(MIN_CONCURRENCY, concurrency - CONCURRENCY_STEP)

                # On définit la limite max comme (last_rate_limit_concurrency - step)
                # pour ne plus jamais remonter au niveau qui a déjà causé 429.
                if last_rate_limit_concurrency is not None:
                    max_safe = max(MIN_CONCURRENCY, last_rate_limit_concurrency - CONCURRENCY_STEP)
                    if new_concurrency > max_safe:
                        new_concurrency = max_safe

                if new_concurrency < concurrency:
                    concurrency = new_concurrency
                    stats["concurrency_decreases"] += 1
                    print(f"      🔽 Nouvelle concurrency: {concurrency}")
                else:
                    print(f"      ℹ️ Concurrency déjà au minimum safe: {concurrency}")

                print("      ⏸️  Attente 30s avant retry du même batch...")
                await asyncio.sleep(30)
                # On NE BOUGE PAS position → on retentera les mêmes courses
                continue

            # Pas de 429: batch "réussi"
            consecutive_successes += 1

            # Tentative d'augmentation de la concurrency après N batches OK
            if consecutive_successes >= CONSECUTIVE_THRESHOLD:
                can_increase = True

                if last_rate_limit_concurrency is not None:
                    # On ne doit jamais atteindre la concurrency qui a causé 429
                    max_allowed = max(MIN_CONCURRENCY, last_rate_limit_concurrency - CONCURRENCY_STEP)
                    if concurrency >= max_allowed:
                        can_increase = False
                        print(
                            f"      ℹ️ Concurrency au maximum safe ({concurrency}), "
                            f"limite basée sur dernier 429={last_rate_limit_concurrency}"
                        )

                if can_increase:
                    new_concurrency = concurrency + CONCURRENCY_STEP

                    # Si on a déjà une limite connue, on clamp
                    if last_rate_limit_concurrency is not None:
                        max_allowed = max(MIN_CONCURRENCY, last_rate_limit_concurrency - CONCURRENCY_STEP)
                        if new_concurrency > max_allowed:
                            new_concurrency = max_allowed

                    if new_concurrency > concurrency:
                        concurrency = new_concurrency
                        stats["concurrency_increases"] += 1
                        print(f"      🔼 Augmentation concurrency: {concurrency}")

                consecutive_successes = 0

            # Batch terminé: on avance dans la liste
            position += current_batch_size

            if position < total_courses:
                await asyncio.sleep(1)

    print(f"\n{'=' * 80}")
    print(f"RÉSUMÉ ANNÉE {year}")
    print(f"{'=' * 80}")
    print(f"✓ Succès:                 {stats['success']}/{total_courses}")
    print(f"✗ Échecs:                 {stats['failed']}")
    print(f"⚠️  Rate limits:            {stats['rate_limits']}")
    print(f"🔼 Incr. concurrency:      {stats['concurrency_increases']}")
    print(f"🔽 Décr. concurrency:      {stats['concurrency_decreases']}")
    if last_rate_limit_concurrency is not None:
        print(f"📌 Dernière concurrency ayant causé 429: {last_rate_limit_concurrency}")
    print(f"💾 Espace final:           {get_disk_space_gb():.2f} GB")
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
        help="Limite globale de courses à traiter",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=INITIAL_BATCH_SIZE,
        help="Taille initiale des batchs (défaut: 200)",
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

    total_courses = sum(
        len(courses)
        for year_data in missing_by_year.values()
        for courses in year_data.values()
    )
    print(f"📊 {len(missing_by_year)} années avec courses manquantes")
    print(f"📊 {total_courses} courses manquantes au total\n")

    courses_processed = 0
    for year in sorted(missing_by_year.keys()):
        free_gb = get_disk_space_gb()
        if free_gb < CRITICAL_DISK_GB:
            print(
                f"⚠️  ARRÊT: Espace disque insuffisant ({free_gb:.2f} GB), "
                f"progression: {courses_processed}/{total_courses} courses traitées"
            )
            break

        if args.max_courses and courses_processed >= args.max_courses:
            print(f"⚠️  Limite globale atteinte ({args.max_courses} courses)")
            break

        await scrape_year(year, missing_by_year[year], args.batch_size)

        git_commit_year(year)

        year_courses = sum(len(c) for c in missing_by_year[year].values())
        courses_processed += year_courses

    print("\n" + "=" * 80)
    print("SCRAPING TERMINÉ")
    print(f"💾 Espace disque final: {get_disk_space_gb():.2f} GB")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
