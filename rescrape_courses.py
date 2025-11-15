# -*- coding: utf-8 -*-
"""
Re-scraping des courses manquantes ZEturf.

- Lit verification_report.txt pour connaître les courses manquantes
- Reconstruit les URLs et télécharge les HTML manquants
- Concurrency fixe = 100, pause fixe = 30s entre lots
- Travail par année, dans l'ordre
- Commit + push par année (répertoires resultats-et-rapports/YYYY)
- Support d'une liste d'années (--years) pour matrix / jobs parallèles
- Arrêt intelligent avant 6h: on n'entame pas une année si on estime qu'on
  ne pourra pas la terminer avant la limite
- NE COMMIT PAS une année tant que toutes ses courses manquantes n'ont pas
  été récupérées (succes HTTP 200).
"""

import os
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

# Concurrency fixe : nombre de requêtes HTTP en parallèle
CONCURRENCY = 100

# Pause fixe entre lots (en secondes)
SLEEP_BETWEEN_LOTS = 30

# Seuils disque
WARN_DISK_GB = 5
CRITICAL_DISK_GB = 2
YEAR_SKIP_DISK_GB = 3

# Estimation temps / année
MAX_JOB_MINUTES = 360.0          # limite GitHub Actions (6h)
SAFETY_MARGIN_MINUTES = 15.0     # marge de sécurité (commit, fin de job, etc.)
AVG_COURSES_PER_MINUTE = 200.0   # ton observation: ~200 courses/min
PER_YEAR_OVERHEAD_MIN = 5.0      # overhead fixe par année (git, etc.)
JOB_START_ENV = "JOB_START_EPOCH"

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
# Time estimation helpers
# =========================
def estimate_year_minutes(year: str, year_courses: int) -> float:
    """Estimation de la durée pour une année, en minutes."""
    return (year_courses / AVG_COURSES_PER_MINUTE) + PER_YEAR_OVERHEAD_MIN


def can_process_year(year: str, year_courses: int) -> bool:
    """
    Décide si on a assez de temps pour traiter cette année
    avant d’atteindre la limite de 6h du job GitHub Actions.

    Utilise la variable d’environnement JOB_START_EPOCH définie
    dans le workflow (premier step du job).
    """
    start_epoch_str = os.environ.get(JOB_START_ENV)
    if not start_epoch_str:
        # On ne connait pas l’heure de début -> on ne force pas de limite.
        return True

    try:
        start_epoch = float(start_epoch_str)
    except ValueError:
        return True

    now = time.time()
    elapsed_min = (now - start_epoch) / 60.0
    remaining_min = MAX_JOB_MINUTES - elapsed_min - SAFETY_MARGIN_MINUTES

    est_min = estimate_year_minutes(year, year_courses)

    print(
        f"⏱️  Estimation pour l'année {year}: {year_courses} courses "
        f"→ ~{est_min:.1f} min. "
        f"Temps écoulé: ~{elapsed_min:.1f} min, "
        f"marge restante avant 6h: ~{remaining_min:.1f} min."
    )

    if remaining_min <= 0:
        print("⚠️  Plus de marge suffisante, arrêt immédiat.")
        return False

    if est_min > remaining_min:
        print(
            f"⚠️  On NE traite PAS l'année {year}, "
            "car on estime qu'on n'aura pas le temps de la terminer avant la limite 6h."
        )
        return False

    return True

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
async def scrape_year(year: str, dates_courses: dict):
    """
    Scrape toutes les courses manquantes pour une année, en respectant l'ordre
    date → réunion → course, avec une concurrency fixe et des lots successifs.

    Retourne: (stats, year_complete)
      - stats: dict de stats sur l'année
      - year_complete: True si toutes les courses ont été récupérées (succès == total)
    """
    print(f"\n{'=' * 80}")
    print(f"ANNÉE {year}")
    print(f"{'=' * 80}\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque disponible: {free_gb:.2f} GB")

    if free_gb < YEAR_SKIP_DISK_GB:
        print("⚠️  Espace insuffisant pour traiter cette année, on saute.")
        return {
            "total": 0,
            "success": 0,
            "errors": 0,
            "lots": 0,
            "stopped_disk": True,
        }, False

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
        return {
            "total": 0,
            "success": 0,
            "errors": 0,
            "lots": 0,
            "stopped_disk": False,
        }, True

    pending = list(all_courses)

    stats = {
        "total": total_courses,
        "success": 0,
        "errors": 0,       # erreurs rencontrées (mais toutes sont retentées)
        "lots": 0,
        "stopped_disk": False,
    }

    lot_index = 0

    async with aiohttp.ClientSession() as session:
        while pending:
            if check_disk_space_critical():
                print("⚠️  Arrêt pour manque d'espace disque.")
                stats["stopped_disk"] = True
                break

            lot_index += 1
            stats["lots"] += 1

            free_gb = get_disk_space_gb()
            lot_size = min(CONCURRENCY, len(pending))
            current_lot = pending[:lot_size]
            pending = pending[lot_size:]

            first_idx = total_courses - len(pending) - lot_size + 1
            last_idx = total_courses - len(pending)

            print(
                f"\n  🧩 Lot #{lot_index}: courses {first_idx}-{last_idx}/{total_courses} "
                f"(taille lot: {lot_size}, concurrency: {CONCURRENCY})"
            )
            print(f"  💾 Espace libre avant lot: {free_gb:.2f} GB")
            print(f"  ⏱️  Pause fixe entre lots: {SLEEP_BETWEEN_LOTS}s")

            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [
                _scrape_one_course(sem, session, date_str, reunion_slug, course_file, filepath)
                for (date_str, reunion_slug, course_file, filepath) in current_lot
            ]

            lot_start = time.time()
            lot_success = 0
            lot_errors = 0
            retry_failed = []

            # On consomme toutes les tâches, et TOUT ce qui n'est pas 200
            # est remis dans retry_failed pour le lot suivant (429 + autres).
            for (date_str, reunion_slug, course_file, filepath), coro in zip(
                current_lot, asyncio.as_completed(tasks)
            ):
                status, err = await coro
                if status == 200:
                    lot_success += 1
                else:
                    lot_errors += 1
                    retry_failed.append((date_str, reunion_slug, course_file, filepath))

            lot_duration = time.time() - lot_start

            stats["success"] += lot_success
            stats["errors"] += lot_errors

            if retry_failed:
                print(f"  🔁 {lot_errors} courses en erreur seront retentées au lot suivant.")
                pending = retry_failed + pending

            print(
                f"  ⏱️  Lot #{lot_index} terminé en {lot_duration:.2f}s "
                f"(succès: {lot_success}, erreurs: {lot_errors})"
            )
            print(f"  💾 Espace libre après lot: {get_disk_space_gb():.2f} GB")

            if pending:
                print(f"  ⏳ Pause de {SLEEP_BETWEEN_LOTS}s avant le lot suivant...")
                await asyncio.sleep(SLEEP_BETWEEN_LOTS)

    year_complete = (stats["success"] == total_courses) and not stats["stopped_disk"]

    print(f"\n{'=' * 80}")
    print(f"RÉSUMÉ ANNÉE {year}")
    print(f"{'=' * 80}")
    print(f"  Total prévu:      {stats['total']}")
    print(f"  ✓ Succès:         {stats['success']}")
    print(f"  ✗ Erreurs (toutes retentées au max): {stats['errors']}")
    print(f"  🔁 Nombre de lots: {stats['lots']}")
    print(f"  💾 Espace final:   {get_disk_space_gb():.2f} GB")
    if not year_complete:
        print("  ⚠️  Année NON COMPLÈTE (certaines courses n'ont jamais réussi).")
    else:
        print("  ✅ Année COMPLÈTE (toutes les courses ont été récupérées).")
    print(f"{'=' * 80}\n")

    return stats, year_complete

# =========================
# Git operations
# =========================
def git_commit_year(year: str) -> None:
    """Commit et push les changements pour l'année donnée."""
    print(f"\n📤 Git commit pour l'année {year}...")

    target_path = f"{REPO_ROOT}/{year}"

    try:
        subprocess.run(
            ["git", "config", "user.name", "GitHub Actions Bot"],
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.email", "actions@github.com"],
            check=True,
        )

        # Y a-t-il des changements pour cette année ?
        status = subprocess.run(
            ["git", "status", "--porcelain", target_path],
            capture_output=True,
            text=True,
            check=True,
        )
        if not status.stdout.strip():
            print("  ℹ️  Aucun changement pour cette année")
            return

        # Stage uniquement cette année
        subprocess.run(["git", "add", target_path], check=True)

        files_changed = status.stdout.count("\n")
        commit_msg = f"Re-scrape: {year} - {files_changed} fichiers modifiés/ajoutés"
        timestamp_msg = f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"

        subprocess.run(
            ["git", "commit", "-m", commit_msg, "-m", timestamp_msg],
            check=True,
        )

        # Intégrer les commits des autres jobs sans réécrire l'historique
        subprocess.run(
            ["git", "pull", "--no-rebase", "--no-edit", "origin", "main"],
            check=False,
        )

        # Push
        subprocess.run(["git", "push", "origin", "HEAD:main"], check=True)

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
        "--years",
        type=str,
        nargs="*",
        help="(Optionnel) Liste d'années à traiter (ex: 2008 2011 2014). "
             "Si non fourni, toutes les années présentes dans verification_report.txt.",
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

    # Filtrage par années si fourni
    if args.years:
        allowed = set(args.years)
        missing_by_year = {
            y: data for (y, data) in missing_by_year.items() if y in allowed
        }
        if not missing_by_year:
            print(f"ℹ️  Aucune course manquante pour les années demandées: {sorted(allowed)}")
            return
        print(f"➡️  Années filtrées: {', '.join(sorted(missing_by_year.keys()))}\n")

    total_courses = sum(
        len(courses)
        for year_data in missing_by_year.values()
        for courses in year_data.values()
    )
    print(f"📊 {len(missing_by_year)} années avec courses manquantes")
    print(f"📊 {total_courses} courses manquantes au total\n")

    courses_planned = 0
    for year in sorted(missing_by_year.keys()):
        year_courses = sum(len(c) for c in missing_by_year[year].values())

        if args.max_courses and courses_planned >= args.max_courses:
            print(f"⚠️  Limite globale atteinte (~{courses_planned} courses planifiées).")
            break

        # Vérifier si on a assez de temps pour cette année
        if not can_process_year(year, year_courses):
            print("⏹️  Arrêt anticipé pour éviter de gaspiller un run presque terminé.")
            break

        print(f"➡️  Traitement de l'année {year} ({year_courses} courses prévues)")
        year_stats, year_complete = await scrape_year(year, missing_by_year[year])

        if year_complete:
            git_commit_year(year)
            courses_planned += year_courses
        else:
            print(f"⚠️  Année {year} incomplète, aucun commit n'a été effectué pour cette année.")
            # On ne met pas à jour courses_planned, puisqu'on n'a pas une année terminée.

    print("\n" + "=" * 80)
    print("SCRAPING TERMINÉ")
    print(f"💾 Espace disque final: {get_disk_space_gb():.2f} GB")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
