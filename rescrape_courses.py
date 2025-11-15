# -*- coding: utf-8 -*-
"""
Re-scraping intelligent des courses manquantes ZEturf.

- Lit verification_report.txt pour trouver les courses manquantes
- Reconstruit les URLs à partir des noms de fichiers
- Scrape par année, dans l'ordre date / réunion / course croissant
- Concurrency fixe (lot de 100 courses en parallèle)
- Pause fixe entre les lots (30s) pour éviter les 429
- AUCUNE course n'est "skippée" silencieusement :
    * toutes les erreurs (HTTP 429, 5xx, etc.) sont retentées
    * si des courses restent en échec après plusieurs tentatives, l'année N'EST PAS commit
- Monitoring de l'espace disque (arrêt propre si disque trop plein)
"""
from __future__ import annotations

import asyncio
import aiohttp
import re
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

BASE = "https://www.zeturf.fr"
REPO_ROOT = "resultats-et-rapports"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

# Concurrency / timing
CONCURRENCY = 100                 # 100 requêtes en parallèle
SLEEP_BETWEEN_LOTS = 30.0         # 30 secondes entre 2 lots

# Robustesse
MAX_ATTEMPTS_PER_COURSE = 5       # On essaie chaque course jusqu'à 5 fois
CRITICAL_DISK_GB = 3.0            # On s'arrête si on descend sous ce seuil


@dataclass
class CourseTask:
    date_str: str
    reunion_slug: str
    course_file: str
    filepath: Path
    attempts: int = 0  # nombre de tentatives déjà effectuées


# =========================
# Disk monitoring
# =========================
def get_disk_space_gb() -> float:
    """Retourne l'espace disque disponible en GB pour '/'."""
    stat = shutil.disk_usage('/')
    return stat.free / (1024 ** 3)


def check_disk_space_critical() -> bool:
    """True si l'espace disque est critique, et log un message."""
    free_gb = get_disk_space_gb()
    if free_gb < CRITICAL_DISK_GB:
        print(f"\n⚠️  ALERTE DISQUE: {free_gb:.2f} GB restants (< {CRITICAL_DISK_GB} GB)")
        print("    Arrêt du scraping pour éviter la saturation du runner.")
        return True
    return False


# =========================
# Path helpers
# =========================
def get_date_directory(date_str: str) -> Path:
    """Retourne le chemin du dossier de la date: YYYY/MM/YYYY-MM-DD/"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str


def save_html(filepath: Path, html: str) -> None:
    """Sauvegarde le HTML dans le fichier (création des dossiers au besoin)."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(html, encoding="utf-8")


# =========================
# Parse verification report
# =========================
def parse_missing_courses(
    report_path: Path = Path("verification_report.txt"),
) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
    """
    Parse le rapport de vérification pour extraire les courses manquantes.

    Format attendu dans le rapport:
        DATE: 2006-04-16 - STATUS: INCOMPLETE
        ❌ R1-auteuil/R1C2-prix-du-president-de-la-republique.html

    Retourne:
        dict[year][date] = [(reunion_slug, course_file), ...]
    """
    if not report_path.exists():
        print(f"❌ Fichier {report_path} introuvable")
        return {}

    missing: Dict[str, Dict[str, List[Tuple[str, str]]]] = defaultdict(lambda: defaultdict(list))
    current_date: Optional[str] = None

    with report_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()

            # En-tête de date
            if line.startswith("DATE:") and "STATUS:" in line:
                m = re.search(r"DATE:\s*(\d{4}-\d{2}-\d{2})", line)
                if m:
                    current_date = m.group(1)
                continue

            if not current_date:
                continue

            # Lignes "❌ xxx/yyy.html"
            if line.startswith("❌") and "/" in line and ".html" in line:
                m = re.search(r"❌\s*([^/]+)/([^/]+\.html)", line)
                if m:
                    reunion_slug = m.group(1)
                    course_file = m.group(2)
                    year = current_date[:4]
                    missing[year][current_date].append((reunion_slug, course_file))

    # On renvoie un dict classique (pas defaultdict) pour éviter les surprises
    return {y: dict(dates) for y, dates in missing.items()}


# =========================
# URL reconstruction
# =========================
def build_course_url(date_str: str, reunion_slug: str, course_file: str) -> str:
    """
    Reconstruit l'URL de la course depuis le nom de fichier.

    Ex: date=2006-04-16, reunion=R1-auteuil, file=R1C2-prix-du-president-de-la-republique.html
    → https://www.zeturf.fr/fr/course/2006-04-16/R1C2-auteuil-prix-du-president-de-la-republique
    """
    hippodrome = reunion_slug.split("-", 1)[1] if "-" in reunion_slug else reunion_slug

    # "R1C2-prix-du-president-de-la-republique.html" -> "R1C2-prix-du-president-de-la-republique"
    base_slug = course_file[:-5] if course_file.endswith(".html") else course_file

    # On sépare code course ("R1C2") du titre
    code, _, title_part = base_slug.partition("-")
    if not code:
        # fallback grossier si le format est inattendu
        code = base_slug

    url = f"{BASE}/fr/course/{date_str}/{code}-{hippodrome}-{title_part}"
    return url


# =========================
# HTTP fetching
# =========================
async def fetch_course(
    session: aiohttp.ClientSession, url: str, retries: int = 3
) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Récupère le HTML d'une course.

    Retourne: (html ou None, status HTTP ou None, message d'erreur éventuel)
    """
    last_error: Optional[str] = None
    for attempt in range(retries):
        try:
            async with session.get(
                url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                html = await resp.text()
                return html, resp.status, None
        except asyncio.TimeoutError:
            last_error = "Timeout"
            await asyncio.sleep(2 * (attempt + 1))
        except Exception as e:  # noqa: BLE001
            last_error = f"{type(e).__name__}: {e}"
            await asyncio.sleep(2 * (attempt + 1))

    return None, None, last_error


async def fetch_and_save_course(
    session: aiohttp.ClientSession, task: CourseTask
) -> Tuple[CourseTask, Optional[int], Optional[str]]:
    """
    Fait 1 requête pour une course, sauvegarde si 200.

    Retourne: (task, status HTTP ou None, message d'erreur)
    """
    url = build_course_url(task.date_str, task.reunion_slug, task.course_file)
    html, status, error = await fetch_course(session, url)

    if status == 200 and html is not None:
        save_html(task.filepath, html)
        return task, 200, None

    if status is not None:
        return task, status, f"HTTP {status}"

    return task, None, error or "Erreur inconnue"


# =========================
# Scraping par année
# =========================
@dataclass
class YearResult:
    year: str
    total_courses: int
    success: int
    permanent_failures: List[CourseTask]
    aborted_for_disk: bool


async def scrape_year(
    year: str,
    dates_courses: Dict[str, List[Tuple[str, str]]],
    concurrency: int = CONCURRENCY,
    sleep_between_lots: float = SLEEP_BETWEEN_LOTS,
) -> YearResult:
    """
    Scrape toutes les courses manquantes d'une année.

    - On traite les courses dans l'ordre date/reunion/course croissant
    - On travaille par "lots" de taille = concurrency
    - Tous les statuts != 200 sont retentés jusqu'à MAX_ATTEMPTS_PER_COURSE
    - Si des courses restent en échec après MAX_ATTEMPTS_PER_COURSE tentatives,
      l'année est marquée comme incomplète (pas de commit).
    """
    print(f"\n{'=' * 80}")
    print(f"ANNÉE {year}")
    print(f"{'=' * 80}\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque au début de {year}: {free_gb:.2f} GB")

    if free_gb < CRITICAL_DISK_GB:
        print(f"⚠️  Espace insuffisant pour démarrer l'année {year}")
        return YearResult(year, 0, 0, [], aborted_for_disk=True)

    # Aplatir toutes les courses de l'année en gardant l'ordre
    all_tasks: List[CourseTask] = []
    for date_str in sorted(dates_courses.keys()):
        courses_list = dates_courses[date_str]
        for reunion_slug, course_file in courses_list:
            date_dir = get_date_directory(date_str)
            reunion_dir = date_dir / reunion_slug
            filepath = reunion_dir / course_file
            all_tasks.append(CourseTask(date_str, reunion_slug, course_file, filepath))

    total_courses = len(all_tasks)
    if total_courses == 0:
        print(f"ℹ️  Aucune course à traiter pour {year}")
        return YearResult(year, 0, 0, [], aborted_for_disk=False)

    print(f"📊 {total_courses} courses à récupérer pour {year}\n")

    # Pending + stats
    pending: List[CourseTask] = all_tasks[:]  # copie
    permanent_failures: List[CourseTask] = []
    success_count = 0
    lot_index = 0
    aborted_for_disk = False

    timeout = aiohttp.ClientTimeout(total=60)  # 1 minute max par requête
    conn = aiohttp.TCPConnector(limit=concurrency)

    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        while pending:
            if check_disk_space_critical():
                aborted_for_disk = True
                break

            lot_index += 1
            lot_size = min(concurrency, len(pending))
            current_lot = pending[:lot_size]
            pending = pending[lot_size:]

            free_gb = get_disk_space_gb()
            first_task = current_lot[0]
            last_task = current_lot[-1]
            print(
                f"\n  📦 Lot {lot_index}: courses "
                f"{success_count + 1}-{success_count + lot_size}/{total_courses} "
                f"(taille: {lot_size})"
            )
            print(f"  💾 Espace libre: {free_gb:.2f} GB")
            print(
                f"  🗓️  De {first_task.date_str} ({first_task.reunion_slug}) "
                f"à {last_task.date_str} ({last_task.reunion_slug})"
            )

            # Lancer les requêtes en parallèle
            tasks = [fetch_and_save_course(session, t) for t in current_lot]
            results = await asyncio.gather(*tasks)

            # Préparer la liste des courses à retenter
            next_pending: List[CourseTask] = []

            for task, status, error in results:
                label = f"{task.date_str} {task.reunion_slug}/{task.course_file}"

                if status == 200:
                    success_count += 1
                    print(f"      ✓ {label}")
                    continue

                # Erreur (HTTP ou exception réseau)
                task.attempts += 1
                if status == 429:
                    print(f"      ⚠️  {label} (429 Too Many Requests, tentative {task.attempts})")
                elif status is not None:
                    print(f"      ✗ {label} (HTTP {status}, tentative {task.attempts})")
                else:
                    print(f"      ✗ {label} ({error}, tentative {task.attempts})")

                if task.attempts < MAX_ATTEMPTS_PER_COURSE:
                    # On retentera cette course dans un lot suivant
                    next_pending.append(task)
                else:
                    # Echec définitif
                    permanent_failures.append(task)

            # Important : on remet les échecs (à retenter) au début de la file,
            # puis le reste des pending pour garder l'ordre global.
            pending = next_pending + pending

            # Si on a encore du travail à faire, on attend avant le prochain lot
            if pending:
                print(
                    f"  ⏱️  Pause de {sleep_between_lots:.0f}s avant le lot suivant "
                    f"(courses restantes: {len(pending)})"
                )
                await asyncio.sleep(sleep_between_lots)

    print(f"\n{'=' * 80}")
    print(f"RÉSUMÉ ANNÉE {year}")
    print(f"{'=' * 80}")
    print(f"✓ Succès:            {success_count}/{total_courses}")
    print(f"✗ Echecs définitifs: {len(permanent_failures)}")
    print(f"💾 Espace final:     {get_disk_space_gb():.2f} GB")
    if aborted_for_disk:
        print("⚠️  Année interrompue à cause de l'espace disque")
    print(f"{'=' * 80}\n")

    return YearResult(
        year=year,
        total_courses=total_courses,
        success=success_count,
        permanent_failures=permanent_failures,
        aborted_for_disk=aborted_for_disk,
    )


# =========================
# Git operations
# =========================
def git_commit_year(year: str) -> None:
    """Commit et push les changements pour l'année (si des fichiers ont changé)."""
    print(f"\n📤 Git commit pour l'année {year}...")
    try:
        subprocess.run(["git", "config", "user.name", "GitHub Actions Bot"], check=True)
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)

        # Vérifier s'il y a des changements dans l'arbre de l'année
        status = subprocess.run(
            ["git", "status", "--porcelain", f"{REPO_ROOT}/{year}"],
            capture_output=True,
            text=True,
            check=True,
        )
        if not status.stdout.strip():
            print("  ℹ️  Aucun fichier modifié pour cette année (rien à commit).")
            return

        # Compter (grossièrement) les fichiers affectés
        files_changed = len(status.stdout.strip().splitlines())

        # Stage uniquement l'année courante
        subprocess.run(["git", "add", f"{REPO_ROOT}/{year}"], check=True)

        # Commit
        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                f"Re-scrape: {year} - {files_changed} fichiers modifiés",
                "-m",
                f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            ],
            check=True,
        )

        # Push
        subprocess.run(["git", "push"], check=True)
        print(f"  ✓ Année {year} committée et poussée ({files_changed} fichiers)\n")

    except subprocess.CalledProcessError as e:
        print(f"  ✗ Erreur Git pour l'année {year}: {e}\n")


# =========================
# Main orchestrator
# =========================
async def main() -> None:
    import argparse
    import time

    parser = argparse.ArgumentParser(
        description="Re-scrape intelligent des courses ZEturf manquantes"
    )
    parser.add_argument(
        "--max-courses",
        type=int,
        default=None,
        help="Limite globale de courses à traiter (pour tester).",
    )
    parser.add_argument(
        "--years",
        type=str,
        default="",
        help="Liste d'années à traiter, séparées par des virgules (ex: 2008,2011,2014). "
             "Si vide, toutes les années présentes dans le rapport sont traitées.",
    )
    args = parser.parse_args()

    start_ts = time.time()

    print("=" * 80)
    print("RE-SCRAPING DES COURSES MANQUANTES")
    print("=" * 80 + "\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque initial: {free_gb:.2f} GB\n")

    if free_gb < CRITICAL_DISK_GB:
        print("❌ Espace disque déjà critique au démarrage, abandon.")
        return

    # Parser le rapport
    missing_by_year = parse_missing_courses()
    if not missing_by_year:
        print("✓ Aucune course manquante détectée dans verification_report.txt\n")
        return

    # Filtrage des années si --years est fourni
    if args.years.strip():
        requested_years = {y.strip() for y in args.years.split(",") if y.strip()}
        missing_by_year = {
            y: dates for (y, dates) in missing_by_year.items() if y in requested_years
        }
        if not missing_by_year:
            print("❌ Aucun des années demandées n'a de courses manquantes.")
            return

    # Résumé global
    total_courses = sum(
        len(courses)
        for year_data in missing_by_year.values()
        for courses in year_data.values()
    )
    print(f"📊 {len(missing_by_year)} années avec des courses manquantes")
    print(f"📊 {total_courses} courses manquantes au total\n")

    courses_processed = 0
    HARD_LIMIT_MINUTES = 355  # on se garde ~5 minutes de marge sous la limite GitHub (6h)

    for year in sorted(missing_by_year.keys()):
        # Limite globale de courses (option pour tests)
        if args.max_courses is not None and courses_processed >= args.max_courses:
            print(
                f"⚠️  Limite globale atteinte ({courses_processed}/{args.max_courses} courses), arrêt."
            )
            break

        # Estimation grossière du temps restant avant de démarrer une nouvelle année
        elapsed_min = (time.time() - start_ts) / 60.0
        remaining_min = HARD_LIMIT_MINUTES - elapsed_min
        year_course_count = sum(len(c) for c in missing_by_year[year].values())

        # Hypothèse : ~200 courses / minute par job (100 concu, lot toutes les 30s)
        ESTIMATED_COURSES_PER_MIN = 200.0
        estimated_min_for_year = year_course_count / ESTIMATED_COURSES_PER_MIN + 5.0  # +5min pour commit/git

        print(
            f"\n=== Préparation année {year} ===\n"
            f"Courses à traiter: {year_course_count}\n"
            f"Temps écoulé: {elapsed_min:.1f} min, reste ~{remaining_min:.1f} min "
            f"(limite soft {HARD_LIMIT_MINUTES}min)\n"
            f"Estimation temps pour {year}: {estimated_min_for_year:.1f} min\n"
        )

        if remaining_min < estimated_min_for_year:
            print(
                f"⚠️  On estime que l'année {year} ne tiendra pas dans le temps restant "
                f"({remaining_min:.1f} min). On s'arrête pour ne pas gaspiller des minutes."
            )
            break

        # Scraper l'année
        year_result = await scrape_year(year, missing_by_year[year])

        courses_processed += year_result.success

        # Si le disque est devenu critique, on arrête tout
        if year_result.aborted_for_disk:
            print(
                f"⚠️  Arrêt global: disque critique pendant l'année {year}. "
                f"Progression: {courses_processed}/{total_courses} courses téléchargées."
            )
            break

        # Si des échecs définitifs subsistent, on ne commit PAS cette année
        if year_result.permanent_failures:
            print(
                f"❌ L'année {year} n'est pas complète : "
                f"{len(year_result.permanent_failures)} courses en échec définitif."
            )
            print("   → AUCUN commit n'est fait pour cette année. À rejouer plus tard.")
            # Tu peux décider de `break` ici si tu préfères arrêter complètement.
            continue

        # Tout est OK pour cette année -> commit + push
        git_commit_year(year)

    print("\n" + "=" * 80)
    print("SCRAPING TERMINÉ")
    print(f"Courses téléchargées avec succès: {courses_processed}/{total_courses}")
    print(f"💾 Espace disque final: {get_disk_space_gb():.2f} GB")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
