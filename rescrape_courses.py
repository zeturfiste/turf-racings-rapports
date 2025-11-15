# -*- coding: utf-8 -*-
"""
Re-scraping intelligent des courses ZEturf manquantes.

Principes clés :
- Lecture de verification_report.txt pour trouver les courses manquantes
- Traitement par année, dans l'ordre date / réunion / course
- Requêtes HTTP asynchrones avec aiohttp, concurrency fixe = 100
- Lots de 100 requêtes en parallèle, avec pause de 30s entre les lots
- AUCUNE course "skippée" : si 429 ou erreur réseau, on retente dans le lot suivant
- On ne commit une année QUE si toutes les courses ciblées pour cette année sont récupérées (aucun échec définitif)
- Commit + push robuste avec fetch + rebase + retries
"""

import asyncio
import aiohttp
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# =========================
# Configuration générale
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

# Concurrency et rythme
CONCURRENCY = 100          # 100 requêtes en parallèle
BATCH_SLEEP_SECONDS = 30   # pause fixe de 30s entre lots

# Seuil disque
CRITICAL_FREE_GB = 2.0     # arrêt si espace < 2 Go

# =========================
# Data classes
# =========================

@dataclass
class CourseTask:
    year: str
    date: str          # "YYYY-MM-DD"
    reunion_slug: str  # ex: "R1-auteuil"
    course_file: str   # ex: "R1C2-prix-du-president-de-la-republique.html"

@dataclass
class YearResult:
    year: str
    total_courses: int
    success_count: int
    permanent_failures: List[CourseTask]
    aborted_for_disk: bool


# =========================
# Utilitaires disque
# =========================

def get_disk_space_gb() -> float:
    """Retourne l'espace disque libre (sur /) en Go."""
    usage = shutil.disk_usage("/")
    return usage.free / (1024 ** 3)


def check_disk_space_critical() -> bool:
    """Retourne True si l'espace disque est critique."""
    free_gb = get_disk_space_gb()
    if free_gb < CRITICAL_FREE_GB:
        print(
            f"\n⚠️  ALERTE DISQUE: {free_gb:.2f} Go libres < {CRITICAL_FREE_GB} Go. "
            "Arrêt pour éviter la saturation."
        )
        return True
    return False


# =========================
# Helpers chemins & URL
# =========================

def get_date_directory(date_str: str) -> Path:
    """Retourne le chemin du dossier de la date: resultats-et-rapports/YYYY/MM/YYYY-MM-DD/"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str


def save_html(filepath: Path, html: str) -> None:
    """Sauvegarde le HTML dans le fichier (en créant les dossiers)."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(html, encoding="utf-8")


def build_course_url(date_str: str, reunion_slug: str, course_file: str) -> str:
    """
    Reconstruit l'URL de la course depuis le nom de fichier et la réunion.

    Ex: date=2006-04-16, reunion=R1-auteuil, course_file=R1C2-prix-du-president-de-la-republique.html
    → https://www.zeturf.fr/fr/course/2006-04-16/R1C2-auteuil-prix-du-president-de-la-republique
    """
    # hippodrome: "R1-auteuil" → "auteuil"
    hippodrome = reunion_slug.split("-", 1)[1] if "-" in reunion_slug else reunion_slug

    # Remove .html extension
    course_slug = course_file.replace(".html", "")

    # course_slug = "R1C2-prix-du-president-de-la-republique"
    # code_part = "R1C2"
    # title_part = "prix-du-president-de-la-republique"
    dash_index = course_slug.find("-")
    if dash_index == -1:
        code_part = course_slug
        title_part = ""
    else:
        code_part = course_slug[:dash_index]
        title_part = course_slug[dash_index + 1 :]

    if title_part:
        path_part = f"{code_part}-{hippodrome}-{title_part}"
    else:
        path_part = f"{code_part}-{hippodrome}"

    return f"{BASE}/fr/course/{date_str}/{path_part}"


# =========================
# Parsing du rapport
# =========================

def parse_missing_courses(report_path: Path = Path("verification_report.txt")) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
    """
    Parse verification_report.txt pour extraire les courses manquantes.

    Format attendu dans le rapport:
        DATE: 2006-04-16 - STATUS: INCOMPLETE
          ❌ R1-auteuil/R1C2-prix-du-president-de-la-republique.html

    Retourne:
        dict[year][date] = [(reunion_slug, course_file), ...]
    """
    if not report_path.exists():
        print(f"❌ Fichier {report_path} introuvable")
        return {}

    missing: Dict[str, Dict[str, List[Tuple[str, str]]]] = {}
    current_date: Optional[str] = None

    with report_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()

            # En-tête de date
            if line.startswith("DATE:") and "STATUS:" in line:
                match = re.search(r"DATE:\s*(\d{4}-\d{2}-\d{2})", line)
                if match:
                    current_date = match.group(1)
                    year = current_date[:4]
                    missing.setdefault(year, {})
                    missing[year].setdefault(current_date, [])
                continue

            # Lignes de courses manquantes
            if current_date and line.startswith("❌") and "/" in line and ".html" in line:
                # Exemple: "❌ R1-auteuil/R1C2-prix-du-president-de-la-republique.html"
                match = re.search(r"❌\s*([^/]+)/([^/]+\.html)", line)
                if match:
                    reunion_slug = match.group(1)
                    course_file = match.group(2)
                    year = current_date[:4]
                    missing[year][current_date].append((reunion_slug, course_file))

    return missing


# =========================
# HTTP / scraping
# =========================

async def fetch_one(
    session: aiohttp.ClientSession,
    task: CourseTask,
    sem: asyncio.Semaphore,
) -> Tuple[CourseTask, Optional[int], Optional[str], Optional[Exception]]:
    """
    Récupère une course et retourne (task, status, html, exception).
    - status = code HTTP ou None si pas de réponse
    - html = contenu si status == 200
    - exception = éventuelle exception réseau
    """
    url = build_course_url(task.date, task.reunion_slug, task.course_file)
    try:
        async with sem:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                status = resp.status
                html = await resp.text()
                return task, status, html, None
    except Exception as e:
        return task, None, None, e


async def scrape_year(
    year: str,
    dates_courses: Dict[str, List[Tuple[str, str]]],
    max_courses_for_year: Optional[int] = None,
) -> YearResult:
    """
    Scrape toutes les courses manquantes pour une année.

    - dates_courses: dict[date] = [(reunion_slug, course_file), ...]
    - max_courses_for_year: limite optionnelle pour cette année (ou None = toutes)

    Retourne YearResult avec stats et éventuels échecs définitifs.
    """
    print("\n" + "=" * 80)
    print(f"ANNÉE {year}")
    print("=" * 80)

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque disponible au début de l'année {year}: {free_gb:.2f} Go\n")

    # Construire la liste plate des CourseTask dans l'ordre:
    # date croissante, puis ordre du rapport pour cette date
    all_tasks: List[CourseTask] = []
    for date_str in sorted(dates_courses.keys()):
        for reunion_slug, course_file in dates_courses[date_str]:
            all_tasks.append(
                CourseTask(
                    year=year,
                    date=date_str,
                    reunion_slug=reunion_slug,
                    course_file=course_file,
                )
            )

    if not all_tasks:
        print(f"ℹ️  Aucune course à récupérer pour {year}")
        return YearResult(year=year, total_courses=0, success_count=0, permanent_failures=[], aborted_for_disk=False)

    if max_courses_for_year is not None:
        # On ne traite que le début de la liste pour respecter la limite globale
        all_tasks = all_tasks[:max_courses_for_year]

    total = len(all_tasks)
    print(f"📊 {total} courses à récupérer pour {year}")

    pending: List[CourseTask] = list(all_tasks)
    success_count = 0
    permanent_failures: List[CourseTask] = []
    aborted_for_disk = False

    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        batch_index = 0
        while pending:
            batch_index += 1

            if check_disk_space_critical():
                aborted_for_disk = True
                break

            # Extraire un lot de CONCURRENCY au maximum
            batch = pending[:CONCURRENCY]
            pending = pending[CONCURRENCY:]

            start_idx = total - len(pending) - len(batch) + 1
            end_idx = total - len(pending)
            free_gb = get_disk_space_gb()

            print(f"\n  📦 Lot {batch_index}: courses {start_idx}-{end_idx}/{total} (taille: {len(batch)})")
            print(f"  💾 Espace libre: {free_gb:.2f} Go")

            t0 = time.time()
            tasks = [fetch_one(session, task, sem) for task in batch]
            results: List[Tuple[CourseTask, Optional[int], Optional[str], Optional[Exception]]] = await asyncio.gather(*tasks)
            elapsed = time.time() - t0

            # Courses à retenter (429, erreurs réseau, etc.)
            to_retry: List[CourseTask] = []

            for task, status, html, exc in results:
                # Log compact: date + chemin
                display_path = f"{task.date} {task.reunion_slug}/{task.course_file}"

                if status == 200 and html is not None:
                    # Succès
                    date_dir = get_date_directory(task.date)
                    reunion_dir = date_dir / task.reunion_slug
                    filepath = reunion_dir / task.course_file
                    save_html(filepath, html)
                    success_count += 1
                    print(f"      ✓ {display_path}")
                elif status == 429:
                    # Rate limit → retenter plus tard
                    print(f"      ⚠️  429 Too Many Requests, sera retenté: {display_path}")
                    to_retry.append(task)
                elif status is not None and 500 <= status < 600:
                    # Erreurs serveur, on retente
                    print(f"      ⚠️  HTTP {status}, sera retenté: {display_path}")
                    to_retry.append(task)
                elif status is None and exc is not None:
                    # Erreur réseau (timeout, etc.) → retente
                    msg = str(exc)
                    print(f"      ⚠️  Erreur réseau, sera retenté: {display_path} ({msg[:80]})")
                    to_retry.append(task)
                else:
                    # Erreur 4xx autre que 429 → échec définitif
                    code = status if status is not None else "ERR"
                    print(f"      ✗ Échec définitif {code}: {display_path}")
                    permanent_failures.append(task)

            # Ré-injecter les à-retenter en tête de la file pending pour ne rien sauter
            if to_retry:
                print(f"  🔁 {len(to_retry)} courses seront retentées au lot suivant.")
                pending = to_retry + pending

            print(
                f"  ⏱️  Lot {batch_index} terminé en {elapsed:.1f}s "
                f"(succès cumulés: {success_count}/{total}, en attente: {len(pending)}, "
                f"échecs définitifs: {len(permanent_failures)})"
            )

            if pending:
                # Pause fixe de 30s entre les lots, comme validé empiriquement
                print(f"  ⏸️  Pause {BATCH_SLEEP_SECONDS}s avant le prochain lot...")
                await asyncio.sleep(BATCH_SLEEP_SECONDS)

    print("\n" + "=" * 80)
    print(f"RÉSUMÉ ANNÉE {year}")
    print("=" * 80)
    print(f"✓ Succès:            {success_count}/{total}")
    print(f"✗ Échecs définitifs: {len(permanent_failures)}")
    print(f"💾 Espace final:     {get_disk_space_gb():.2f} Go")
    print("=" * 80 + "\n")

    return YearResult(
        year=year,
        total_courses=total,
        success_count=success_count,
        permanent_failures=permanent_failures,
        aborted_for_disk=aborted_for_disk,
    )


# =========================
# Git : commit & push robuste
# =========================

def git_commit_year(year: str) -> None:
    """Commit et push les changements pour l'année (robuste, avec retry)."""
    print(f"\n📤 Git commit pour l'année {year}...")

    try:
        # Config user (idempotent)
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

        files_changed = len(status.stdout.strip().splitlines())
        print(f"  ℹ️  Fichiers modifiés pour {year}: {files_changed}")

        # Stage uniquement l'année
        subprocess.run(["git", "add", f"{REPO_ROOT}/{year}"], check=True)

        # Déterminer la branche courante (main, etc.)
        branch_result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        branch = branch_result.stdout.strip() or "main"
        print(f"  ℹ️  Branche courante détectée: {branch}")

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
        print(f"  ✓ Commit local OK pour l'année {year}")

        # Push avec fetch/rebase et retry
        max_push_attempts = 20
        for attempt in range(1, max_push_attempts + 1):
            print(f"  🔁 Tentative de push {attempt}/{max_push_attempts} sur {branch}...")

            # Récupérer les dernières modifications distantes
            fetch_proc = subprocess.run(
                ["git", "fetch", "origin", branch],
                capture_output=True,
                text=True,
            )
            if fetch_proc.returncode != 0:
                print("    ⚠️  git fetch a échoué:")
                if fetch_proc.stdout.strip():
                    print("      STDOUT:", fetch_proc.stdout.strip())
                if fetch_proc.stderr.strip():
                    print("      STDERR:", fetch_proc.stderr.strip())
                time.sleep(2)
                continue

            # Rebase sur origin/branch pour intégrer les commits des autres jobs
            rebase_proc = subprocess.run(
                ["git", "rebase", f"origin/{branch}"],
                capture_output=True,
                text=True,
            )
            if rebase_proc.returncode != 0:
                print("    ⚠️  git rebase a échoué:")
                if rebase_proc.stdout.strip():
                    print("      STDOUT:", rebase_proc.stdout.strip())
                if rebase_proc.stderr.strip():
                    print("      STDERR:", rebase_proc.stderr.strip())
                # Si conflit improbable (normalement chaque job touche des années différentes),
                # on arrête tout de suite car ce n'est plus automatique.
                raise RuntimeError(f"Rebase impossible pour l'année {year} (voir logs ci-dessus).")

            # Push
            push_proc = subprocess.run(
                ["git", "push", "origin", branch],
                capture_output=True,
                text=True,
            )

            if push_proc.returncode == 0:
                print(f"  ✅ Push réussi pour l'année {year} sur {branch}")
                return

            # Push raté : log complet et on décide si on retente
            print("    ✗ Push échoué:")
            if push_proc.stdout.strip():
                print("      STDOUT:", push_proc.stdout.strip())
            if push_proc.stderr.strip():
                print("      STDERR:", push_proc.stderr.strip())

            stderr_lower = (push_proc.stderr or "").lower()

            # Cas typique : non-fast-forward (course entre jobs)
            if "non-fast-forward" in stderr_lower or "fetch first" in stderr_lower:
                print("    ℹ️  Conflit non-fast-forward (concurrence entre jobs). Retry après backoff.")
                time.sleep(3 * attempt)
                continue

            # Erreurs réseau transitoires
            if "timed out" in stderr_lower or "connection reset" in stderr_lower:
                print("    ℹ️  Erreur réseau transitoire. Retry après backoff.")
                time.sleep(3 * attempt)
                continue

            # Autre type d'erreur (permissions, branche protégée, etc.) → non récupérable automatiquement
            print("  ❌ Erreur de push non récupérable automatiquement.")
            push_proc.check_returncode()  # lève CalledProcessError

        # Si on arrive ici, 20 tentatives ont échoué
        raise RuntimeError(f"Push échoué après {max_push_attempts} tentatives pour l'année {year}")

    except subprocess.CalledProcessError as e:
        print(f"  ❌ Erreur Git pour l'année {year}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"  ❌ Erreur inattendue pendant le commit/push pour {year}: {e}")
        sys.exit(1)


# =========================
# Main
# =========================

async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Re-scraping intelligent des courses ZEturf manquantes"
    )
    parser.add_argument(
        "--max-courses",
        type=int,
        default=None,
        help="Limite globale de courses à traiter dans ce run (optionnel)",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Années à traiter, séparées par des virgules (ex: '2008,2011,2014'). "
             "Si non fourni: toutes les années présentes dans verification_report.txt.",
    )

    args = parser.parse_args()

    print("=" * 80)
    print("RE-SCRAPING DES COURSES MANQUANTES ZETURF")
    print("=" * 80 + "\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque initial: {free_gb:.2f} Go\n")
    if free_gb < 5.0:
        print("⚠️  WARNING: Espace disque faible (< 5 Go). Le run peut s'arrêter prématurément.\n")

    # Parse le rapport
    missing_by_year = parse_missing_courses()
    if not missing_by_year:
        print("✓ Aucune course manquante détectée dans verification_report.txt\n")
        return

    # Filtre éventuel par années
    years_filter: Optional[set] = None
    if args.years:
        years_filter = {y.strip() for y in args.years.split(",") if y.strip()}

    # Récap global
    total_missing_global = 0
    for year, dates_courses in missing_by_year.items():
        if years_filter and year not in years_filter:
            continue
        for courses in dates_courses.values():
            total_missing_global += len(courses)

    print(f"📊 Total de courses manquantes (toutes années considérées): {total_missing_global}")
    if args.max_courses is not None:
        print(f"📊 Limite globale pour ce run: {args.max_courses} courses\n")
    else:
        print()

    remaining_global = args.max_courses
    years = sorted(missing_by_year.keys())

    for year in years:
        if years_filter and year not in years_filter:
            continue

        # Déterminer combien de courses on peut traiter pour cette année,
        # en respectant éventuellement la limite globale.
        dates_courses = missing_by_year[year]
        year_total = sum(len(c) for c in dates_courses.values())

        if remaining_global is None:
            max_for_year = None
        else:
            if remaining_global <= 0:
                print(f"⚠️  Limite globale atteinte (max-courses). Arrêt avant l'année {year}.")
                break
            max_for_year = min(remaining_global, year_total)

        # Scraper l'année
        result = await scrape_year(year, dates_courses, max_courses_for_year=max_for_year)

        if remaining_global is not None:
            remaining_global -= result.success_count

        # Si on a dû s'arrêter pour manque de disque, on ne commit pas.
        if result.aborted_for_disk:
            print(f"⚠️  Année {year}: arrêt pour manque de disque. Aucun commit.")
            break

        # Si toutes les courses traitées pour cette année sont OK, on commit.
        if result.success_count == result.total_courses and not result.permanent_failures:
            git_commit_year(year)
        else:
            print(
                f"⚠️  Année {year}: toutes les courses prévues n'ont pas été récupérées "
                f"(succès {result.success_count}/{result.total_courses}, "
                f"échecs définitifs: {len(result.permanent_failures)}). Pas de commit."
            )

        # Si limite globale consommée, on s'arrête.
        if remaining_global is not None and remaining_global <= 0:
            print("⚠️  Limite globale max-courses atteinte. Arrêt du run.")
            break

    print("\n" + "=" * 80)
    print("FIN DU RE-SCRAPING")
    print(f"💾 Espace disque final: {get_disk_space_gb():.2f} Go")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
