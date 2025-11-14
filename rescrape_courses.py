# -*- coding: utf-8 -*-
"""
Re-scraping des courses manquantes ZEturf via wget en parallèle.

- Parse verification_report.txt pour identifier les courses manquantes
- Reconstruit les URLs directement depuis les noms de fichiers
- Utilise wget lancé en sous-processus, avec une concurrency élevée
- Chaque course est téléchargée dans le bon sous-dossier
- Time sleep fixe de 1 seconde après chaque wget
- Traitement année par année, avec commit Git après chaque année
"""

import os
import re
import time
import asyncio
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import subprocess
import shutil

# =========================
# Configuration générale
# =========================
BASE = "https://www.zeturf.fr"
REPO_ROOT = "resultats-et-rapports"

# Concurrency "max raisonnable" en fonction du runner
CPU_COUNT = os.cpu_count() or 2
CONCURRENCY = min(128, CPU_COUNT * 32)  # ex: 2 CPU -> 64 wget en parallèle

# Seuils disque
WARN_DISK_GB = 5
CRITICAL_DISK_GB = 2
YEAR_SKIP_DISK_GB = 3


# =========================
# Helpers disque
# =========================
def get_disk_space_gb() -> float:
    """Espace disque disponible (en GB) sur /."""
    stat = shutil.disk_usage("/")
    return stat.free / (1024 ** 3)


def check_disk_space_critical() -> bool:
    """Retourne True si l'espace disque est critique (< 2GB)."""
    free_gb = get_disk_space_gb()
    if free_gb < CRITICAL_DISK_GB:
        print(f"\n⚠️  ALERTE: Espace disque critique: {free_gb:.2f} GB restants")
        print("Arrêt du scraping pour éviter saturation...")
        return True
    return False


# =========================
# Helpers chemins
# =========================
def get_date_directory(date_str: str) -> Path:
    """Chemin du dossier de la date: resultats-et-rapports/YYYY/MM/YYYY-MM-DD/."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str


# =========================
# Parsing du rapport
# =========================
def parse_missing_courses(report_path: Path = Path("verification_report.txt")):
    """
    Parse verification_report.txt pour récupérer les courses manquantes.

    Format attendu dans le rapport:
        DATE: 2006-04-16 - STATUS: INCOMPLETE
        ❌ R1-auteuil/R1C2-prix-du-president-de-la-republique.html

    Retour:
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

            if line.startswith("DATE:") and "STATUS:" in line:
                m = re.search(r"DATE:\s*(\d{4}-\d{2}-\d{2})", line)
                if m:
                    current_date = m.group(1)

            elif current_date and line.startswith("❌") and "/" in line and ".html" in line:
                # Exemple: ❌ R1-auteuil/R1C2-prix-xxx.html
                m = re.search(r"❌\s*([^/]+)/([^/]+\.html)", line)
                if m:
                    reunion_slug = m.group(1)
                    course_file = m.group(2)
                    year = current_date[:4]
                    missing[year][current_date].append((reunion_slug, course_file))

    return dict(missing)


# =========================
# Construction d'URL
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
# wget (async, via sous-processus)
# =========================
async def download_course_with_wget(
    sem: asyncio.Semaphore,
    date_str: str,
    reunion_slug: str,
    course_file: str,
    filepath: Path,
    url: str,
    index: int,
    total: int,
):
    """
    Télécharge une course avec wget.

    Retourne:
        "ok"    si succès
        "skip"  si déjà présent
        "error" sinon
    """
    async with sem:
        # Si le fichier existe déjà et n'est pas vide, on le saute
        if filepath.exists() and filepath.stat().st_size > 0:
            print(f"[{index}/{total}] = {date_str} {reunion_slug}/{course_file} (déjà présent)")
            return "skip"

        # S'assurer que le dossier existe
        filepath.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            "wget",
            "-q",           # mode silencieux (on log nous-mêmes)
            "-T", "30",     # timeout 30s
            "-O", str(filepath),
            url,
        ]

        start = time.time()
        proc = await asyncio.create_subprocess_exec(*cmd)
        rc = await proc.wait()
        duration = time.time() - start

        # Time sleep fixe 1 seconde
        await asyncio.sleep(1)

        if rc == 0:
            print(f"[{index}/{total}] ✓ {date_str} {reunion_slug}/{course_file} ({duration:.2f}s)")
            return "ok"
        else:
            print(f"[{index}/{total}] ✗ {date_str} {reunion_slug}/{course_file} (code {rc}, {duration:.2f}s)")
            return "error"


# =========================
# Scraping d'une année
# =========================
async def scrape_year(year: str, courses: list[tuple[str, str, str]]):
    """
    Scrape toutes les courses d'une année avec wget en parallèle.

    courses = liste de (date_str, reunion_slug, course_file)
    """
    print(f"\n{'=' * 80}")
    print(f"ANNÉE {year}")
    print(f"{'=' * 80}\n")

    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque disponible: {free_gb:.2f} GB")

    if free_gb < YEAR_SKIP_DISK_GB:
        print("⚠️  Espace insuffisant pour traiter cette année, on saute.")
        return

    if not courses:
        print("Aucune course à traiter pour cette année.")
        return

    total = len(courses)
    print(f"📊 {total} courses à récupérer pour {year}")
    print(f"⚙️  Concurrency (wget en parallèle): {CONCURRENCY}")
    print("⏱️  Time sleep entre téléchargements: 1s\n")

    sem = asyncio.Semaphore(CONCURRENCY)

    tasks = []
    for idx, (date_str, reunion_slug, course_file) in enumerate(courses, start=1):
        if check_disk_space_critical():
            print("⚠️  Arrêt de l'année en cours pour manque d'espace disque.")
            break

        date_dir = get_date_directory(date_str)
        filepath = date_dir / reunion_slug / course_file
        url = build_course_url(date_str, reunion_slug, course_file)

        tasks.append(
            download_course_with_wget(
                sem,
                date_str,
                reunion_slug,
                course_file,
                filepath,
                url,
                idx,
                total,
            )
        )

    if not tasks:
        print("ℹ️  Aucun téléchargement lancé pour cette année (espace disque critique ?)")
        return

    results = await asyncio.gather(*tasks, return_exceptions=False)

    success = sum(1 for r in results if r == "ok")
    skipped = sum(1 for r in results if r == "skip")
    errors = sum(1 for r in results if r == "error")

    print(f"\n{'=' * 80}")
    print(f"RÉSUMÉ ANNÉE {year}")
    print(f"{'=' * 80}")
    print(f"  Total prévu: {total}")
    print(f"  ✓ Succès:    {success}")
    print(f"  = Skipped:   {skipped}")
    print(f"  ✗ Erreurs:   {errors}")
    print(f"  💾 Espace final: {get_disk_space_gb():.2f} GB")
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
        commit_msg = f"Re-scrape (wget): {year} - {files_changed} fichiers modifiés/ajoutés"
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
        description="Re-scrape des courses ZEturf manquantes via wget en parallèle"
    )
    parser.add_argument(
        "--max-courses",
        type=int,
        default=None,
        help="(Optionnel) Limite globale de courses à traiter (toutes années confondues)",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("RE-SCRAPING DES COURSES MANQUANTES (wget parallèle)")
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
