#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
verify.py - 2 en 1 : vérification + re-scraping ZEturf

- Vérifie les dates, réunions FR et courses dans resultats-et-rapports.
- Écrit les éléments manquants dans verify_courses/<year>/missing_<year>.txt.
- Re-scrape ce qui manque :
    * MISSING_DATE   → page date + réunions FR + courses
    * MISSING_REUNION → réunion FR + courses
    * MISSING_COURSE  → course seule
- Commit & push par année (resultats-et-rapports/<year> + verify_courses/<year>).

Pensé pour être appelé depuis un workflow GitHub Actions par groupes d'années.
"""

import argparse
import os
import re
import time
import unicodedata
from datetime import datetime, date, timedelta
from pathlib import Path
from urllib.parse import urljoin
from collections import defaultdict
import subprocess

import requests
from bs4 import BeautifulSoup

# =========================
# Configuration générale
# =========================

BASE = "https://www.zeturf.fr"
DATE_URL_TPL = BASE + "/fr/resultats-et-rapports/{date}"  # {date} = YYYY-MM-DD

REPO_ROOT = Path("resultats-et-rapports")
VERIFY_ROOT = Path("verify_courses")

# Bornes globales (à adapter si besoin)
GLOBAL_START_DATE = date(2005, 4, 27)
GLOBAL_END_DATE = date(2025, 11, 11)

# HTTP session
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
})


def safe_get(url, referer=None, retries=4, timeout=30):
    """GET robuste avec quelques retry + petit sleep pour ne pas bourriner le site."""
    for i in range(retries):
        try:
            headers = {}
            if referer:
                headers["Referer"] = referer
            r = SESSION.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            # on thrott le rythme
            time.sleep(0.25)
            return r.text
        except Exception as e:
            if i == retries - 1:
                print(f"    [HTTP] Échec définitif sur {url}: {e}")
                raise
            wait = 1.5 * (i + 1)
            print(f"    [HTTP] Retry {i+1}/{retries} sur {url} dans {wait:.1f}s...")
            time.sleep(wait)


# =========================
# Helpers filesystem
# =========================

def slugify(text: str) -> str:
    """Slugify compatible avec ton arbo existante (ASCII only)."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text)
    text = text.strip("-").lower()
    return text


def get_date_directory(date_str: str) -> Path:
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    return REPO_ROOT / f"{dt.year:04d}" / f"{dt.month:02d}" / date_str


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def save_text(path: Path, text: str):
    ensure_dir(path.parent)
    # idempotent : si fichier existant non vide, on ne réécrit pas
    if path.exists() and path.stat().st_size > 128:
        return
    path.write_text(text, encoding="utf-8")


def read_date_html_if_exists(date_str: str):
    date_dir = get_date_directory(date_str)
    date_file = date_dir / f"{date_str}.html"
    if date_file.exists() and date_file.stat().st_size > 128:
        return date_file.read_text(encoding="utf-8")
    return None


# =========================
# Parsing HTML (réunions + courses)
# =========================

def parse_reunions_fr_from_html(date_str: str, html: str):
    """
    Parse une page de date et retourne les réunions FR attendues.

    Retourne une liste de dict:
    {
        "reunion_code": "R1",
        "hippodrome": "Cagnes-sur-Mer",
        "url": "https://www.zeturf.fr/fr/reunion/...",
        "date": "YYYY-MM-DD",
    }
    """
    soup = BeautifulSoup(html, "lxml")
    out = []

    root = soup.select_one("div#list-reunion")
    if not root:
        return out

    for tr in root.select("table.programme tbody tr.item"):
        a = tr.select_one('a[href^="/fr/reunion/"][data-tc-pays="FR"]')
        if not a:
            continue

        href = a.get("href", "") or ""
        reunion_url = urljoin(BASE, href)

        numero_td = tr.select_one("td.numero")
        reunion_code = None
        if numero_td:
            txt = numero_td.get_text(strip=True)
            reunion_code = txt.replace("FR", "R").strip() or None
        if not reunion_code:
            m = re.search(r"/fr/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
            if m:
                reunion_code = m.group(1)

        hippo_node = tr.select_one("td.nom h2 span span") or tr.select_one("td.nom h2 span, td.nom h2")
        hippodrome = hippo_node.get_text(strip=True) if hippo_node else ""
        if not hippodrome:
            title = a.get("title", "")
            if title:
                hippodrome = title.strip()

        if not reunion_code:
            continue

        out.append({
            "reunion_code": reunion_code,
            "hippodrome": hippodrome,
            "url": reunion_url,
            "date": date_str,
        })
    return out


def parse_courses_from_reunion_html(reunion_html: str, reunion_url: str):
    """
    Parse une page de réunion et retourne (courses, html).

    courses = [
      {
        "numero": 1,
        "code": "C1",
        "heure": "13:50",
        "intitule": "Prix de ...",
        "url": "https://www.zeturf.fr/fr/course/..."
      }, ...
    ]
    """
    soup = BeautifulSoup(reunion_html, "lxml")

    frise = soup.select_one("#frise-course .strip2.active")
    if not frise:
        frise = soup.select_one("#frise-course .strip2")

    courses = []
    if not frise:
        return courses, reunion_html

    for a in frise.select("ul.scroll-content li.scroll-element a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        course_url = urljoin(BASE, href)

        num_text = ""
        num_sp = a.select_one("span.numero")
        if num_sp:
            num_text = num_sp.get_text(strip=True)

        m = re.search(r"C(\d+)", href)
        numero = int(num_text) if num_text.isdigit() else (int(m.group(1)) if m else None)

        title = a.get("title", "").strip()
        if " - " in title:
            heure, intitule = title.split(" - ", 1)
        else:
            heure, intitule = None, title or None

        code = f"C{numero}" if numero is not None else (m.group(0) if m else None)

        courses.append({
            "numero": numero,
            "code": code,
            "heure": heure,
            "intitule": intitule,
            "url": course_url,
        })

    if courses and all(c["numero"] is not None for c in courses):
        courses.sort(key=lambda c: c["numero"])

    return courses, reunion_html


# =========================
# Vérification (détection MISSING_*)
# =========================

def detect_missing_courses_for_reunion(date_str: str,
                                       reunion_slug: str,
                                       reunion_dir: Path,
                                       reunion_file: Path,
                                       missing_file,
                                       stats: dict):
    try:
        html = reunion_file.read_text(encoding="utf-8")
    except Exception:
        # si on n'arrive même pas à lire, on traitera la réunion comme manquante
        missing_file.write(f"MISSING_REUNION;{date_str};{reunion_slug}\n")
        stats["missing_reunions"] += 1
        return

    courses, _ = parse_courses_from_reunion_html(html, "")
    if not courses:
        return

    reunion_code = reunion_slug.split("-", 1)[0]

    for c in courses:
        intitule = c["intitule"]
        code = c["code"]
        slug_title = slugify(intitule) if intitule else ""
        if not slug_title:
            slug_title = "course"

        code_part = f"{reunion_code}{code or ''}"
        filename = f"{code_part}-{slug_title}.html"
        course_path = reunion_dir / filename

        if not course_path.exists() or course_path.stat().st_size == 0:
            missing_file.write(f"MISSING_COURSE;{date_str};{reunion_slug};{filename}\n")
            stats["missing_courses"] += 1


def detect_missing_for_date(date_str: str, missing_file, stats: dict):
    date_dir = get_date_directory(date_str)
    date_file = date_dir / f"{date_str}.html"

    # Cas 1 : dossier date ou fichier date absents → on traite comme MISSING_DATE
    if not date_dir.exists() or not date_file.exists() or date_file.stat().st_size == 0:
        missing_file.write(f"MISSING_DATE;{date_str}\n")
        stats["missing_dates"] += 1
        return

    try:
        html = date_file.read_text(encoding="utf-8")
    except Exception:
        missing_file.write(f"MISSING_DATE;{date_str}\n")
        stats["missing_dates"] += 1
        return

    soup = BeautifulSoup(html, "lxml")
    container = soup.select_one("div#list-reunion")
    if not container:
        # page date inutilisable → on re-scrapera la date entière
        missing_file.write(f"MISSING_DATE;{date_str}\n")
        stats["missing_dates"] += 1
        return

    reunions = parse_reunions_fr_from_html(date_str, html)
    if not reunions:
        # aucune réunion FR → rien à rescaper pour cette date
        return

    for r in reunions:
        reunion_code = r["reunion_code"]
        hippodrome = r["hippodrome"] or ""
        reunion_slug = f"{reunion_code}-{slugify(hippodrome)}"
        date_dir = get_date_directory(date_str)
        reunion_dir = date_dir / reunion_slug
        reunion_file = reunion_dir / f"{reunion_slug}.html"

        if (not reunion_dir.exists()) or (not reunion_file.exists()) or reunion_file.stat().st_size == 0:
            missing_file.write(f"MISSING_REUNION;{date_str};{reunion_slug}\n")
            stats["missing_reunions"] += 1
            continue

        # Réunion présente → on cherche les courses manquantes
        detect_missing_courses_for_reunion(
            date_str, reunion_slug, reunion_dir, reunion_file, missing_file, stats
        )


def compute_year_range(year: int):
    """Retourne (start_date_str, end_date_str) bornés par GLOBAL_START/END, ou (None, None) si hors plage."""
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)

    start = max(year_start, GLOBAL_START_DATE)
    end = min(year_end, GLOBAL_END_DATE)

    if start > end:
        return None, None

    return start.isoformat(), end.isoformat()


def collect_missing_for_year(year: int, missing_path: Path, start_str: str, end_str: str):
    """Parcourt toutes les dates de l'année et écrit les MISSING_* dans missing_path."""
    start_dt = date.fromisoformat(start_str)
    end_dt = date.fromisoformat(end_str)

    ensure_dir(missing_path.parent)

    stats = {
        "dates_total": (end_dt - start_dt).days + 1,
        "missing_dates": 0,
        "missing_reunions": 0,
        "missing_courses": 0,
    }

    print(f"\n{'='*80}")
    print(f"VÉRIFICATION ANNÉE {year} ({start_str} → {end_str})")
    print(f"{'='*80}")

    with missing_path.open("w", encoding="utf-8") as f:
        cur = start_dt
        idx = 1
        while cur <= end_dt:
            date_str = cur.isoformat()
            print(f"[VERIFY] {year} {idx}/{stats['dates_total']} {date_str}")
            detect_missing_for_date(date_str, f, stats)
            idx += 1
            cur += timedelta(days=1)

    total_missing = stats["missing_dates"] + stats["missing_reunions"] + stats["missing_courses"]

    print(f"\nRésumé année {year}:")
    print(f"  Dates vérifiées:    {stats['dates_total']}")
    print(f"  Dates manquantes:   {stats['missing_dates']}")
    print(f"  Réunions manquantes:{stats['missing_reunions']}")
    print(f"  Courses manquantes: {stats['missing_courses']}")
    print(f"  Total manquants:    {total_missing}")

    return stats


# =========================
# Rescrape (dates, réunions, courses)
# =========================

def scrape_full_date(date_str: str):
    """Re-scrape une date complète : page date + réunions FR + courses."""
    print(f"\n[SCRAPE DATE] {date_str}")
    date_dir = get_date_directory(date_str)
    ensure_dir(date_dir)

    date_url = DATE_URL_TPL.format(date=date_str)
    try:
        date_html = safe_get(date_url)
    except Exception as e:
        print(f"  [ERREUR] Impossible de récupérer la page date {date_str}: {e}")
        return

    date_file = date_dir / f"{date_str}.html"
    save_text(date_file, date_html)

    reunions = parse_reunions_fr_from_html(date_str, date_html)
    if not reunions:
        print("  (Aucune réunion FR détectée)")
        return

    for r in reunions:
        scrape_reunion_from_info(date_str, r)


def scrape_reunion_from_info(date_str: str, reunion_info: dict):
    """Re-scrape une réunion FR (page réunion + courses) à partir d'un dict reunion_info."""
    reunion_code = reunion_info["reunion_code"]
    hippo = reunion_info["hippodrome"] or ""
    reunion_slug = f"{reunion_code}-{slugify(hippo) or 'hippodrome'}"

    print(f"  [SCRAPE RÉUNION] {date_str} {reunion_slug}")

    reunion_url = reunion_info["url"]
    date_url = DATE_URL_TPL.format(date=date_str)

    try:
        reunion_html = safe_get(reunion_url, referer=date_url)
    except Exception as e:
        print(f"    [ERREUR] Récup réunion {reunion_slug}: {e}")
        return

    date_dir = get_date_directory(date_str)
    reunion_dir = date_dir / reunion_slug
    ensure_dir(reunion_dir)

    reunion_file = reunion_dir / f"{reunion_slug}.html"
    save_text(reunion_file, reunion_html)

    courses, _ = parse_courses_from_reunion_html(reunion_html, reunion_url)
    if not courses:
        print("    (Aucune course visible dans la frise)")
        return

    for c in courses:
        scrape_course_from_info(date_str, reunion_slug, reunion_code, c, reunion_url)


def scrape_course_from_info(date_str: str,
                            reunion_slug: str,
                            reunion_code: str,
                            course_info: dict,
                            reunion_url: str):
    """Re-scrape une course à partir de la description trouvée dans la page réunion."""
    cname = course_info["intitule"] or course_info["code"] or "course"
    cslug = slugify(cname) or (course_info["code"] or "Cx")
    code = course_info["code"] or ""
    course_code = f"{reunion_code}{code}".replace("None", "")
    filename = f"{course_code}-{cslug}.html"

    date_dir = get_date_directory(date_str)
    reunion_dir = date_dir / reunion_slug
    ensure_dir(reunion_dir)

    course_path = reunion_dir / filename
    if course_path.exists() and course_path.stat().st_size > 128:
        return

    print(f"    [SCRAPE COURSE] {date_str} {reunion_slug}/{filename}")

    try:
        course_html = safe_get(course_info["url"], referer=reunion_url)
    except Exception as e:
        print(f"    [WARN] Course {course_code}: {e}")
        return

    save_text(course_path, course_html)


def scrape_reunion(date_str: str, reunion_slug: str):
    """
    Re-scrape une réunion FR manquante :
    - si page date absente ou inutilisable → on fait scrape_full_date(date_str)
    - sinon, on retrouve la réunion dans la page date et on la scrape seule.
    """
    print(f"\n[SCRAPE RÉUNION ISOLEE] {date_str} {reunion_slug}")

    html = read_date_html_if_exists(date_str)
    if html is None:
        print("  Page date absente → scraping de la date complète.")
        scrape_full_date(date_str)
        return

    reunions = parse_reunions_fr_from_html(date_str, html)
    if not reunions:
        print("  Aucune réunion FR dans la page date → scraping de la date complète.")
        scrape_full_date(date_str)
        return

    target = None
    for r in reunions:
        code = r["reunion_code"]
        hippo = r["hippodrome"] or ""
        slug = f"{code}-{slugify(hippo)}"
        if slug == reunion_slug:
            target = r
            break

    if target is None:
        print("  Réunion non trouvée dans la page date → scraping de la date complète.")
        scrape_full_date(date_str)
        return

    scrape_reunion_from_info(date_str, target)


def build_course_url(date_str: str, reunion_slug: str, course_file: str) -> str:
    """
    Reconstruit l'URL de course à partir du nom de fichier et du slug de réunion.
    Exemple: date=2006-04-16, reunion_slug=R1-auteuil, file=R1C2-prix-du-president.html
    → https://www.zeturf.fr/fr/course/2006-04-16/R1C2-auteuil-prix-du-president
    """
    hippodrome = reunion_slug.split("-", 1)[1] if "-" in reunion_slug else reunion_slug

    course_slug = course_file.replace(".html", "")
    idx = course_slug.find("-")
    if idx == -1:
        code_part = course_slug
        title_part = ""
    else:
        code_part = course_slug[:idx]
        title_part = course_slug[idx + 1:]

    return f"{BASE}/fr/course/{date_str}/{code_part}-{hippodrome}-{title_part}"


def scrape_course(date_str: str, reunion_slug: str, course_filename: str):
    """Re-scrape une course isolée (sans repasser par la page réunion)."""
    date_dir = get_date_directory(date_str)
    reunion_dir = date_dir / reunion_slug
    course_path = reunion_dir / course_filename

    if course_path.exists() and course_path.stat().st_size > 128:
        return

    url = build_course_url(date_str, reunion_slug, course_filename)
    print(f"\n[SCRAPE COURSE ISOLEE] {date_str} {reunion_slug}/{course_filename}")
    print(f"  URL: {url}")

    try:
        html = safe_get(url, referer=url)
    except Exception as e:
        print(f"  [ERREUR] Course {course_filename}: {e}")
        return

    save_text(course_path, html)


def rescrape_from_missing(year: int, missing_path: Path):
    """Lit verify_courses/<year>/missing_<year>.txt et re-scrape ce qui manque."""
    if not missing_path.exists():
        print(f"\n[AUCUN MISSING] {year}: pas de fichier {missing_path}")
        return

    missing_dates = set()
    missing_reunions = defaultdict(set)      # {date_str} -> {reunion_slug}
    missing_courses = []                     # [(date_str, reunion_slug, filename)]

    with missing_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(";")
            if parts[0] == "MISSING_DATE" and len(parts) >= 2:
                missing_dates.add(parts[1])
            elif parts[0] == "MISSING_REUNION" and len(parts) >= 3:
                date_str, reunion_slug = parts[1], parts[2]
                # si la date est déjà à rescaper entièrement, pas la peine de stocker la réunion
                if date_str not in missing_dates:
                    missing_reunions[date_str].add(reunion_slug)
            elif parts[0] == "MISSING_COURSE" and len(parts) >= 4:
                date_str, reunion_slug, filename = parts[1], parts[2], parts[3]
                missing_courses.append((date_str, reunion_slug, filename))

    total_dates = len(missing_dates)
    total_reunions = sum(len(s) for s in missing_reunions.values())
    total_courses = len(missing_courses)

    print(f"\n{'='*80}")
    print(f"RE-SCRAPE ANNÉE {year}")
    print(f"{'='*80}")
    print(f"Dates à rescaper complètement : {total_dates}")
    print(f"Réunions isolées à rescaper :  {total_reunions}")
    print(f"Courses isolées à rescaper :   {total_courses}")

    # 1) Dates complètes
    for date_str in sorted(missing_dates):
        scrape_full_date(date_str)

    # 2) Réunions isolées (seulement si la date n'est pas dans missing_dates)
    for date_str in sorted(missing_reunions.keys()):
        if date_str in missing_dates:
            continue
        for reunion_slug in sorted(missing_reunions[date_str]):
            scrape_reunion(date_str, reunion_slug)

    # 3) Courses isolées
    for date_str, reunion_slug, filename in missing_courses:
        if date_str in missing_dates:
            continue
        if reunion_slug in missing_reunions.get(date_str, set()):
            # la réunion entière a été re-scrapée, la course devrait exister
            course_path = get_date_directory(date_str) / reunion_slug / filename
            if course_path.exists() and course_path.stat().st_size > 128:
                continue
        course_path = get_date_directory(date_str) / reunion_slug / filename
        if course_path.exists() and course_path.stat().st_size > 128:
            continue
        scrape_course(date_str, reunion_slug, filename)


# =========================
# Git commit / push par année
# =========================

def git_commit_year(year: int, stats_before: dict, stats_after: dict):
    """Commit + push des changements pour une année (données + verify_courses)."""
    year_str = str(year)
    data_dir = REPO_ROOT / year_str
    verify_dir = VERIFY_ROOT / year_str

    paths_to_add = []
    if data_dir.exists():
        paths_to_add.append(str(data_dir))
    if verify_dir.exists():
        paths_to_add.append(str(verify_dir))

    if not paths_to_add:
        print(f"\n[Git] Rien à ajouter pour {year} (aucun dossier pour cette année).")
        return

    # Config git
    subprocess.run(["git", "config", "user.name", "GitHub Actions Bot"], check=False)
    subprocess.run(["git", "config", "user.email", "actions@github.com"], check=False)

    # Stage
    subprocess.run(["git", "add"] + paths_to_add, check=True)

    # Vérifier s'il y a réellement des fichiers modifiés
    res = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=True,
    )
    files = [line for line in res.stdout.splitlines() if line.strip()]
    if not files:
        print(f"\n[Git] Aucun changement à commit pour l'année {year}.")
        subprocess.run(["git", "reset"], check=False)
        return

    before_missing = stats_before["missing_dates"] + stats_before["missing_reunions"] + stats_before["missing_courses"]
    after_missing = stats_after["missing_dates"] + stats_after["missing_reunions"] + stats_after["missing_courses"]

    msg = f"Verify/rescrape {year_str}: {before_missing} manquants -> {after_missing} restants"

    # Branche courante
    branch_res = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    branch = branch_res.stdout.strip() or "main"

    # Commit
    print(f"\n[Git] Commit année {year} ({len(files)} fichiers modifiés)")
    subprocess.run(["git", "commit", "-m", msg], check=True)

    # Push avec retry + pull --rebase en cas de non-fast-forward
    for attempt in range(1, 4):
        try:
            print(f"[Git] Push (tentative {attempt}/3) sur {branch}...")
            subprocess.run(["git", "push", "origin", branch], check=True)
            print("[Git] Push OK.")
            return
        except subprocess.CalledProcessError as e:
            print(f"[Git] Push échoué (tentative {attempt}): {e}")
            if attempt == 3:
                print("[Git] Abandon après 3 tentatives de push.")
                # On laisse le workflow en échec pour signaler le problème
                raise
            print("[Git] Pull --rebase avant nouveau push...")
            subprocess.run(["git", "pull", "--rebase", "origin", branch], check=True)


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(
        description="Vérification + re-scraping ZEturf (dates / réunions FR / courses)."
    )
    parser.add_argument(
        "--years",
        type=str,
        help='Liste d\'années séparées par des espaces, ex: "2017 2018 2019"',
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Une seule année à traiter",
    )
    args = parser.parse_args()

    years = []
    if args.years:
        years.extend(int(x) for x in args.years.split() if x.strip())
    if args.year is not None:
        years.append(args.year)

    if not years:
        raise SystemExit("Aucune année fournie. Utiliser --year 2017 ou --years \"2017 2018\".")

    years = sorted(set(years))

    print("=" * 80)
    print("VERIFY + RESCRAPE ZETURF")
    print("=" * 80)
    print(f"Années à traiter: {', '.join(str(y) for y in years)}")
    print(f"Période globale: {GLOBAL_START_DATE.isoformat()} → {GLOBAL_END_DATE.isoformat()}")
    print("=" * 80)

    for year in years:
        r = compute_year_range(year)
        if r is None:
            print(f"\n[SKIP] Année {year}: hors plage globale, rien à faire.")
            continue
        start_str, end_str = r
        missing_path = VERIFY_ROOT / str(year) / f"missing_{year}.txt"

        # 1) Vérification initiale
        stats_before = collect_missing_for_year(year, missing_path, start_str, end_str)
        missing_total_before = (
            stats_before["missing_dates"]
            + stats_before["missing_reunions"]
            + stats_before["missing_courses"]
        )

        if missing_total_before == 0:
            print(f"\n[OK] Année {year}: aucune donnée manquante, pas de re-scraping ni de commit.")
            continue

        # 2) Re-scrape des éléments manquants
        rescrape_from_missing(year, missing_path)

        # 3) Nouvelle vérification après scraping (écrase missing_<year>.txt par l'état à jour)
        stats_after = collect_missing_for_year(year, missing_path, start_str, end_str)

        # 4) Commit & push pour cette année
        git_commit_year(year, stats_before, stats_after)

    print("\n=== TERMINÉ ===")


if __name__ == "__main__":
    main()
