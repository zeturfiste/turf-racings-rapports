#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script unique : vérification + réparation des données ZEturf.

Fonctions :
- Vérifie les dates d'une période (structure des dossiers + fichiers HTML)
- Identifie les dates MISSING / INCOMPLETE / ERROR
- Re-scrape :
    * la page de date
    * toutes les réunions FR
    * toutes les courses de ces réunions
- Refait une vérification finale
- Génère un rapport texte (par période, typiquement par année)

Usage (exemple) :
  python verify_and_repair.py --start-date 2017-01-01 --end-date 2017-12-31 \
      --max-passes 2 --report-file verification_report_2017.txt
"""

import argparse
import re
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# =========================
# Config
# =========================

REPO_ROOT = "resultats-et-rapports"
BASE = "https://www.zeturf.fr"
DATE_URL_TPL = BASE + "/fr/resultats-et-rapports/{date}"

# =========================
# Helpers
# =========================

def slugify(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text


def get_date_directory(date_str: str) -> Path:
    """
    Returns path like: resultats-et-rapports/2025/11/2025-11-10/
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str


def date_range(start_date: str, end_date: str) -> List[str]:
    """
    Generate all dates in [start_date, end_date] inclusive.
    """
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_date, "%Y-%m-%d").date()
    if d0 > d1:
        d0, d1 = d1, d0
    days = (d1 - d0).days
    return [(d0 + timedelta(days=i)).isoformat() for i in range(days + 1)]


# =========================
# Vérification (lecture locale)
# =========================

def verify_reunion_courses(reunion_file: Path, reunion_dir: Path, reunion_code: str) -> List[str]:
    """
    Vérifie que toutes les courses d'une réunion ont leur fichier HTML.
    Retourne une liste de messages d'issues (chaîne vide si tout est OK).
    """
    issues: List[str] = []

    try:
        html = reunion_file.read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")
        frise = soup.select_one("#frise-course .strip2.active") or soup.select_one("#frise-course .strip2")

        if not frise:
            issues.append(f"⚠️  {reunion_dir.name}/: Aucune frise de courses trouvée")
            return issues

        expected_courses = []
        for a in frise.select("ul.scroll-content li.scroll-element a"):
            href = a.get("href", "")
            if not href:
                continue

            # Extract course number
            numero_txt_el = a.select_one("span.numero")
            numero_txt = numero_txt_el.get_text(strip=True) if numero_txt_el else ""
            mC = re.search(r"C(\d+)", href)
            numero = int(numero_txt) if numero_txt.isdigit() else (int(mC.group(1)) if mC else None)

            # Extract title
            title = a.get("title", "").strip()
            if " - " in title:
                _, intitule = title.split(" - ", 1)
            else:
                intitule = title or None

            code = f"C{numero}" if numero is not None else (mC.group(0) if mC else None)

            # Build expected filename
            slug = slugify(intitule) if intitule else ""
            if not slug:
                slug = "course"
            code_part = f"{reunion_code}{(code or '').upper()}"
            filename = f"{code_part}-{slug}.html"

            expected_courses.append({
                "filename": filename,
                "numero": numero,
                "code": code,
            })

        # Verify each course file exists
        for course in expected_courses:
            course_file = reunion_dir / course["filename"]
            if not course_file.exists() or course_file.stat().st_size == 0:
                issues.append(f"❌ {reunion_dir.name}/{course['filename']}")

    except Exception as e:
        issues.append(f"❌ {reunion_dir.name}/: Erreur analyse courses - {e}")

    return issues


def verify_date(date_str: str) -> Dict:
    """
    Vérifie une date complète:
      1. Dossier existe
      2. Fichier HTML de la date existe
      3. Toutes les réunions FR ont leur dossier + fichier
      4. Toutes les courses de chaque réunion ont leur fichier

    Returns: dict with keys:
      - date: str
      - status: "OK" / "MISSING" / "INCOMPLETE" / "WARNING" / "ERROR"
      - issues: List[str]
    """
    result = {
        "date": date_str,
        "status": "OK",
        "issues": [],
    }

    date_dir = get_date_directory(date_str)
    date_file = date_dir / f"{date_str}.html"

    # Check 1: Date directory exists
    if not date_dir.exists():
        result["status"] = "MISSING"
        result["issues"].append(f"❌ Dossier absent: {date_dir}")
        return result

    # Check 2: Date HTML file exists
    if not date_file.exists() or date_file.stat().st_size == 0:
        result["status"] = "INCOMPLETE"
        result["issues"].append(f"❌ Fichier date absent ou vide: {date_file}")
        return result

    # Parse date HTML to get expected reunions FR
    try:
        html = date_file.read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")
        container = soup.select_one("div#list-reunion")

        if not container:
            result["status"] = "WARNING"
            result["issues"].append(
                f"⚠️  Aucun conteneur #list-reunion trouvé dans {date_file.name}"
            )
            return result

        expected_reunions = []
        for tr in container.select("table.programme tbody tr.item"):
            a = tr.select_one('td.numero a[data-tc-pays="FR"]')
            if not a:
                continue

            href = a.get("href", "").strip()
            if not href:
                continue

            # Extract reunion code
            m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
            reunion_code = m.group(1) if m else (a.get_text(strip=True).replace("FR", "R"))

            # Extract hippodrome
            hippo_el = tr.select_one("td.nom h2 span span")
            hippodrome = hippo_el.get_text(strip=True) if hippo_el else ""
            reunion_slug = f"{reunion_code}-{slugify(hippodrome)}"

            expected_reunions.append(
                {
                    "code": reunion_code,
                    "slug": reunion_slug,
                    "hippodrome": hippodrome,
                    "href": href,
                }
            )

        # Check 3: Verify each reunion
        for reunion in expected_reunions:
            reunion_dir = date_dir / reunion["slug"]
            reunion_file = reunion_dir / f"{reunion['slug']}.html"

            if not reunion_dir.exists():
                result["status"] = "INCOMPLETE"
                result["issues"].append(f"❌ Dossier réunion absent: {reunion['slug']}/")
                continue

            if not reunion_file.exists() or reunion_file.stat().st_size == 0:
                result["status"] = "INCOMPLETE"
                result["issues"].append(
                    f"❌ Fichier réunion absent: {reunion['slug']}/{reunion_file.name}"
                )
                continue

            # Check 4: Verify courses for this reunion
            reunion_issues = verify_reunion_courses(
                reunion_file, reunion_dir, reunion["code"]
            )
            if reunion_issues:
                result["status"] = "INCOMPLETE"
                result["issues"].extend(reunion_issues)

        if not expected_reunions:
            result["status"] = "WARNING"
            result["issues"].append(f"⚠️  Aucune réunion FR trouvée pour {date_str}")

    except Exception as e:
        result["status"] = "ERROR"
        result["issues"].append(f"❌ Erreur lors de l'analyse: {e}")

    return result


# =========================
# HTTP + scraping helpers
# =========================

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
        ),
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
)


def safe_get(
    url: str, referer: Optional[str] = None, retries: int = 4, timeout: int = 30
) -> str:
    """
    GET robuste :
      - headers corrects
      - gestion des erreurs réseau
      - retries exponentiels
      - léger sleep entre les tentatives
    """
    for i in range(retries):
        try:
            headers = {}
            if referer:
                headers["Referer"] = referer
            resp = SESSION.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            time.sleep(0.25)
            return resp.text
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.5 * (i + 1))


def save_text(path: Path, text: str) -> None:
    """
    Sauvegarde un fichier texte en UTF-8.
    Si le fichier existe déjà et est "non trivial" (>128 bytes), on ne le réécrit pas.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 128:
        return
    path.write_text(text, encoding="utf-8")


def parse_reunions_fr_for_date(date_str: str) -> List[Dict]:
    """
    Récupère toutes les réunions FR pour une date donnée depuis le site ZEturf.

    Retourne une liste de dict:
      {
        "reunion_code": "R1",
        "hippodrome": "Pau",
        "url": "https://www.zeturf.fr/fr/reunion/....",
        "date": "YYYY-MM-DD"
      }
    """
    url = DATE_URL_TPL.format(date=date_str)
    html = safe_get(url)
    soup = BeautifulSoup(html, "lxml")

    root = soup.select_one("div#list-reunion")
    if not root:
        return []

    out: List[Dict] = []

    for tr in root.select("table.programme tbody tr.item"):
        a = tr.select_one('a[href^="/fr/reunion/"][data-tc-pays="FR"]')
        if not a:
            continue

        href = a.get("href", "")
        if not href:
            continue

        reunion_url = urljoin(BASE, href)

        # Code de réunion R1, R2, ...
        numero_td = tr.select_one("td.numero")
        reunion_code: Optional[str] = None
        if numero_td:
            txt = numero_td.get_text(strip=True)
            reunion_code = txt.replace("FR", "R").strip() or None

        if not reunion_code:
            m = re.search(r"/fr/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
            if m:
                reunion_code = m.group(1)

        # Hippodrome
        hippo_node = (
            tr.select_one("td.nom h2 span span")
            or tr.select_one("td.nom h2 span, td.nom h2")
        )
        hippodrome = hippo_node.get_text(strip=True) if hippo_node else ""

        if not hippodrome:
            title = a.get("title", "")
            if title:
                hippodrome = title.strip().upper()
            else:
                m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/R\d+-([a-z0-9-]+)", href)
                if m:
                    hippodrome = m.group(1).replace("-", " ").upper()

        if reunion_code:
            out.append(
                {
                    "reunion_code": reunion_code,
                    "hippodrome": hippodrome,
                    "url": reunion_url,
                    "date": date_str,
                }
            )

    return out


def parse_courses_from_reunion_page(reunion_url: str) -> List[Dict]:
    """
    Parse une page de réunion et renvoie la liste des courses avec leurs URLs.
    """
    html = safe_get(reunion_url, referer=reunion_url)
    soup = BeautifulSoup(html, "lxml")

    frise = soup.select_one("#frise-course .strip2.active") or soup.select_one(
        "#frise-course .strip2"
    )

    courses: List[Dict] = []
    if not frise:
        return courses

    for a in frise.select("ul.scroll-content li.scroll-element a[href]"):
        href = a.get("href", "")
        if not href:
            continue

        course_url = urljoin(BASE, href)

        # Numéro de course
        numero_txt = ""
        num_sp = a.select_one("span.numero")
        if num_sp:
            numero_txt = num_sp.get_text(strip=True)

        mC = re.search(r"C(\d+)", href)
        if numero_txt.isdigit():
            numero: Optional[int] = int(numero_txt)
        elif mC:
            numero = int(mC.group(1))
        else:
            numero = None

        # Intitulé / heure
        title = a.get("title", "").strip()
        if " - " in title:
            heure, intitule = title.split(" - ", 1)
        else:
            heure, intitule = None, title or None

        # Code course
        if numero is not None:
            code = f"C{numero}"
        elif mC:
            code = mC.group(0)  # "C2"
        else:
            code = None

        courses.append(
            {
                "numero": numero,
                "code": code,
                "heure": heure,
                "intitule": intitule,
                "url": course_url,
            }
        )

    # Trier si on a tous les numéros
    if courses and all(c["numero"] is not None for c in courses):
        courses.sort(key=lambda c: c["numero"])

    return courses


def scrape_full_date(date_str: str) -> None:
    """
    Re-scrape une date complète:
      - page de date
      - toutes les réunions FR
      - toutes les courses

    Respecte l'arborescence attendue:
      resultats-et-rapports/YYYY/MM/YYYY-MM-DD/Rn-hippodrome/...
    """
    date_dir = get_date_directory(date_str)
    date_file = date_dir / f"{date_str}.html"

    print(f"\n=== RE-SCRAPE DATE {date_str} → {date_dir} ===")

    # 1) Page de date
    date_url = DATE_URL_TPL.format(date=date_str)
    try:
        date_html = safe_get(date_url)
        save_text(date_file, date_html)
        print(f"  ✓ Page date {date_file}")
    except Exception as e:
        print(f"  ❌ [ERREUR] Page date {date_str}: {e}")
        return

    # 2) Réunions FR
    try:
        reunions = parse_reunions_fr_for_date(date_str)
    except Exception as e:
        print(f"  ❌ [ERREUR] parse_reunions_fr_for_date({date_str}): {e}")
        return

    if not reunions:
        print("  ℹ️  Aucune réunion FR détectée pour cette date")
        return

    # 3) Réunions -> courses
    for r in reunions:
        reunion_code = r["reunion_code"]
        hippo = r["hippodrome"] or ""
        reunion_slug = f"{reunion_code}-{slugify(hippo)}"
        reunion_dir = date_dir / reunion_slug
        reunion_file = reunion_dir / f"{reunion_slug}.html"

        print(f"  - Réunion {reunion_slug}")

        # Page réunion
        try:
            reunion_html = safe_get(r["url"], referer=date_url)
            save_text(reunion_file, reunion_html)
            print(f"    ✓ Page réunion {reunion_file}")
        except Exception as e:
            print(f"    ❌ [ERREUR] Récup réunion {reunion_slug}: {e}")
            continue

        # Courses de la réunion
        try:
            courses = parse_courses_from_reunion_page(r["url"])
        except Exception as e:
            print(f"    ❌ [ERREUR] parse_courses_from_reunion_page: {e}")
            continue

        if not courses:
            print("    ℹ️  Aucune course visible dans la frise")
            continue

        for c in courses:
            intitule = c["intitule"]
            slug = slugify(intitule) if intitule else ""
            if not slug:
                slug = "course"

            code = c["code"]  # "C1", "C2", ...
            code_part = f"{reunion_code}{(code or '').upper()}"
            filename = f"{code_part}-{slug}.html"
            course_file = reunion_dir / filename

            try:
                course_html = safe_get(c["url"], referer=r["url"])
                save_text(course_file, course_html)
                print(f"    ✓ {filename}")
            except Exception as e:
                print(f"    ⚠️  [WARN] Échec course {filename}: {e}")


# =========================
# Vérification sur intervalle
# =========================

def verify_period(start_date: str, end_date: str) -> Tuple[Dict, List[Dict]]:
    """
    Vérifie toutes les dates entre start_date et end_date (inclus).

    Retourne:
      - stats: dict des compteurs
      - results: liste de dicts {date, status, issues}
    """
    all_dates = date_range(start_date, end_date)
    total_dates = len(all_dates)

    stats: Dict[str, int] = {
        "total": total_dates,
        "ok": 0,
        "missing": 0,
        "incomplete": 0,
        "warning": 0,
        "error": 0,
    }

    results: List[Dict] = []

    print("=" * 80)
    print(f"VÉRIFICATION {start_date} → {end_date}")
    print("=" * 80)

    for i, date_str in enumerate(all_dates, 1):
        print(f"[{i}/{total_dates}] Vérification {date_str}...", end=" ", flush=True)
        result = verify_date(date_str)
        results.append(result)

        status = result["status"]
        if status == "OK":
            stats["ok"] += 1
            print("✓ OK")
        elif status == "MISSING":
            stats["missing"] += 1
            print("✗ MISSING")
        elif status == "INCOMPLETE":
            stats["incomplete"] += 1
            print("✗ INCOMPLETE")
        elif status == "WARNING":
            stats["warning"] += 1
            print("⚠️ WARNING")
        elif status == "ERROR":
            stats["error"] += 1
            print("✗ ERROR")
        else:
            print(status)

    return stats, results


def write_report(
    start_date: str,
    end_date: str,
    report_file: str,
    stats: Dict,
    results: List[Dict],
) -> None:
    """
    Génère un rapport texte dans l'esprit de l'ancien verify.py,
    mais limité à l'intervalle [start_date, end_date].
    """
    path = Path(report_file)
    with path.open("w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("RAPPORT DE VÉRIFICATION ZETURF\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Date de vérification: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Période analysée: {start_date} → {end_date}\n\n")

        f.write("STATISTIQUES\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total de dates:        {stats['total']}\n")
        if stats["total"] > 0:
            pct_ok = stats["ok"] / stats["total"] * 100.0
        else:
            pct_ok = 0.0
        f.write(f"✓ Complètes:           {stats['ok']} ({pct_ok:.1f}%)\n")
        f.write(f"❌ Absentes:            {stats['missing']}\n")
        f.write(f"⚠️  Incomplètes:         {stats['incomplete']}\n")
        f.write(f"⚠️  Warnings:            {stats['warning']}\n")
        f.write(f"❌ Erreurs:             {stats['error']}\n\n")

        problematic = [
            r for r in results if r["status"] in ("MISSING", "INCOMPLETE", "WARNING", "ERROR")
        ]

        if problematic:
            f.write("\n" + "=" * 80 + "\n")
            f.write("DATES INCOMPLÈTES OU PROBLÉMATIQUES\n")
            f.write("=" * 80 + "\n\n")

            for r in problematic:
                f.write("\n" + "=" * 80 + "\n")
                f.write(f"DATE: {r['date']} - STATUS: {r['status']}\n")
                f.write("=" * 80 + "\n")
                for issue in r["issues"]:
                    f.write(f"  {issue}\n")
        else:
            f.write("\n🎉 Toutes les dates sont complètes sur cette période !\n")

    print(f"\n📄 Rapport détaillé écrit dans: {path}")


# =========================
# Boucle verify + repair
# =========================

def verify_and_repair_period(
    start_date: str,
    end_date: str,
    max_passes: int = 2,
    report_file: str = "verification_report.txt",
) -> int:
    """
    Boucle:
      - Vérifie toutes les dates
      - Re-scrape les dates MISSING / INCOMPLETE / ERROR
      - Re-vérifie
      - Génère un rapport final

    max_passes = nombre maximum de passes "vérif + réparation" avant de s'arrêter.
    """
    print("=" * 80)
    print("VERIFY + REPAIR ZETURF")
    print("=" * 80)
    print(f"Période: {start_date} → {end_date}")
    print(f"Max passes de réparation: {max_passes}\n")

    for pass_idx in range(1, max_passes + 1):
        print(f"\n===== PASS {pass_idx}/{max_passes} : VÉRIFICATION =====\n")
        stats, results = verify_period(start_date, end_date)

        # Dates à réparer: MISSING / INCOMPLETE / ERROR
        bad_dates = [
            r["date"]
            for r in results
            if r["status"] in ("MISSING", "INCOMPLETE", "ERROR")
        ]

        if not bad_dates:
            print("\n🎉 Plus aucune date problématique, rien à réparer.")
            break

        if pass_idx == max_passes:
            print(
                f"\n❌ Il reste {len(bad_dates)} dates problématiques après la dernière passe."
            )
            break

        print(f"\n⚠️  {len(bad_dates)} dates à re-scraper pour cette passe:\n")
        for d in bad_dates:
            print(f"  → Re-scrape {d}")
            scrape_full_date(d)

    # Vérif finale + rapport
    print("\n===== VÉRIFICATION FINALE POUR LE RAPPORT =====\n")
    final_stats, final_results = verify_period(start_date, end_date)
    write_report(start_date, end_date, report_file, final_stats, final_results)

    if (
        final_stats["missing"] > 0
        or final_stats["incomplete"] > 0
        or final_stats["error"] > 0
    ):
        print(
            "\n❌ Des problèmes subsistent après réparation. "
            "Voir le rapport pour le détail."
        )
        return 1
    else:
        print("\n✓ Vérification complète avec succès après réparation.")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify + re-scrape ZEturf data sur une période."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Date de début (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="Date de fin (YYYY-MM-DD, inclus)",
    )
    parser.add_argument(
        "--max-passes",
        type=int,
        default=2,
        help="Nombre max de passes verify+repair (défaut: 2)",
    )
    parser.add_argument(
        "--report-file",
        default="verification_report.txt",
        help="Nom du fichier rapport (défaut: verification_report.txt)",
    )

    args = parser.parse_args()

    return verify_and_repair_period(
        start_date=args.start_date,
        end_date=args.end_date,
        max_passes=args.max_passes,
        report_file=args.report_file,
    )


if __name__ == "__main__":
    raise SystemExit(main())
