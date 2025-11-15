#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verify_and_repair.py

Pipeline complet pour une période:
  1) Vérifie toutes les dates via verify.verify_date
  2) Re-scrape toutes les dates problématiques (dates, réunions FR, courses)
  3) Re-vérifie après réparation
  4) Génère un rapport de vérification détaillé sur la période

Sortie:
  - Fichier texte: verification_report.txt (ou autre, via --report-file)
  - Code de retour:
      0 = tout OK (aucune date manquante/incomplète/erreur)
      1 = il reste des problèmes après les passes de réparation
"""

import argparse
import time
import re
from pathlib import Path
from typing import Dict, List, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# On réutilise les constantes et helpers de ton verify.py
from verify import (
    REPO_ROOT,
    BASE,
    slugify,
    get_date_directory,
    date_range,
    verify_date,
)

# =========================
# HTTP helpers (scraping)
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

DATE_URL_TPL = f"{BASE}/fr/resultats-et-rapports/{{date}}"


def safe_get(url: str, referer: str | None = None, retries: int = 4, timeout: int = 30) -> str:
    """
    GET avec:
      - headers corrects
      - gestion des erreurs réseau
      - retries exponentiels
      - petit sleep pour ne pas brutaliser le site
    """
    for i in range(retries):
        try:
            headers = {}
            if referer:
                headers["Referer"] = referer
            resp = SESSION.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            # Petit délai pour limiter le risque de 429 sur ce script qui reste séquentiel
            time.sleep(0.25)
            return resp.text
        except Exception:
            if i == retries - 1:
                raise
            # backoff progressif
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


# =========================
# Parsing ZEturf (date, réunions FR, courses)
# =========================

def parse_reunions_fr_for_date(date_str: str) -> List[Dict]:
    """
    Récupère toutes les réunions FR pour une date donnée.

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

    out = []
    # même logique globale que ton ancien script + verify.py
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
        reunion_code = None
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

    Retourne une liste de dict:
      {
        "numero": int | None,
        "code": "C1" / "C2" / etc,
        "heure": "13h55" ou None,
        "intitule": "Prix de xxx" ou None,
        "url": "https://www.zeturf.fr/..."
      }
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
            numero = int(numero_txt)
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

        # Code course (même logique que verify_reunion_courses)
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

    # Optionnel: trier par numéro si dispo
    if courses and all(c["numero"] is not None for c in courses):
        courses.sort(key=lambda c: c["numero"])

    return courses


# =========================
# Re-scrape d'une date complète
# =========================

def scrape_full_date(date_str: str) -> None:
    """
    Re-scrape une date complète:
      - page de date
      - toutes les réunions FR
      - toutes les courses

    Respecte l'arborescence attendue par verify.py:
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

    stats = {
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
    Génère un rapport texte dans le même esprit que verify.py,
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

    last_stats: Dict | None = None
    last_results: List[Dict] | None = None

    for pass_idx in range(1, max_passes + 1):
        print(f"\n===== PASS {pass_idx}/{max_passes} : VÉRIFICATION =====\n")
        stats, results = verify_period(start_date, end_date)
        last_stats, last_results = stats, results

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


# =========================
# Entrée CLI
# =========================

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
