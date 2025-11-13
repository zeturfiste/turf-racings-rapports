# check_inventory.py
# -*- coding: utf-8 -*-
import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path
from datetime import datetime, timedelta

from bs4 import BeautifulSoup

# -------------------------------------------------------------------
# Config (aligné avec scraper.py)
# -------------------------------------------------------------------
REPO_ROOT = Path("resultats-et-rapports")

# Période complète du scraper (tu peux les modifier si tu changes ton scraper)
DEFAULT_START_DATE = "2005-04-27"
DEFAULT_END_DATE = "2025-11-11"

DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
REUNION_DIR_RE = re.compile(r"^R\d+-[a-z0-9-]+$")
COURSE_FILE_RE = re.compile(r"^R\d+C\d+-[a-z0-9-]+\.html$")


# -------------------------------------------------------------------
# Helpers génériques
# -------------------------------------------------------------------
def slugify(text: str) -> str:
    """Même logique que dans scraper.py."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text


def read_html(p: Path) -> str | None:
    """Lis un fichier HTML si présent et non vide, sinon renvoie None."""
    try:
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        pass
    return None


def date_range_asc(start_date: str, end_date: str) -> list[str]:
    """Génère toutes les dates ISO de start_date à end_date (inclus)."""
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_date, "%Y-%m-%d").date()
    if d0 > d1:
        d0, d1 = d1, d0
    days = (d1 - d0).days
    return [(d0 + timedelta(days=i)).isoformat() for i in range(days + 1)]


def get_date_directory(date_str: str, root: Path) -> Path:
    """Construit le chemin du dossier date (comme dans scraper.py)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return root / year / month / date_str


# -------------------------------------------------------------------
# Parseurs HTML alignés sur scraper.py
# -------------------------------------------------------------------
def parse_reunions_fr_from_date_html(html: str) -> list[dict]:
    """
    Renvoie une liste de dicts {slug, reunion_code, hippodrome} pour
    toutes les réunions FR trouvées dans la page de date.

    Logique calquée sur scrape_date() du scraper.
    """
    soup = BeautifulSoup(html, "lxml")
    container = soup.select_one("div#list-reunion")
    if not container:
        return []

    reunions = []
    for tr in container.select("table.programme tbody tr.item"):
        # Lien FR dans la colonne "numero"
        a = tr.select_one('td.numero a[data-tc-pays="FR"]')
        if not a:
            continue

        href = (a.get("href") or "").strip()
        if not href:
            continue

        # Code reunion (R1, R2, ...)
        m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
        reunion_code = m.group(1) if m else (a.get_text(strip=True).replace("FR", "R"))

        # Hippodrome
        hippo_el = tr.select_one("td.nom h2 span span")
        hippodrome = hippo_el.get_text(strip=True) if hippo_el else ""

        reunion_slug = f"{reunion_code}-{slugify(hippodrome)}"

        reunions.append(
            {
                "slug": reunion_slug,
                "reunion_code": reunion_code,
                "hippodrome": hippodrome,
            }
        )
    return reunions


def parse_course_filenames_from_reunion_html(html: str, reunion_code: str) -> list[str]:
    """
    Renvoie la liste des noms de fichiers attendus pour les courses
    d'une réunion, par ex: 'R1C3-prix-de-paris.html'.

    Logique calquée sur scrape_reunion() du scraper.
    """
    soup = BeautifulSoup(html, "lxml")
    frise = soup.select_one("#frise-course .strip2.active") or soup.select_one("#frise-course .strip2")
    if not frise:
        return []

    filenames: list[str] = []
    for a in frise.select("ul.scroll-content li.scroll-element a"):
        href = a.get("href", "")
        if not href:
            continue

        numero_txt_el = a.select_one("span.numero")
        numero_txt = numero_txt_el.get_text(strip=True) if numero_txt_el else ""
        mC = re.search(r"C(\d+)", href)

        if numero_txt.isdigit():
            numero = int(numero_txt)
        elif mC:
            numero = int(mC.group(1))
        else:
            numero = None

        code = f"C{numero}" if numero is not None else (mC.group(0) if mC else None)
        if not code:
            continue

        title = a.get("title", "").strip()
        intitule = title.split(" - ", 1)[1] if " - " in title else (title or "")
        slug = slugify(intitule) or "course"

        filename = f"{reunion_code}{code.upper()}-{slug}.html"
        filenames.append(filename)

    return filenames


# -------------------------------------------------------------------
# Main check
# -------------------------------------------------------------------
def run_check(
    repo_root: Path,
    out_root: Path,
    start_date: str,
    end_date: str,
) -> int:
    out_root.mkdir(parents=True, exist_ok=True)

    all_dates = date_range_asc(start_date, end_date)

    problems_total = 0
    global_summary: list[tuple[str, bool]] = []          # (date, any_missing)
    per_year_summary: dict[str, list[tuple[str, bool]]] = {}

    for date_str in all_dates:
        year_str = date_str[:4]
        date_dir = get_date_directory(date_str, repo_root)
        date_html_path = date_dir / f"{date_str}.html"
        year_outdir = out_root / year_str
        year_outdir.mkdir(parents=True, exist_ok=True)

        date_dir_exists = date_dir.exists() and date_dir.is_dir()
        date_html = read_html(date_html_path) if date_dir_exists else None

        report = {
            "date": date_str,
            "paths": {
                "date_dir": str(date_dir),
                "date_html": str(date_html_path),
            },
            "expected": {
                "reunions_fr": [],
                "courses_by_reunion": {},  # reunion_slug -> [filenames]
            },
            "missing": {
                "date_dir": False,
                "date_html": False,
                "reunion_dirs": [],  # liste de chemins
                "reunion_html": [],
                "course_files": [],
            },
            "checked_at": datetime.utcnow().isoformat() + "Z",
        }

        # --- Dossier date ---
        if not date_dir_exists:
            report["missing"]["date_dir"] = True
            report["missing"]["date_html"] = True  # forcément manquant
            problems_total += 1
            any_missing = True
        else:
            # --- Fichier HTML de la date ---
            if not date_html:
                report["missing"]["date_html"] = True
                problems_total += 1

            expected_reunions = []
            if date_html:
                # Réunions FR attendues pour cette date
                reunions = parse_reunions_fr_from_date_html(date_html)
                expected_reunions = [r["slug"] for r in reunions]
                report["expected"]["reunions_fr"] = expected_reunions

            # --- Vérification de chaque réunion attendue ---
            for reunion_slug in expected_reunions:
                reunion_dir = date_dir / reunion_slug
                if not (reunion_dir.exists() and reunion_dir.is_dir()):
                    report["missing"]["reunion_dirs"].append(str(reunion_dir))
                    problems_total += 1
                    continue

                reunion_html_path = reunion_dir / f"{reunion_slug}.html"
                reunion_html = read_html(reunion_html_path)
                if not reunion_html:
                    report["missing"]["reunion_html"].append(str(reunion_html_path))
                    problems_total += 1
                    continue

                # Déterminer le code Rn (R1, R2, etc.) à partir du slug
                mR = re.match(r"^(R\d+)-", reunion_slug)
                reunion_code = mR.group(1) if mR else None
                if not reunion_code:
                    # Pas de code Rn -> on ne cherche pas les courses
                    continue

                expected_course_files = parse_course_filenames_from_reunion_html(
                    reunion_html, reunion_code
                )
                report["expected"]["courses_by_reunion"][reunion_slug] = expected_course_files

                for fname in expected_course_files:
                    cpath = reunion_dir / fname
                    if not (cpath.exists() and cpath.is_file() and cpath.stat().st_size > 0):
                        report["missing"]["course_files"].append(str(cpath))
                        problems_total += 1

            miss = report["missing"]
            any_missing = (
                miss["date_dir"]
                or miss["date_html"]
                or bool(miss["reunion_dirs"])
                or bool(miss["reunion_html"])
                or bool(miss["course_files"])
            )

        # Sauvegarde JSON par date dans missing/<année>/YYYY-MM-DD.json
        out_json = year_outdir / f"{date_str}.json"
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        # Accumuler pour les summary
        global_summary.append((date_str, any_missing))
        per_year_summary.setdefault(year_str, []).append((date_str, any_missing))

    # ------------------------------------------------------------------
    # Résumé global
    # ------------------------------------------------------------------
    lines = [f"# Contrôle inventaire — {datetime.utcnow().isoformat()}Z\n"]
    if not global_summary:
        lines.append("Aucune date dans l'intervalle.")
    else:
        lines.append("| Date | Manquants |")
        lines.append("|------|-----------|")
        for date_str, any_missing in sorted(global_summary):
            lines.append(f"| {date_str} | {'❌' if any_missing else '✅'} |")

    (out_root / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    # ------------------------------------------------------------------
    # Résumés par année
    # ------------------------------------------------------------------
    for year_str, rows in per_year_summary.items():
        ylines = [f"# Contrôle inventaire {year_str} — {datetime.utcnow().isoformat()}Z\n"]
        ylines.append("| Date | Manquants |")
        ylines.append("|------|-----------|")
        for date_str, any_missing in sorted(rows):
            ylines.append(f"| {date_str} | {'❌' if any_missing else '✅'} |")

        (out_root / year_str / "summary.md").write_text("\n".join(ylines), encoding="utf-8")

    if problems_total > 0:
        print(f"[KO] Des éléments manquent. Voir le dossier: {out_root}/")
        return 1

    print("[OK] Aucun manquant détecté.")
    return 0


# -------------------------------------------------------------------
# Entrée CLI
# -------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Vérification de l'inventaire ZEturf (dates, réunions FR, courses)."
    )
    p.add_argument(
        "--root",
        default=str(REPO_ROOT),
        help="Racine des données (par défaut: resultats-et-rapports)",
    )
    p.add_argument(
        "--outdir",
        default="missing",
        help="Dossier de sortie pour les rapports (par défaut: missing)",
    )
    p.add_argument(
        "--start-date",
        help=f"Date de début (YYYY-MM-DD, par défaut: {DEFAULT_START_DATE})",
    )
    p.add_argument(
        "--end-date",
        help=f"Date de fin (YYYY-MM-DD, par défaut: {DEFAULT_END_DATE})",
    )
    p.add_argument(
        "--year",
        help="Limiter le scan à une année (YYYY). Ignore les dates hors de cette année.",
    )
    p.add_argument(
        "--date",
        help="Limiter le scan à une seule date (YYYY-MM-DD).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    repo_root = Path(args.root)
    if not repo_root.exists():
        print(f"Racine introuvable: {repo_root}", file=sys.stderr)
        sys.exit(2)

    # Résolution de l'intervalle de dates
    start_date = args.start_date
    end_date = args.end_date

    if args.date:
        # Une seule date -> start = end
        start_date = end_date = args.date
    elif args.year:
        # Année donnée -> start/end bornés à cette année si non fournis
        if not start_date:
            start_date = f"{args.year}-01-01"
        if not end_date:
            end_date = f"{args.year}-12-31"

    if not start_date:
        start_date = DEFAULT_START_DATE
    if not end_date:
        end_date = DEFAULT_END_DATE

    try:
        # Juste pour valider les formats
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        print(f"Date invalide: {e}", file=sys.stderr)
        sys.exit(2)

    out_root = Path(args.outdir)

    code = run_check(repo_root=repo_root, out_root=out_root, start_date=start_date, end_date=end_date)
    sys.exit(code)


if __name__ == "__main__":
    main()
