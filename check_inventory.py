# check_inventory.py
# -*- coding: utf-8 -*-
import argparse
import json
import re
import sys
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

REPO_ROOT = Path("resultats-et-rapports")

DATE_DIR_RE     = re.compile(r"^\d{4}-\d{2}-\d{2}$")
REUNION_DIR_RE  = re.compile(r"^R\d+-[a-z0-9-]+$")
COURSE_FILE_RE  = re.compile(r"^R\d+C\d+-[a-z0-9-]+\.html$")

def load_html(p: Path) -> str | None:
    try:
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        pass
    return None

def parse_fr_reunions_from_date_html(html: str) -> list[str]:
    """
    Retourne la liste des slugs de réunions FR (ex: 'R1-saint-cloud') trouvés
    dans la frise du programme FR de la page 'date'.
    """
    soup = BeautifulSoup(html, "lxml")
    slugs = []
    # Chaque 'li.scroll-element' contient un <a href="/fr/reunion/YYYY-MM-DD/Rx-...">
    for li in soup.select("#frise-programme #frise-reunion li.scroll-element"):
        code = li.select_one("span.hide-tld-fr")
        if not code:
            continue
        code_txt = (code.get_text(strip=True) or "").upper()
        if not code_txt.startswith("FR"):
            continue  # on ne garde que les réunions FR
        a = li.select_one("a[href*='/fr/reunion/']")
        if not a:
            continue
        href = a.get("href", "")
        # On récupère le segment final 'Rx-hippo'
        m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/([^/?#]+)$", href)
        if m:
            slugs.append(m.group(1).strip())
    return slugs

def parse_course_slugs_from_reunion_html(html: str) -> list[str]:
    """
    Retourne la liste des slugs de courses (ex: 'R1C1-saint-cloud-prix-...') trouvés
    dans la frise des courses d'une page 'réunion'.
    """
    soup = BeautifulSoup(html, "lxml")
    slugs = []
    for a in soup.select("#frise-course .strip2 ul.scroll-content li a[href*='/fr/course/']"):
        href = a.get("href", "")
        m = re.search(r"/course/\d{4}-\d{2}-\d{2}/([^/?#]+)$", href)
        if m:
            slugs.append(m.group(1).strip())
    return slugs

def collect_dates_to_scan(root: Path, only_date: str | None = None) -> list[Path]:
    dates = []
    for ydir in sorted(root.glob("[0-9][0-9][0-9][0-9]")):
        for mdir in sorted(ydir.glob("[0-1][0-9]")):
            for ddir in sorted(mdir.iterdir()):
                if ddir.is_dir() and DATE_DIR_RE.match(ddir.name):
                    if only_date and ddir.name != only_date:
                        continue
                    dates.append(ddir)
    return dates

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def main():
    ap = argparse.ArgumentParser(description="Contrôle d'inventaire ZEturf")
    ap.add_argument("--root", default=str(REPO_ROOT), help="Racine des données (resultats-et-rapports)")
    ap.add_argument("--date", help="Scanner UNIQUEMENT cette date (YYYY-MM-DD)")
    ap.add_argument("--outdir", default="missing", help="Dossier de sortie pour les rapports")
    args = ap.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"Racine introuvable: {root}", file=sys.stderr)
        sys.exit(2)

    outdir = Path(args.outdir)
    ensure_dir(outdir)

    dates = collect_dates_to_scan(root, only_date=args.date)
    if not dates:
        print("Aucune date à scanner.")
        return

    missing_all = []  # cumulé pour summary.md

    for ddir in dates:
        date_str = ddir.name
        ydir = ddir.parent.parent
        mdir = ddir.parent

        date_html_path = ddir / f"{date_str}.html"
        date_html = load_html(date_html_path)

        report = {
            "date": date_str,
            "paths": {
                "date_dir": str(ddir),
                "date_html": str(date_html_path),
            },
            "missing": {
                "date_dir": False,
                "date_html": False,
                "reunion_dirs": [],
                "reunion_html": [],
                "course_files": [],
            },
            "expected": {
                "reunions_fr": [],
                "courses_by_reunion": {},
            },
            "checked_at": datetime.utcnow().isoformat()+"Z",
        }

        # 1) Date dir + html
        if not ddir.exists():
            report["missing"]["date_dir"] = True
        if not date_html:
            report["missing"]["date_html"] = True

        # 2) Si on a le HTML de date, lister les réunions FR attendues
        if date_html:
            fr_reunions = parse_fr_reunions_from_date_html(date_html)
            report["expected"]["reunions_fr"] = fr_reunions

            for rslug in fr_reunions:
                rdir = ddir / rslug
                if not rdir.exists():
                    report["missing"]["reunion_dirs"].append(str(rdir))
                    continue

                rhtml_path = rdir / f"{rslug}.html"
                rhtml = load_html(rhtml_path)
                if not rhtml:
                    report["missing"]["reunion_html"].append(str(rhtml_path))
                    continue

                # 3) Depuis le HTML de réunion, lister les courses attendues
                course_slugs = parse_course_slugs_from_reunion_html(rhtml)
                report["expected"]["courses_by_reunion"][rslug] = course_slugs

                for cslug in course_slugs:
                    cfile = rdir / f"{cslug}.html"
                    if not (cfile.exists() and cfile.is_file() and cfile.stat().st_size > 0):
                        report["missing"]["course_files"].append(str(cfile))

        # Sauvegarde du rapport JSON de la date
        out_json = outdir / f"{date_str}.json"
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        # Ajout au cumul pour summary
        if report["missing"]["date_dir"] or report["missing"]["date_html"] \
           or report["missing"]["reunion_dirs"] or report["missing"]["reunion_html"] \
           or report["missing"]["course_files"]:
            missing_all.append(report)

    # Écriture d'un résumé Markdown global
    lines = []
    lines.append(f"# Manquants détectés — {datetime.utcnow().isoformat()}Z\n")
    if not missing_all:
        lines.append("Aucun fichier manquant détecté 🎉")
    else:
        for rep in missing_all:
            lines.append(f"## {rep['date']}")
            m = rep["missing"]
            if m["date_dir"]:
                lines.append(f"- ❌ Dossier de date manquant: `{rep['paths']['date_dir']}`")
            if m["date_html"]:
                lines.append(f"- ❌ Fichier HTML de date manquant: `{rep['paths']['date_html']}`")
            for p in m["reunion_dirs"]:
                lines.append(f"- ❌ Dossier de réunion manquant: `{p}`")
            for p in m["reunion_html"]:
                lines.append(f"- ❌ Fichier HTML de réunion manquant: `{p}`")
            for p in m["course_files"]:
                lines.append(f"- ❌ Fichier HTML de course manquant: `{p}`")
            lines.append("")
    (outdir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    # Log console (bref)
    if missing_all:
        print(f"[KO] Des manquants ont été consignés dans {outdir}/")
        sys.exit(1)
    else:
        print("[OK] Aucun manquant.")
        sys.exit(0)

if __name__ == "__main__":
    main()
