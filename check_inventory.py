# check_inventory.py
# -*- coding: utf-8 -*-
import json
import re
import sys
import unicodedata
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

REPO_ROOT = Path("resultats-et-rapports")

DATE_DIR_RE    = re.compile(r"^\d{4}-\d{2}-\d{2}$")
REUNION_DIR_RE = re.compile(r"^R\d+-[a-z0-9-]+$")
COURSE_FILE_RE = re.compile(r"^R\d+C\d+-[a-z0-9-]+\.html$")

def slugify(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text

def read_html(p: Path) -> str | None:
    try:
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        pass
    return None

# ---- Parseurs alignés sur scraper.py ----------------------------------------

def parse_reunions_fr_from_date_html(html: str):
    """
    Renvoie une liste de dicts {slug, reunion_code, hippodrome}.
    Logique calquée sur scrape_date() du scraper.
    """
    soup = BeautifulSoup(html, "lxml")
    container = soup.select_one("div#list-reunion")
    if not container:
        return []

    reunions = []
    for tr in container.select("table.programme tbody tr.item"):
        a = tr.select_one('td.numero a[data-tc-pays="FR"]')
        if not a:
            continue
        href = (a.get("href") or "").strip()
        if not href:
            continue

        m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
        reunion_code = m.group(1) if m else (a.get_text(strip=True).replace("FR", "R"))

        hippo_el = tr.select_one("td.nom h2 span span")
        hippodrome = hippo_el.get_text(strip=True) if hippo_el else ""
        reunion_slug = f"{reunion_code}-{slugify(hippodrome)}"

        reunions.append({
            "slug": reunion_slug,
            "reunion_code": reunion_code,
            "hippodrome": hippodrome,
        })
    return reunions

def parse_course_filenames_from_reunion_html(html: str, reunion_code: str):
    """
    Renvoie la liste des noms de fichiers de courses attendus (ex: 'R1C3-prix-de-paris.html')
    Logique calquée sur scrape_reunion() du scraper.
    """
    soup = BeautifulSoup(html, "lxml")
    frise = soup.select_one("#frise-course .strip2.active") or soup.select_one("#frise-course .strip2")
    if not frise:
        return []

    filenames = []
    for a in frise.select("ul.scroll-content li.scroll-element a"):
        href = a.get("href", "")
        if not href:
            continue

        numero_txt_el = a.select_one("span.numero")
        numero_txt = numero_txt_el.get_text(strip=True) if numero_txt_el else ""
        mC = re.search(r"C(\d+)", href)
        numero = int(numero_txt) if numero_txt.isdigit() else (int(mC.group(1)) if mC else None)
        code = f"C{numero}" if numero is not None else (mC.group(0) if mC else None)
        if not code:
            # pas de code course → on ignore pour éviter les faux positifs
            continue

        title = a.get("title", "").strip()
        intitule = title.split(" - ", 1)[1] if " - " in title else (title or "")
        slug = slugify(intitule) or "course"
        filename = f"{reunion_code}{code.upper()}-{slug}.html"
        filenames.append(filename)
    return filenames

# ---- Scan global -------------------------------------------------------------

def iter_date_dirs(root: Path):
    for ydir in sorted(root.glob("[0-9][0-9][0-9][0-9]")):
        for mdir in sorted(ydir.glob("[0-1][0-9]")):
            for ddir in sorted(mdir.iterdir()):
                if ddir.is_dir() and DATE_DIR_RE.match(ddir.name):
                    yield ddir

def main():
    root = REPO_ROOT
    if not root.exists():
        print(f"Racine introuvable: {root}", file=sys.stderr)
        sys.exit(2)

    outdir = Path("missing")
    outdir.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    problems_total = 0

    for ddir in iter_date_dirs(root):
        date_str = ddir.name
        date_html_path = ddir / f"{date_str}.html"
        date_html = read_html(date_html_path)

        report = {
            "date": date_str,
            "paths": {"date_dir": str(ddir), "date_html": str(date_html_path)},
            "missing": {
                "date_html": False,
                "reunion_dirs": [],
                "reunion_html": [],
                "course_files": [],
            },
            "extras": {
                "reunion_dirs": [],
                "course_files": [],
            },
            "expected": {
                "reunions_fr": [],
                "courses_by_reunion": {},
            },
            "checked_at": datetime.utcnow().isoformat()+"Z",
        }

        # --- Date HTML ---
        if not date_html:
            report["missing"]["date_html"] = True
            problems_total += 1
            # même si la page date manque, on continue pour signaler d'éventuels dossiers orphelins
            expected_reunions = []
        else:
            expected = parse_reunions_fr_from_date_html(date_html)
            expected_reunions = [r["slug"] for r in expected]
            report["expected"]["reunions_fr"] = expected_reunions

        # --- Réunions attendues (présence + html) ---
        expected_set = set(expected_reunions)
        for rslug in expected_reunions:
            rdir = ddir / rslug
            if not rdir.exists():
                report["missing"]["reunion_dirs"].append(str(rdir))
                problems_total += 1
                continue

            rhtml_path = rdir / f"{rslug}.html"
            rhtml = read_html(rhtml_path)
            if not rhtml:
                report["missing"]["reunion_html"].append(str(rhtml_path))
                problems_total += 1
                continue

            # Courses attendues (depuis le HTML de la réunion)
            # Retrouver le code 'Rx' à partir du slug
            mR = re.match(r"^(R\d+)-", rslug)
            reunion_code = mR.group(1) if mR else None
            expected_courses = parse_course_filenames_from_reunion_html(rhtml, reunion_code) if reunion_code else []
            report["expected"]["courses_by_reunion"][rslug] = expected_courses
            expected_courses_set = set(expected_courses)

            # Signaler manquants
            for cfile in expected_courses:
                cpath = rdir / cfile
                if not (cpath.exists() and cpath.is_file() and cpath.stat().st_size > 0):
                    report["missing"]["course_files"].append(str(cpath))
                    problems_total += 1

            # Signaler extras (fichiers course présents mais non attendus)
            present_courses = {p.name for p in rdir.glob("R*C*-*.html") if p.name != f"{rslug}.html"}
            extras = sorted(present_courses - expected_courses_set)
            for ex in extras:
                report["extras"]["course_files"].append(str((rdir / ex)))

        # --- Extras : répertoires de réunion non attendus ---
        present_rdirs = {p.name for p in ddir.iterdir() if p.is_dir() and REUNION_DIR_RE.match(p.name)}
        extras_r = sorted(present_rdirs - expected_set)
        for exr in extras_r:
            report["extras"]["reunion_dirs"].append(str(ddir / exr))

        # --- Sauvegarde JSON par date ---
        (outdir / f"{date_str}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        # --- Accumuler pour résumé ---
        miss = report["missing"]
        any_missing = miss["date_html"] or miss["reunion_dirs"] or miss["reunion_html"] or miss["course_files"]
        any_extra = report["extras"]["reunion_dirs"] or report["extras"]["course_files"]
        summary_rows.append((date_str, any_missing, any_extra))

    # --- Résumé Markdown global ---
    lines = [f"# Contrôle inventaire — {datetime.utcnow().isoformat()}Z\n"]
    if not summary_rows:
        lines.append("Aucune date trouvée.")
    else:
        lines.append("| Date | Manquants | Extras |")
        lines.append("|------|-----------|--------|")
        for date_str, any_missing, any_extra in summary_rows:
            lines.append(f"| {date_str} | {'❌' if any_missing else '✅'} | {'⚠️' if any_extra else '—'} |")
    (outdir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    if problems_total > 0:
        print(f"[KO] Des éléments manquent. Voir le dossier: {outdir}/")
        sys.exit(1)
    print("[OK] Aucun manquant.")
    sys.exit(0)

if __name__ == "__main__":
    main()
