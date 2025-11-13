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
    """Retourne [{slug, reunion_code, hippodrome}] depuis la page date (FR uniquement)."""
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
        reunions.append({"slug": reunion_slug, "reunion_code": reunion_code, "hippodrome": hippodrome})
    return reunions

def parse_course_filenames_from_reunion_html(html: str, reunion_code: str):
    """Retourne ['R1C3-<slug>.html', ...] depuis la page réunion."""
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
            continue
        title = a.get("title", "").strip()
        intitule = title.split(" - ", 1)[1] if " - " in title else (title or "")
        slug = slugify(intitule) or "course"
        filenames.append(f"{reunion_code}{code.upper()}-{slug}.html")
    return filenames

# ---- Scan global -------------------------------------------------------------

def iter_date_dirs(root: Path):
    for ydir in sorted(root.glob("[0-9][0-9][0-9][0-9]")):
        for mdir in sorted(ydir.glob("[0-1][0-9]")):
            for ddir in sorted(mdir.iterdir()):
                if ddir.is_dir() and DATE_DIR_RE.match(ddir.name):
                    yield ddir

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def main():
    root = REPO_ROOT
    if not root.exists():
        print(f"Racine introuvable: {root}", file=sys.stderr)
        sys.exit(2)

    out_root = Path("missing")
    ensure_dir(out_root)

    incomplete_dates = []           # ex. ["2025-11-09", ...]
    incomplete_by_year = {}         # ex. {"2025": ["2025-11-09", ...]}
    any_problem = False

    # Pour un résumé léger (par date uniquement)
    summary_rows = []

    for ddir in iter_date_dirs(root):
        date_str = ddir.name
        year = date_str[:4]
        out_year_dir = out_root / year
        ensure_dir(out_year_dir)

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
            "expected": {"reunions_fr": [], "courses_by_reunion": {}},
            "checked_at": datetime.utcnow().isoformat()+"Z",
        }

        # 1) Page date
        if not date_html:
            report["missing"]["date_html"] = True
        else:
            expected_reunions = [r["slug"] for r in parse_reunions_fr_from_date_html(date_html)]
            report["expected"]["reunions_fr"] = expected_reunions

            # 2) Dossiers + page réunion + 3) courses
            for rslug in expected_reunions:
                rdir = ddir / rslug
                if not rdir.exists():
                    report["missing"]["reunion_dirs"].append(str(rdir))
                    continue

                rhtml_path = rdir / f"{rslug}.html"
                rhtml = read_html(rhtml_path)
                if not rhtml:
                    report["missing"]["reunion_html"].append(str(rhtml_path))
                    continue

                mR = re.match(r"^(R\d+)-", rslug)
                reunion_code = mR.group(1) if mR else None
                expected_courses = parse_course_filenames_from_reunion_html(rhtml, reunion_code) if reunion_code else []
                report["expected"]["courses_by_reunion"][rslug] = expected_courses

                for cfile in expected_courses:
                    cpath = rdir / cfile
                    if not (cpath.exists() and cpath.is_file() and cpath.stat().st_size > 0):
                        report["missing"]["course_files"].append(str(cpath))

        # Écrire le rapport par date dans missing/<year>/<date>.json
        (out_year_dir / f"{date_str}.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        # Marquer date incomplète si quelque chose manque
        m = report["missing"]
        is_incomplete = m["date_html"] or m["reunion_dirs"] or m["reunion_html"] or m["course_files"]
        summary_rows.append((date_str, is_incomplete))
        if is_incomplete:
            any_problem = True
            incomplete_dates.append(date_str)
            incomplete_by_year.setdefault(year, []).append(date_str)

    # --- Sorties “dates incomplètes” (simples à consommer) ---
    # 1) liste brute
    (out_root / "dates_incomplete.txt").write_text("\n".join(sorted(incomplete_dates)), encoding="utf-8")
    # 2) json
    (out_root / "dates_incomplete.json").write_text(json.dumps({"dates": sorted(incomplete_dates),
                                                                "by_year": {k: sorted(v) for k, v in incomplete_by_year.items()}},
                                                               ensure_ascii=False, indent=2), encoding="utf-8")

    # 3) résumé lisible (uniquement les dates incomplètes)
    lines = [f"# Dates incomplètes — {datetime.utcnow().isoformat()}Z\n"]
    if not incomplete_dates:
        lines.append("Aucune date incomplète 🎉")
    else:
        lines.append("| Date | Incomplète |")
        lines.append("|------|------------|")
        for date_str, is_incomplete in sorted(summary_rows):
            if is_incomplete:
                lines.append(f"| {date_str} | ❌ |")
    (out_root / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    # 4) Générer un script de re-scrape ciblé si besoin
    gen_script = Path("scrape_incomplete.py")
    if incomplete_dates:
        gen = [
            "# -*- coding: utf-8 -*-",
            "import asyncio",
            "from scraper import scrape_year, group_by_year, git_commit_push",
            "",
            f"INCOMPLETE_DATES = {sorted(incomplete_dates)!r}",
            "",
            "async def main():",
            "    years = group_by_year(INCOMPLETE_DATES)",
            "    for year in sorted(years.keys()):",
            "        print(f'== Re-scrape année {year} ({len(years[year])} dates) ==')",
            "        await scrape_year(year, years[year])",
            "        git_commit_push(year)",
            "",
            "if __name__ == '__main__':",
            "    asyncio.run(main())",
            "",
        ]
        gen_script.write_text("\n".join(gen), encoding="utf-8")
    else:
        # S'il n'y a rien à refaire, supprimer un ancien script éventuel pour éviter la confusion
        if gen_script.exists():
            try:
                gen_script.unlink()
            except Exception:
                pass

    # Code de sortie
    if any_problem:
        print("[KO] Des dates incomplètes ont été détectées. Voir le dossier missing/ et le script scrape_incomplete.py")
        sys.exit(1)
    else:
        print("[OK] Toutes les dates sont complètes.")
        sys.exit(0)

if __name__ == "__main__":
    main()
