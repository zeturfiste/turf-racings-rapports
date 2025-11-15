# -*- coding: utf-8 -*-
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
REPO_ROOT = "resultats-et-rapports"
BASE = "https://www.zeturf.fr"
START_DATE = "2005-04-27"
END_DATE = "2025-11-11"

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
    """Returns: resultats-et-rapports/2025/11/2025-11-10/"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str

def date_range(start_date: str, end_date: str):
    """Generate all dates in range (inclusive)"""
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_date, "%Y-%m-%d").date()
    if d0 > d1:
        d0, d1 = d1, d0
    days = (d1 - d0).days
    return [(d0 + timedelta(days=i)).isoformat() for i in range(days + 1)]

# =========================
# Verification functions
# =========================
def verify_date(date_str: str):
    """
    Vérifie une date complète:
    1. Dossier existe
    2. Fichier HTML de la date existe
    3. Toutes les réunions FR ont leur dossier + fichier
    4. Toutes les courses de chaque réunion ont leur fichier
    
    Returns: dict with status and missing items
    """
    result = {
        "date": date_str,
        "status": "OK",
        "issues": []
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
            result["issues"].append(f"⚠️  Aucun conteneur #list-reunion trouvé dans {date_file.name}")
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
            
            expected_reunions.append({
                "code": reunion_code,
                "slug": reunion_slug,
                "hippodrome": hippodrome,
                "href": href
            })
        
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
                result["issues"].append(f"❌ Fichier réunion absent: {reunion['slug']}/{reunion_file.name}")
                continue
            
            # Check 4: Verify courses for this reunion
            reunion_issues = verify_reunion_courses(reunion_file, reunion_dir, reunion["code"])
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

def verify_reunion_courses(reunion_file: Path, reunion_dir: Path, reunion_code: str):
    """
    Vérifie que toutes les courses d'une réunion ont leur fichier HTML
    Returns: list of issues (empty if OK)
    """
    issues = []
    
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
            numero_txt = a.select_one("span.numero")
            numero_txt = numero_txt.get_text(strip=True) if numero_txt else ""
            mC = re.search(r"C(\d+)", href)
            numero = int(numero_txt) if numero_txt.isdigit() else (int(mC.group(1)) if mC else None)
            
            # Extract title
            title = a.get("title", "").strip()
            heure, intitule = None, None
            if " - " in title:
                heure, intitule = title.split(" - ", 1)
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
                "code": code
            })
        
        # Verify each course file exists
        for course in expected_courses:
            course_file = reunion_dir / course["filename"]
            if not course_file.exists() or course_file.stat().st_size == 0:
                issues.append(f"❌ {reunion_dir.name}/{course['filename']}")
        
    except Exception as e:
        issues.append(f"❌ {reunion_dir.name}/: Erreur analyse courses - {e}")
    
    return issues

# =========================
# Main verification
# =========================
def run_verification(start_date: str, end_date: str, output_file: Path):
    """
    Parcourt toutes les dates dans [start_date, end_date] (inclus) et génère un rapport.
    Les bornes sont automatiquement clampées à [START_DATE, END_DATE].
    """
    # Clamp de la plage à la plage globale connue
    global_start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    global_end = datetime.strptime(END_DATE, "%Y-%m-%d").date()
    
    d_start = datetime.strptime(start_date, "%Y-%m-%d").date()
    d_end = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    if d_start < global_start:
        d_start = global_start
    if d_end > global_end:
        d_end = global_end
    
    if d_start > d_end:
        # Plage vide
        effective_start = d_start.isoformat()
        effective_end = d_end.isoformat()
        all_dates = []
    else:
        effective_start = d_start.isoformat()
        effective_end = d_end.isoformat()
        all_dates = date_range(effective_start, effective_end)
    
    total_dates = len(all_dates)
    
    print("="*80)
    print("VÉRIFICATION COMPLÈTE DES DONNÉES ZETURF")
    print("="*80)
    print(f"Période demandée : {start_date} → {end_date}")
    print(f"Période effective: {effective_start} → {effective_end}")
    print(f"Nombre de dates à vérifier: {total_dates}\n")
    
    # Statistics
    stats = {
        "total": total_dates,
        "ok": 0,
        "missing": 0,
        "incomplete": 0,
        "warning": 0,
        "error": 0
    }
    
    # Store all issues by date
    incomplete_dates = []
    
    # Verify each date
    for i, date_str in enumerate(all_dates, 1):
        print(f"[{i}/{total_dates}] Vérification {date_str}...", end=" ")
        
        result = verify_date(date_str)
        
        if result["status"] == "OK":
            stats["ok"] += 1
            print("✓ OK")
        else:
            key = result["status"].lower()
            if key in stats:
                stats[key] += 1
            print(f"✗ {result['status']}")
            incomplete_dates.append(result)
    
    # Generate report
    print("\n" + "="*80)
    print("RAPPORT DE VÉRIFICATION")
    print("="*80)
    print(f"\nStatistiques:")
    print(f"  Total de dates:        {stats['total']}")
    print(f"  ✓ Complètes:           {stats['ok']} ({(stats['ok']/stats['total']*100):.1f}%)" if stats['total'] else "  ✓ Complètes:           0 (0.0%)")
    print(f"  ❌ Absentes:            {stats['missing']}")
    print(f"  ⚠️  Incomplètes:         {stats['incomplete']}")
    print(f"  ⚠️  Warnings:            {stats['warning']}")
    print(f"  ❌ Erreurs:             {stats['error']}")
    
    # Write detailed report to file
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with output_file.open("w", encoding="utf-8") as f:
        f.write("="*80 + "\n")
        f.write("RAPPORT DE VÉRIFICATION ZETURF\n")
        f.write("="*80 + "\n\n")
        f.write(f"Date de vérification: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Période analysée: {effective_start} → {effective_end}\n\n")
        
        f.write("STATISTIQUES\n")
        f.write("-"*80 + "\n")
        f.write(f"Total de dates:        {stats['total']}\n")
        f.write(f"✓ Complètes:           {stats['ok']} ({(stats['ok']/stats['total']*100):.1f}%)\n" if stats['total'] else "✓ Complètes:           0 (0.0%)\n")
        f.write(f"❌ Absentes:            {stats['missing']}\n")
        f.write(f"⚠️  Incomplètes:         {stats['incomplete']}\n")
        f.write(f"⚠️  Warnings:            {stats['warning']}\n")
        f.write(f"❌ Erreurs:             {stats['error']}\n\n")
        
        if incomplete_dates:
            f.write("\n" + "="*80 + "\n")
            f.write("DATES INCOMPLÈTES OU PROBLÉMATIQUES\n")
            f.write("="*80 + "\n\n")
            
            for result in incomplete_dates:
                f.write(f"\n{'='*80}\n")
                f.write(f"DATE: {result['date']} - STATUS: {result['status']}\n")
                f.write(f"{'='*80}\n")
                for issue in result["issues"]:
                    f.write(f"  {issue}\n")
        else:
            f.write("\n🎉 Toutes les dates sont complètes !\n")
    
    print(f"\n📄 Rapport détaillé écrit dans: {output_file}")
    print("\n" + "="*80)
    
    return stats, incomplete_dates

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Vérification des HTMLs ZEturf")
    parser.add_argument(
        "--year",
        type=str,
        default=None,
        help="Année à vérifier (ex: 2008). Si absent, utilise START_DATE/END_DATE globaux."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Date de début (YYYY-MM-DD). Ignoré si --year est fourni."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Date de fin (YYYY-MM-DD). Ignoré si --year est fourni."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Dossier où écrire le rapport (par défaut: dossier courant)."
    )
    
    args = parser.parse_args()
    
    if args.year:
        year = args.year.strip()
        start = f"{year}-01-01"
        end = f"{year}-12-31"
        output_dir = Path(args.output_dir)
        output_file = output_dir / f"verify_{year}.txt"
    else:
        start = args.start_date or START_DATE
        end = args.end_date or END_DATE
        output_dir = Path(args.output_dir)
        output_file = output_dir / "verification_report.txt"
    
    # LANCE la vérif (aucun exit(1), même s'il y a des manques)
    run_verification(start, end, output_file)
