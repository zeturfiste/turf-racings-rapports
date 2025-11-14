# -*- coding: utf-8 -*-
"""
Script de re-scraping intelligent des courses manquantes ZEturf
- Parse verification_report.txt pour identifier les courses manquantes
- Reconstruit les URLs directement depuis les noms de fichiers
- Auto-ajustement du batch size avec mémorisation de la limite safe
- Monitoring de l'espace disque
- Commit par année
"""
import os
import re
import asyncio
import aiohttp
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import subprocess

# =========================
# Configuration
# =========================
BASE = "https://www.zeturf.fr"
REPO_ROOT = "resultats-et-rapports"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

# Rate limiting avec auto-ajustement
INITIAL_BATCH_SIZE = 200
MIN_BATCH_SIZE = 10
MAX_SAFE_BATCH_SIZE = None  # Sera défini quand un 429 est détecté
CONSECUTIVE_THRESHOLD = 3   # Nombre de succès avant augmentation
INCREMENT_STEP = 10         # Pas d'augmentation/réduction

# =========================
# Disk monitoring
# =========================
def get_disk_space_gb():
    """Retourne l'espace disque disponible en GB"""
    import shutil
    stat = shutil.disk_usage('/')
    return stat.free / (1024**3)

def check_disk_space_critical():
    """Vérifie si l'espace disque est critique (< 2GB)"""
    free_gb = get_disk_space_gb()
    if free_gb < 2:
        print(f"\n⚠️  ALERTE: Espace disque critique: {free_gb:.2f} GB restants")
        print("Arrêt du scraping pour éviter saturation...")
        return True
    return False

# =========================
# Path helpers
# =========================
def get_date_directory(date_str: str) -> Path:
    """Retourne le chemin du dossier de la date: YYYY/MM/YYYY-MM-DD/"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return Path(REPO_ROOT) / year / month / date_str

def save_html(filepath: Path, html: str):
    """Sauvegarde le HTML dans le fichier"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(html, encoding="utf-8")

# =========================
# Parse verification report
# =========================
def parse_missing_courses(report_path: Path = Path("verification_report.txt")):
    """
    Parse le rapport de vérification pour extraire les courses manquantes.
    
    Format attendu dans le rapport:
        DATE: 2006-04-16 - STATUS: INCOMPLETE
        ❌ R1-auteuil/R1C2-prix-du-president-de-la-republique.html
    
    Returns: 
        dict[year][date] = [(reunion_slug, course_file), ...]
    """
    if not report_path.exists():
        print(f"❌ Fichier {report_path} introuvable")
        return {}
    
    missing = defaultdict(lambda: defaultdict(list))
    current_date = None
    
    with open(report_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            
            # Détecter l'en-tête de date
            if line.startswith("DATE:") and "STATUS:" in line:
                match = re.search(r"DATE:\s*(\d{4}-\d{2}-\d{2})", line)
                if match:
                    current_date = match.group(1)
            
            # Détecter les courses manquantes uniquement
            elif current_date and line.startswith("❌") and "/" in line and ".html" in line:
                # Format: ❌ R1-auteuil/R1C2-prix-du-president-de-la-republique.html
                match = re.search(r"❌\s*([^/]+)/([^/]+\.html)", line)
                if match:
                    reunion_slug = match.group(1)
                    course_file = match.group(2)
                    year = current_date[:4]
                    missing[year][current_date].append((reunion_slug, course_file))
    
    return dict(missing)

# =========================
# URL reconstruction
# =========================
def build_course_url(date_str: str, reunion_slug: str, course_file: str) -> str:
    """
    Reconstruit l'URL de la course depuis le nom de fichier.
    
    Ex: date=2006-04-16, reunion=R1-auteuil, file=R1C2-prix-du-president-de-la-republique.html
    → https://www.zeturf.fr/fr/course/2006-04-16/R1C2-auteuil-prix-du-president-de-la-republique
    """
    # Extract hippodrome from reunion_slug: "R1-auteuil" → "auteuil"
    hippodrome = reunion_slug.split("-", 1)[1] if "-" in reunion_slug else reunion_slug
    
    # Remove .html extension
    course_slug = course_file.replace(".html", "")
    
    # URL format: /fr/course/DATE/CODE-HIPPODROME-TITLE
    # Ex: R1C2-auteuil-prix-du-president-de-la-republique
    url = f"{BASE}/fr/course/{date_str}/{course_slug[:course_slug.find('-')]}-{hippodrome}-{course_slug[course_slug.find('-')+1:]}"
    
    return url

# =========================
# HTTP fetching
# =========================
async def fetch_course(session: aiohttp.ClientSession, url: str, retries=3) -> tuple[str, int]:
    """
    Récupère le HTML d'une course.
    
    Returns: (html, status_code)
    """
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                html = await resp.text()
                return html, resp.status
        except asyncio.TimeoutError:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 * (attempt + 1))
        except Exception as e:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 * (attempt + 1))

# =========================
# Batch scraping avec auto-ajustement
# =========================
async def scrape_courses_batch(session: aiohttp.ClientSession, courses: list, batch_size: int):
    """
    Scrape un batch de courses avec détection du rate limit.
    
    Args:
        courses: [(date, reunion_slug, course_file, filepath), ...]
        batch_size: Nombre de courses à traiter
    
    Returns: 
        (success_count, rate_limited, errors)
    """
    success = 0
    errors = []
    
    for i, (date_str, reunion_slug, course_file, filepath) in enumerate(courses[:batch_size]):
        # Construire l'URL
        url = build_course_url(date_str, reunion_slug, course_file)
        
        try:
            html, status = await fetch_course(session, url)
            
            # Détection du rate limiting
            if status == 429:
                print(f"      ⚠️  Rate limit 429 détecté à la course {i+1}/{batch_size}")
                return success, True, errors
            
            if status == 200:
                save_html(filepath, html)
                success += 1
                print(f"      ✓ {course_file}")
            else:
                errors.append(f"{course_file} (HTTP {status})")
                print(f"      ✗ {course_file} (HTTP {status})")
        
        except Exception as e:
            errors.append(f"{course_file} ({str(e)[:50]})")
            print(f"      ✗ {course_file} (Error: {str(e)[:50]})")
        
        # Petit délai entre les requêtes
        await asyncio.sleep(0.3)
    
    return success, False, errors

# =========================
# Scraping par année avec auto-ajustement
# =========================
async def scrape_year(year: str, dates_courses: dict, initial_batch_size: int):
    """
    Scrape toutes les courses manquantes pour une année avec batch size adaptatif.
    
    Args:
        year: Année à traiter
        dates_courses: dict[date] = [(reunion_slug, course_file), ...]
        initial_batch_size: Taille initiale des batchs
    """
    global MAX_SAFE_BATCH_SIZE
    
    print(f"\n{'='*80}")
    print(f"ANNÉE {year}")
    print(f"{'='*80}\n")
    
    # Vérifier l'espace disque avant de commencer
    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque disponible: {free_gb:.2f} GB")
    
    if free_gb < 3:
        print(f"⚠️  Espace insuffisant pour traiter cette année")
        return
    
    # Aplatir toutes les courses pour cette année
    all_courses = []
    for date_str, courses_list in sorted(dates_courses.items()):
        for reunion_slug, course_file in courses_list:
            date_dir = get_date_directory(date_str)
            reunion_dir = date_dir / reunion_slug
            filepath = reunion_dir / course_file
            all_courses.append((date_str, reunion_slug, course_file, filepath))
    
    total_courses = len(all_courses)
    print(f"📊 {total_courses} courses à récupérer pour {year}")
    
    if total_courses == 0:
        return
    
    # Statistiques
    stats = {
        "success": 0,
        "failed": 0,
        "rate_limits": 0,
        "batch_increases": 0,
        "batch_decreases": 0
    }
    
    batch_size = initial_batch_size
    position = 0
    consecutive_successes = 0  # Compteur pour l'auto-ajustement
    
    async with aiohttp.ClientSession() as session:
        while position < total_courses:
            # Vérifier l'espace disque avant chaque batch
            if check_disk_space_critical():
                print(f"⚠️  Arrêt à la position {position}/{total_courses}")
                break
            
            remaining = total_courses - position
            current_batch_size = min(batch_size, remaining)
            
            # Afficher le statut
            free_gb = get_disk_space_gb()
            print(f"\n  📦 Batch: courses {position+1}-{position+current_batch_size}/{total_courses} (size: {current_batch_size})")
            print(f"  💾 Espace libre: {free_gb:.2f} GB")
            
            # Traiter le batch
            batch = all_courses[position:position+current_batch_size]
            success, rate_limited, errors = await scrape_courses_batch(session, batch, current_batch_size)
            
            stats["success"] += success
            stats["failed"] += len(errors)
            
            if rate_limited:
                # Rate limit détecté
                stats["rate_limits"] += 1
                consecutive_successes = 0
                
                # Mémoriser la limite safe (taille actuelle - 10)
                if MAX_SAFE_BATCH_SIZE is None or batch_size - INCREMENT_STEP < MAX_SAFE_BATCH_SIZE:
                    MAX_SAFE_BATCH_SIZE = batch_size - INCREMENT_STEP
                    print(f"      📌 Limite safe détectée: {MAX_SAFE_BATCH_SIZE}")
                
                # Réduire la taille du batch
                batch_size = max(MIN_BATCH_SIZE, batch_size - INCREMENT_STEP)
                stats["batch_decreases"] += 1
                print(f"      🔽 Réduction batch size: {batch_size}")
                print(f"      ⏸️  Attente 30s avant retry...")
                await asyncio.sleep(30)
                
                # Ne pas incrémenter position - retry le même batch
                continue
            
            # Batch réussi
            consecutive_successes += 1
            
            # Tentative d'augmentation si 3 succès consécutifs
            if consecutive_successes >= CONSECUTIVE_THRESHOLD:
                can_increase = True
                
                # Ne pas dépasser la limite safe connue
                if MAX_SAFE_BATCH_SIZE is not None:
                    if batch_size >= MAX_SAFE_BATCH_SIZE:
                        can_increase = False
                        print(f"      ℹ️  Batch size au maximum safe ({MAX_SAFE_BATCH_SIZE})")
                
                if can_increase:
                    batch_size += INCREMENT_STEP
                    consecutive_successes = 0
                    stats["batch_increases"] += 1
                    print(f"      🔼 Augmentation batch size: {batch_size}")
            
            # Passer au batch suivant
            position += current_batch_size
            
            # Petit délai entre les batchs
            if position < total_courses:
                await asyncio.sleep(2)
    
    # Afficher le résumé
    print(f"\n{'='*80}")
    print(f"RÉSUMÉ ANNÉE {year}")
    print(f"{'='*80}")
    print(f"✓ Succès:          {stats['success']}/{total_courses}")
    print(f"✗ Échecs:          {stats['failed']}")
    print(f"⚠️  Rate limits:     {stats['rate_limits']}")
    print(f"🔼 Augmentations:   {stats['batch_increases']}")
    print(f"🔽 Réductions:      {stats['batch_decreases']}")
    if MAX_SAFE_BATCH_SIZE is not None:
        print(f"📌 Max safe size:   {MAX_SAFE_BATCH_SIZE}")
    print(f"💾 Espace final:    {get_disk_space_gb():.2f} GB")
    print(f"{'='*80}\n")

# =========================
# Git operations
# =========================
def git_commit_year(year: str):
    """Commit et push les changements pour l'année"""
    print(f"\n📤 Git commit pour l'année {year}...")
    try:
        subprocess.run(["git", "config", "user.name", "GitHub Actions Bot"], check=True)
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        
        # Vérifier s'il y a des changements
        result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not result.stdout.strip():
            print("  ℹ️  Aucun changement pour cette année")
            return
        
        subprocess.run(["git", "add", f"{REPO_ROOT}/{year}"], check=True)
        
        # Compter les fichiers ajoutés
        files_added = result.stdout.count('\n')
        
        subprocess.run([
            "git", "commit", "-m", 
            f"Re-scrape: {year} - {files_added} courses ajoutées",
            "-m", f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
        ], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"  ✓ Année {year} committée ({files_added} fichiers)\n")
    except subprocess.CalledProcessError as e:
        print(f"  ✗ Erreur Git: {e}\n")

# =========================
# Main orchestrator
# =========================
async def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Re-scrape intelligent des courses ZEturf manquantes"
    )
    parser.add_argument(
        "--max-courses", 
        type=int, 
        default=None, 
        help="Limite globale de courses à traiter"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=INITIAL_BATCH_SIZE, 
        help="Taille initiale des batchs (défaut: 200)"
    )
    args = parser.parse_args()
    
    print("="*80)
    print("RE-SCRAPING DIRECT DES COURSES MANQUANTES")
    print("="*80 + "\n")
    
    # Vérification initiale de l'espace disque
    free_gb = get_disk_space_gb()
    print(f"💾 Espace disque initial: {free_gb:.2f} GB\n")
    
    if free_gb < 5:
        print("⚠️  WARNING: Espace disque faible! Recommandé: > 5GB")
        print("Continuation avec prudence...\n")
    
    # Parser le rapport
    missing_by_year = parse_missing_courses()
    
    if not missing_by_year:
        print("✓ Aucune course manquante détectée\n")
        return
    
    # Résumé
    total_courses = sum(
        len(courses)
        for year_data in missing_by_year.values()
        for courses in year_data.values()
    )
    print(f"📊 {len(missing_by_year)} années avec courses manquantes")
    print(f"📊 {total_courses} courses manquantes au total\n")
    
    # Traiter année par année
    courses_processed = 0
    for year in sorted(missing_by_year.keys()):
        # Vérifier l'espace disque avant chaque année
        free_gb = get_disk_space_gb()
        if free_gb < 2:
            print(f"⚠️  ARRÊT: Espace disque insuffisant ({free_gb:.2f} GB)")
            print(f"Progression: {courses_processed}/{total_courses} courses traitées")
            break
        
        # Vérifier la limite globale
        if args.max_courses and courses_processed >= args.max_courses:
            print(f"⚠️  Limite globale atteinte ({args.max_courses} courses)")
            break
        
        # Scraper l'année
        await scrape_year(year, missing_by_year[year], args.batch_size)
        
        # Committer pour cette année
        git_commit_year(year)
        
        # Mettre à jour le compteur
        year_courses = sum(len(c) for c in missing_by_year[year].values())
        courses_processed += year_courses
    
    print("\n" + "="*80)
    print("SCRAPING TERMINÉ")
    print(f"💾 Espace disque final: {get_disk_space_gb():.2f} GB")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())
