# -*- coding: utf-8 -*-
import os
import re
import json
import time
import unicodedata
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

# =========================
# Config
# =========================
BASE = "https://www.zeturf.fr"
DATE_URL_TPL = BASE + "/fr/resultats-et-rapports/{date}"  # YYYY-MM-DD
SAVE_ROOT = "/content/drive/MyDrive/TURF/ZEturf/resultats-et-rapports"       # racine demandée

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
})

# =========================
# Helpers
# =========================
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def slugify(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text

def get_html(url: str, retries=3, timeout=30) -> str:
    for i in range(retries):
        try:
            r = SESSION.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(1.2 * (i + 1))

def date_range_desc(start_date: str, end_date: str):
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_date, "%Y-%m-%d").date()
    if d0 > d1:
        d0, d1 = d1, d0
    days = (d1 - d0).days
    all_dates = [(d0 + timedelta(days=i)).isoformat() for i in range(days + 1)]
    return list(reversed(all_dates))  # plus récent -> plus ancien

def get_date_directory(date_str: str) -> str:
    """
    Construit le chemin: <SAVE_ROOT>/<YYYY>/<MM>/<YYYY-MM-DD>/
    Exemple: /content/drive/MyDrive/TURF/ZEturf/resultats-et-rapports/2025/11/2025-11-10/
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    return os.path.join(SAVE_ROOT, year, month, date_str)

def save_html_file(dest_file: str, html: str):
    ensure_dir(os.path.dirname(dest_file))
    with open(dest_file, "w", encoding="utf-8") as f:
        f.write(html)

# =========================
# Scrapers
# =========================
def parse_reunions_fr_for_date(date_str: str):
    """
    Depuis la page de la date, récupère UNIQUEMENT les réunions FR
    présentes dans <div id="list-reunion"> via un <a data-tc-pays="FR"> dans la cellule numéro.
    Sauvegarde la page date en <YYYY-MM-DD>.html dans le dossier de la date.
    Retourne une liste de réunions [{date, reunion_code, hippodrome, url, reunion_slug, date_dir}]
    """
    url = DATE_URL_TPL.format(date=date_str)
    html = get_html(url)
    soup = BeautifulSoup(html, "lxml")

    # Dossier avec année/mois/date
    date_dir = get_date_directory(date_str)
    ensure_dir(date_dir)
    save_html_file(os.path.join(date_dir, f"{date_str}.html"), html)

    container = soup.select_one("div#list-reunion")
    if not container:
        return []

    reunions = []
    for tr in container.select("table.programme tbody tr.item"):
        # -> FR uniquement
        a = tr.select_one('td.numero a[data-tc-pays="FR"]')
        if not a:
            continue

        href = a.get("href", "").strip()
        if not href:
            continue
        reunion_url = urljoin(BASE, href)

        # reunion_code (R1...) depuis l'URL
        m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
        reunion_code = m.group(1) if m else (a.get_text(strip=True).replace("FR", "R"))

        # hippodrome
        hippo_el = tr.select_one("td.nom h2 span span")
        hippodrome = hippo_el.get_text(strip=True) if hippo_el else ""

        reunion_slug = f"{reunion_code}-{slugify(hippodrome)}"
        reunions.append({
            "date": date_str,
            "reunion_code": reunion_code,
            "hippodrome": hippodrome,
            "url": reunion_url,
            "reunion_slug": reunion_slug,
            "date_dir": date_dir,
        })

    return reunions

def parse_courses_from_reunion_page(reunion_url: str, reunion_dir: str, reunion_slug: str):
    """
    Récupère la liste des courses affichées dans la frise.
    Sauvegarde la page réunion en <reunion_slug>.html dans le dossier de la réunion.
    Retour: [{numero, code, heure, intitule, url}]
    """
    html = get_html(reunion_url)
    save_html_file(os.path.join(reunion_dir, f"{reunion_slug}.html"), html)

    soup = BeautifulSoup(html, "lxml")
    frise = soup.select_one("#frise-course .strip2.active") or soup.select_one("#frise-course .strip2")
    if not frise:
        return []

    courses = []
    for a in frise.select("ul.scroll-content li.scroll-element a"):
        href = a.get("href", "")
        if not href:
            continue
        url = urljoin(BASE, href)

        # numéro
        numero_txt = a.select_one("span.numero")
        numero_txt = numero_txt.get_text(strip=True) if numero_txt else ""
        mC = re.search(r"C(\d+)", href)
        numero = int(numero_txt) if numero_txt.isdigit() else (int(mC.group(1)) if mC else None)

        # "12h48 - Prix X"
        title = a.get("title", "").strip()
        heure, intitule = None, None
        if " - " in title:
            heure, intitule = title.split(" - ", 1)
        else:
            intitule = title or None

        code = f"C{numero}" if numero is not None else (mC.group(0) if mC else None)
        courses.append({
            "numero": numero,
            "code": code,
            "heure": heure,
            "intitule": intitule,
            "url": url,
        })

    if courses and all(c["numero"] is not None for c in courses):
        courses.sort(key=lambda x: x["numero"])
    return courses

def build_course_filename(reunion_code: str, course_code: str, intitule: str, url: str) -> str:
    """
    Construit le nom de fichier de la course (sans date).
    Ex: R1C1-prix-montgomery.html
    """
    slug = slugify(intitule) if intitule else ""
    if not slug:
        last = urlparse(url).path.rstrip("/").split("/")[-1]
        slug = slugify(last) or "course"
    code_part = f"{reunion_code}{(course_code or '').upper()}"
    return f"{code_part}-{slug}.html"

def download_course_page_to_file(course_url: str, dest_file: str):
    html = get_html(course_url)
    save_html_file(dest_file, html)
    return html

# =========================
# Orchestrateur range
# =========================
def scrape_range_only_fr(start_date: str, end_date: str, polite_delay=0.5):
    """
    - Itère de end_date -> start_date (desc)
    - FR uniquement (via data-tc-pays="FR" dans #list-reunion)
    - Fichiers organisés par année/mois:
        /content/drive/MyDrive/TURF/ZEturf/resultats-et-rapports/<YYYY>/<MM>/<DATE>/<DATE>.html
        /content/drive/MyDrive/TURF/ZEturf/resultats-et-rapports/<YYYY>/<MM>/<DATE>/<Rk-hippodrome>/<Rk-hippodrome>.html
        /content/drive/MyDrive/TURF/ZEturf/resultats-et-rapports/<YYYY>/<MM>/<DATE>/<Rk-hippodrome>/<RkCk>-<slug-intitule>.html
    - Crée un sommaire JSON lisible à la racine `summary.json`.
    """
    # Structure du sommaire
    summary = {}

    for date_str in date_range_desc(start_date, end_date):
        print(f"\n=== Date {date_str} — RÉUNIONS FR UNIQUEMENT ===")
        date_dir = get_date_directory(date_str)
        ensure_dir(date_dir)

        # Collecte par date dans le sommaire
        if date_str not in summary:
            summary[date_str] = {
                "date_file": os.path.join(date_dir, f"{date_str}.html"),
                "reunions": {}
            }

        # Récupère les réunions FR
        try:
            reunions = parse_reunions_fr_for_date(date_str)
        except Exception as e:
            print(f"  ! Erreur page date: {e}")
            reunions = []

        if not reunions:
            print("  Aucune réunion FR détectée.")
            continue

        for r in reunions:
            reunion_dir = os.path.join(date_dir, r["reunion_slug"])
            ensure_dir(reunion_dir)

            print(f"  {r['reunion_code']} {r['hippodrome']} -> {r['url']}")
            try:
                courses = parse_courses_from_reunion_page(
                    r["url"], reunion_dir, r["reunion_slug"]
                )
            except Exception as e:
                print(f"   ! Erreur lecture réunion : {e}")
                courses = []

            # Init sommaire réunion
            summary[date_str]["reunions"][r["reunion_slug"]] = {
                "reunion_file": os.path.join(reunion_dir, f"{r['reunion_slug']}.html"),
                "reunion_dir": reunion_dir,
                "courses": []
            }

            # Télécharge chaque course au format demandé
            for c in courses:
                filename = build_course_filename(
                    reunion_code=r["reunion_code"],
                    course_code=(c["code"] or "").upper(),
                    intitule=c["intitule"],
                    url=c["url"],
                )
                dest_file = os.path.join(reunion_dir, filename)
                try:
                    _ = download_course_page_to_file(c["url"], dest_file)
                    print(f"    - {filename} (OK)")
                    saved = True
                except Exception as e:
                    print(f"    - {filename} (ERREUR: {e})")
                    saved = False

                summary[date_str]["reunions"][r["reunion_slug"]]["courses"].append({
                    "label": os.path.splitext(filename)[0],  # p.ex. R1C1-prix-montgomery
                    "file": dest_file,
                    "url": c["url"],
                    "saved": saved
                })

                time.sleep(polite_delay)  # rester poli

            time.sleep(polite_delay)

    # Écrit le sommaire JSON à la racine
    ensure_dir(SAVE_ROOT)
    with open(os.path.join(SAVE_ROOT, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    return summary

# =========================
# Exemple d'exécution
# =========================
if __name__ == "__main__":
    # Exemple : du 2015-01-01 au 2025-11-10 (commence par 2025-11-09)
    summary = scrape_range_only_fr("2015-01-01", "2025-11-10", polite_delay=0.4)
    print("\nSommaire écrit dans: /content/drive/MyDrive/TURF/ZEturf/resultats-et-rapports/summary.json")
