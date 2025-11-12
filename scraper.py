# -*- coding: utf-8 -*-
import os
import re
import json
import unicodedata
import asyncio
import aiohttp
import argparse
import subprocess
import random
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from pathlib import Path
from time import perf_counter
from zoneinfo import ZoneInfo
from collections import defaultdict

# =========================
# Config
# =========================
BASE = "https://www.zeturf.fr"
DATE_URL_TPL = BASE + "/fr/resultats-et-rapports/{date}"
REPO_ROOT = "resultats-et-rapports"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
}

# Concurrences
DISCOVERY_MAX_DATES = 16        # 16 dates en simultané
DISCOVERY_MAX_REUNIONS = 240    # 240 réunions en simultané (listing des courses)
SCRAPE_MAX_COURSES = 240        # 240 courses en simultané (HTML courses)

BATCH_HINT = (
    f"cfg:DISCOVERY_MAX_DATES={DISCOVERY_MAX_DATES},"
    f"DISCOVERY_MAX_REUNIONS={DISCOVERY_MAX_REUNIONS},"
    f"SCRAPE_MAX_COURSES={SCRAPE_MAX_COURSES}"
)

# Compteurs de rate-limit
RATE_LIMIT_STATS = defaultdict(int)

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

def file_exists(filepath: Path) -> bool:
    return filepath.exists() and filepath.stat().st_size > 0

def date_range_asc(start_date: str, end_date: str):
    d0 = datetime.strptime(start_date, "%Y-%m-%d").date()
    d1 = datetime.strptime(end_date, "%Y-%m-%d").date()
    if d0 > d1:
        d0, d1 = d1, d0
    days = (d1 - d0).days
    return [(d0 + timedelta(days=i)).isoformat() for i in range(days + 1)]

def group_by_year(dates):
    years = {}
    for date_str in dates:
        year = date_str[:4]
        if year not in years:
            years[year] = []
        years[year].append(date_str)
    return years

def manifest_path_for_date(date_str: str) -> Path:
    return get_date_directory(date_str) / "manifest.json"

def month_manifest_path(year: str, month: str) -> Path:
    return Path(REPO_ROOT) / year / month / "manifest.json"

def year_manifest_path(year: str) -> Path:
    return Path(REPO_ROOT) / year / "manifest.json"

def save_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def paris_yesterday_iso() -> str:
    # Fin = toujours la veille (Europe/Paris)
    today_paris = datetime.now(ZoneInfo("Europe/Paris")).date()
    return (today_paris - timedelta(days=1)).isoformat()

def _log_429(ctx: dict | None, attempt: int, headers: dict, url: str):
    # ctx: {"phase": "...", "resource": "...", ...}
    phase = (ctx or {}).get("phase", "?")
    resource = (ctx or {}).get("resource", "?")
    extra = {k: v for k, v in (ctx or {}).items() if k not in ("phase", "resource")}
    retry_after = headers.get("Retry-After")
    rl_lim = headers.get("X-RateLimit-Limit")
    rl_rem = headers.get("X-RateLimit-Remaining")
    server = headers.get("Server")
    cf_ray = headers.get("CF-RAY")
    RATE_LIMIT_STATS[f"{phase}:{resource}"] += 1
    print(
        f"[429] phase={phase} resource={resource} attempt={attempt+1} "
        f"retry_after={retry_after} rl_rem={rl_rem} rl_lim={rl_lim} "
        f"server={server} cf_ray={cf_ray} url={url} extra={extra} {BATCH_HINT}"
    )

# =========================
# HTTP
# =========================
async def fetch_html(session: aiohttp.ClientSession, url: str, retries=3, ctx: dict | None = None) -> str:
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    _log_429(ctx, attempt, resp.headers, url)
                    ra = resp.headers.get("Retry-After")
                    try:
                        wait = float(ra) if ra is not None else (2 ** attempt)
                    except ValueError:
                        wait = (2 ** attempt)
                    await asyncio.sleep(wait + random.random())
                    continue
                resp.raise_for_status()
                return await resp.text()
        except Exception:
            if attempt == retries - 1:
                raise
            await asyncio.sleep((2 ** attempt) + random.random())
    raise RuntimeError(f"fetch_html: exhausted retries for {url}")

# =========================
# Discovery (Étape 1)
# =========================
async def discover_date(session: aiohttp.ClientSession, date_str: str):
    """Retourne la liste des réunions FR (URL + méta) pour la date."""
    url = DATE_URL_TPL.format(date=date_str)
    html = await fetch_html(session, url, ctx={"phase": "discover", "resource": "date", "date": date_str})
    soup = BeautifulSoup(html, "lxml")
    container = soup.select_one("div#list-reunion")
    if not container:
        return []

    reunions = []
    for tr in container.select("table.programme tbody tr.item"):
        a = tr.select_one('td.numero a[data-tc-pays="FR"]')
        if not a:
            continue
        href = a.get("href", "").strip()
        if not href:
            continue

        reunion_url = urljoin(BASE, href)
        m = re.search(r"/reunion/\d{4}-\d{2}-\d{2}/(R\d+)-", href)
        reunion_code = m.group(1) if m else (a.get_text(strip=True).replace("FR", "R"))

        hippo_el = tr.select_one("td.nom h2 span span")
        hippodrome = hippo_el.get_text(strip=True) if hippo_el else ""
        reunion_slug = f"{reunion_code}-{slugify(hippodrome)}"

        reunions.append({
            "date": date_str,
            "reunion_code": reunion_code,
            "hippodrome": hippodrome,
            "url": reunion_url,
            "reunion_slug": reunion_slug,
        })
    return reunions

async def discover_reunion_courses(session: aiohttp.ClientSession, reunion: dict):
    """Télécharge la page réunion et retourne la liste des courses (url + filename)."""
    html = await fetch_html(session, reunion["url"], ctx={
        "phase": "discover", "resource": "reunion",
        "date": reunion.get("date"), "reunion": reunion.get("reunion_slug")
    })
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

        numero_txt = a.select_one("span.numero")
        numero_txt = numero_txt.get_text(strip=True) if numero_txt else ""
        mC = re.search(r"C(\d+)", href)
        numero = int(numero_txt) if numero_txt.isdigit() else (int(mC.group(1)) if mC else None)

        title = a.get("title", "").strip()
        if " - " in title:
            heure, intitule = title.split(" - ", 1)
        else:
            heure, intitule = None, (title or None)
        code = f"C{numero}" if numero is not None else (mC.group(0) if mC else None)

        slug = slugify(intitule) if intitule else "course"
        code_part = f"{reunion['reunion_code']}{(code or '').upper()}"
        filename = f"{code_part}-{slug}.html"

        courses.append({
            "url": url,
            "filename": filename,
            "code": code or "",
            "title": intitule or "",
            "heure": heure or "",
        })
    return courses

async def discovery(start_date: str, end_date: str):
    """Étape 1: découvre toutes les réunions FR et liste leurs courses, puis écrit des manifestes JSON."""
    all_dates = date_range_asc(start_date, end_date)
    years = group_by_year(all_dates)

    # Agrégats pour manifestes year/month
    year_summary = defaultdict(lambda: {"months": defaultdict(lambda: {"dates": [], "reunions": 0, "courses": 0}),
                                        "reunions": 0, "courses": 0})

    print(f"[DISCOVERY] Période: {start_date} → {end_date} ({len(all_dates)} jours)")
    t0 = perf_counter()

    connector = aiohttp.TCPConnector(limit=DISCOVERY_MAX_DATES + DISCOVERY_MAX_REUNIONS, limit_per_host=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, len(all_dates), DISCOVERY_MAX_DATES):
            date_batch = all_dates[i:i + DISCOVERY_MAX_DATES]
            print(f"  [DATES] Batch: {', '.join(date_batch)}")

            date_tasks = [discover_date(session, d) for d in date_batch]
            date_results = await asyncio.gather(*date_tasks, return_exceptions=True)

            reunions = []
            for d, res in zip(date_batch, date_results):
                if isinstance(res, Exception):
                    print(f"    ! Error date {d}: {res}")
                    continue
                reunions.extend(res)

            if not reunions:
                continue

            print(f"    [REUNIONS] {len(reunions)} FR → découverte des courses (<= {DISCOVERY_MAX_REUNIONS} simultanées)")
            # Manifeste par date (agrégé)
            all_date_manifests = defaultdict(lambda: {"date": "", "reunions": []})

            for j in range(0, len(reunions), DISCOVERY_MAX_REUNIONS):
                sub = reunions[j:j + DISCOVERY_MAX_REUNIONS]
                tasks = [discover_reunion_courses(session, r) for r in sub]
                courses_lists = await asyncio.gather(*tasks, return_exceptions=True)

                for reunion, courses in zip(sub, courses_lists):
                    if isinstance(courses, Exception):
                        print(f"      ! Error reunion {reunion.get('reunion_slug')}: {courses}")
                        continue
                    dstr = reunion["date"]
                    year = dstr[:4]; month = dstr[5:7]
                    m = all_date_manifests[dstr]
                    m["date"] = dstr
                    m["reunions"].append({
                        "reunion_code": reunion["reunion_code"],
                        "hippodrome": reunion["hippodrome"],
                        "reunion_slug": reunion["reunion_slug"],
                        "url": reunion["url"],
                        "courses": courses,
                    })
                    # Agrégats
                    if dstr not in year_summary[year]["months"][month]["dates"]:
                        year_summary[year]["months"][month]["dates"].append(dstr)
                    year_summary[year]["months"][month]["reunions"] += 1
                    year_summary[year]["months"][month]["courses"] += len(courses)
                    year_summary[year]["reunions"] += 1
                    year_summary[year]["courses"] += len(courses)

            # Écrit les manifestes "par date"
            for dstr, payload in all_date_manifests.items():
                path = manifest_path_for_date(dstr)
                save_json(path, payload)
                print(f"      [+] manifest {dstr} → {path} "
                      f"(reunions={len(payload['reunions'])}, "
                      f"courses={sum(len(r['courses']) for r in payload['reunions'])})")

    # Écrit les manifestes "mois" et "année"
    for y, ydata in year_summary.items():
        ym_path = year_manifest_path(y)
        ym_payload = {
            "year": y,
            "reunions": ydata["reunions"],
            "courses": ydata["courses"],
            "months": {m: {"reunions": ydata["months"][m]["reunions"],
                           "courses": ydata["months"][m]["courses"],
                           "dates": sorted(ydata["months"][m]["dates"])}
                       for m in sorted(ydata["months"].keys())}
        }
        save_json(ym_path, ym_payload)

        for m, mdata in ydata["months"].items():
            mm_path = month_manifest_path(y, m)
            mm_payload = {"year": y, "month": m, "reunions": mdata["reunions"],
                          "courses": mdata["courses"], "dates": sorted(mdata["dates"])}
            save_json(mm_path, mm_payload)

    elapsed = perf_counter() - t0
    print(f"[DISCOVERY] Terminé en {elapsed/60:.2f} min")

    # Récap 429 pour discovery
    d_date = RATE_LIMIT_STATS.get("discover:date", 0)
    d_reun = RATE_LIMIT_STATS.get("discover:reunion", 0)
    print(f"[DISCOVERY] 429 summary → date={d_date}, reunion={d_reun}")
    RATE_LIMIT_STATS["discover:date"] = 0
    RATE_LIMIT_STATS["discover:reunion"] = 0

# =========================
# Scraping (Étape 2)
# =========================
async def scrape_course(session: aiohttp.ClientSession, course: dict, reunion_dir: Path):
    filepath = reunion_dir / course["filename"]
    if file_exists(filepath):
        return "SKIP", course["filename"]
    try:
        html = await fetch_html(session, course["url"], ctx={
            "phase": "scrape", "resource": "course",
            "date": reunion_dir.parent.name,      # YYYY-MM-DD
            "reunion": reunion_dir.name,          # Rn-slug
            "filename": course["filename"]
        })
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(html, encoding="utf-8")
        return "OK", course["filename"]
    except Exception as e:
        return "ERR", f"{course['filename']} - {e}"

def git_commit_push_year(year: str):
    print(f"\n{'='*60}\nGit commit & push pour l'année {year}\n{'='*60}\n")
    try:
        changed = subprocess.run(
            ["git", "status", "--porcelain", f"{REPO_ROOT}/{year}"],
            check=True, capture_output=True, text=True
        ).stdout.strip()
        if not changed:
            print(f"(aucun changement pour {year})\n")
            return
        subprocess.run(["git", "config", "user.name", "GitHub Actions"], check=True)
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "add", f"{REPO_ROOT}/{year}"], check=True)
        subprocess.run(["git", "commit", "-m", f"Add ZEturf data for year {year}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"✓ Année {year} committée et pushée avec succès\n")
    except subprocess.CalledProcessError as e:
        print(f"✗ Erreur Git: {e}\n")

async def scrape_from_manifests(start_date: str, end_date: str):
    """Lit tous les manifestes de la période, calcule les courses manquantes et les scrape (<=240). Commit par année."""
    all_dates = date_range_asc(start_date, end_date)
    years = group_by_year(all_dates)

    print(f"[SCRAPE] Période: {start_date} → {end_date} ({len(all_dates)} jours)")
    t_all = perf_counter()

    connector = aiohttp.TCPConnector(limit=SCRAPE_MAX_COURSES, limit_per_host=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        for year in sorted(years.keys()):
            t_year = perf_counter()
            dates = years[year]

            # Construire la todo list depuis les manifestes
            todo_courses = []
            for d in dates:
                mp = manifest_path_for_date(d)
                if not mp.exists():
                    continue
                payload = json.loads(mp.read_text(encoding="utf-8"))
                date_dir = get_date_directory(d)
                for r in payload.get("reunions", []):
                    reunion_dir = date_dir / r["reunion_slug"]
                    for c in r.get("courses", []):
                        filepath = reunion_dir / c["filename"]
                        if not file_exists(filepath):
                            todo_courses.append((d, r["reunion_slug"], c, reunion_dir))

            if not todo_courses:
                print(f"[SCRAPE] Année {year} : rien à faire (déjà complet)")
                continue

            print(f"[SCRAPE] Année {year} : {len(todo_courses)} courses à télécharger "
                  f"(limite {SCRAPE_MAX_COURSES} simultanées)")

            ok = skip = err = 0
            for i in range(0, len(todo_courses), SCRAPE_MAX_COURSES):
                batch = todo_courses[i:i + SCRAPE_MAX_COURSES]
                tasks = [scrape_course(session, c, reunion_dir) for (_, _, c, reunion_dir) in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in results:
                    if isinstance(res, Exception):
                        err += 1
                        print(f"    ! ERROR batch: {res}")
                    else:
                        status, info = res
                        if status == "OK":
                            ok += 1
                            print(f"      OK: {info}")
                        elif status == "SKIP":
                            skip += 1
                        else:
                            err += 1
                            print(f"      ERROR: {info}")

            elapsed = perf_counter() - t_year
            print(f"[SCRAPE] Année {year} terminée en {elapsed/60:.2f} min "
                  f"(new={ok}, skip={skip}, err={err})")

            # Commit par année
            git_commit_push_year(year)

    print(f"[SCRAPE] Terminé en {(perf_counter()-t_all)/60:.2f} min")

    # Récap 429 pour scrape
    s_course = RATE_LIMIT_STATS.get("scrape:course", 0)
    print(f"[SCRAPE] 429 summary → course={s_course}")
    RATE_LIMIT_STATS["scrape:course"] = 0

# =========================
# CLI
# =========================
def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("discover", help="Étape 1: découvrir réunions FR et lister les courses; manifestes JSON.")
    p1.add_argument("--start", default="2005-04-27")
    p1.add_argument("--end",   help="(ignoré) fin = veille (Europe/Paris)")

    p2 = sub.add_parser("scrape", help="Étape 2: lire manifestes et scraper les courses manquantes; commit par année.")
    p2.add_argument("--start", default="2005-04-27")
    p2.add_argument("--end",   help="(ignoré) fin = veille (Europe/Paris)")

    args = parser.parse_args()
    end_date = paris_yesterday_iso()  # fin = J-1 Europe/Paris

    if args.cmd == "discover":
        asyncio.run(discovery(args.start, end_date))
    elif args.cmd == "scrape":
        asyncio.run(scrape_from_manifests(args.start, end_date))

if __name__ == "__main__":
    main()
