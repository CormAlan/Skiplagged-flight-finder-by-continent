#!/usr/bin/env python3
"""
skiplagged_search_elite.py

Elite mode:
  - Builds destination IATA list from OurAirports + OpenFlights (route-degree hubness)
  - Then searches Skiplagged via mcporter with rate limit + parallelism + SQLite cache

Examples:
  # ARN -> Asia, next 10 days, polite rate limit
  python skiplagged_search_elite.py \
    --origin ARN --continent AS \
    --date-range 2026-03-15:2026-03-25 \
    --top-per-continent 220 --min-degree 15 --include-medium \
    --workers 6 --rps 2.0 --burst 4 \
    --cache-db cache.sqlite --ttl-hours 12 \
    --max-stops many --limit 5 --top 30 --verbose

  # Only build and export hubs (no searching)
  python skiplagged_search_elite.py --origin ARN --continent EU --export-hubs hubs_out --no-search

  # Use explicit destination list (no continent building)
  python skiplagged_search_elite.py --origin ARN --destinations HND,ICN,BKK --date 2026-03-20
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import sqlite3
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


# -------------------------
# MCP / Skiplagged
# -------------------------
MCP_BASE = "https://mcp.skiplagged.com/mcp"
MCP_TOOL = f"{MCP_BASE}.sk_flights_search"


# -------------------------
# Elite data sources
# -------------------------
OURAIRPORTS_AIRPORTS_CSV = "https://ourairports.com/airports.csv"
OPENFLIGHTS_AIRPORTS_DAT = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
OPENFLIGHTS_ROUTES_DAT = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"

CONTINENT_NAME = {
    "AF": "africa",
    "AS": "asia",
    "EU": "europe",
    "NA": "NA",
    "SA": "SA",
    "OC": "oceania",
    "AN": "antarctica",
}

# Weight helps prefer large airports even if degree ties; still mostly degree-driven.
TYPE_WEIGHT = {
    "large_airport": 1000,
    "medium_airport": 200,
    "small_airport": 0,
}


# -------------------------
# Rate limiter (token bucket)
# -------------------------
class TokenBucket:
    """
    Token bucket limiter:
    - rps: tokens added per second
    - burst: max tokens (bucket capacity)
    Each request consumes 1 token.
    """
    def __init__(self, rps: float, burst: int) -> None:
        self.rps = max(0.0, float(rps))
        self.capacity = max(1, int(burst))
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if self.rps <= 0:
            return  # unlimited

        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rps)

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                missing = 1.0 - self.tokens
                wait_s = missing / self.rps if self.rps > 0 else 0.0

            await asyncio.sleep(max(0.0, wait_s))


# -------------------------
# SQLite cache with TTL
# -------------------------
class SQLiteCache:
    def __init__(self, path: str) -> None:
        self.path = path
        self._init_db()

    def _init_db(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with sqlite3.connect(self.path) as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS cache (
                  key TEXT PRIMARY KEY,
                  created_at INTEGER NOT NULL,
                  payload TEXT NOT NULL
                )
                """
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_cache_created_at ON cache(created_at)")
            con.commit()

    @staticmethod
    def _now_ts() -> int:
        return int(time.time())

    def get(self, key: str, ttl_seconds: int) -> Optional[Dict[str, Any]]:
        cutoff = self._now_ts() - ttl_seconds
        with sqlite3.connect(self.path) as con:
            row = con.execute(
                "SELECT created_at, payload FROM cache WHERE key = ?",
                (key,),
            ).fetchone()
            if not row:
                return None
            created_at, payload = row
            if created_at < cutoff:
                return None
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                return None

    def set(self, key: str, payload: Dict[str, Any]) -> None:
        with sqlite3.connect(self.path) as con:
            con.execute(
                "INSERT OR REPLACE INTO cache(key, created_at, payload) VALUES (?, ?, ?)",
                (key, self._now_ts(), json.dumps(payload)),
            )
            con.commit()


# -------------------------
# Helpers
# -------------------------
def parse_iata_list(s: str) -> List[str]:
    items = [x.strip().upper() for x in s.split(",") if x.strip()]
    return list(dict.fromkeys(items))


def read_iata_file(path: str) -> List[str]:
    """
    Supports lines like:
      HND
      HND  # comment
      HND  # 123 Tokyo Haneda ...
    """
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            # keep only first token before whitespace or '#'
            token = raw.split("#", 1)[0].strip().split()[0].upper()
            if token:
                out.append(token)
    return list(dict.fromkeys(out))


def parse_date(s: str) -> date:
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def date_range(start: date, end: date) -> List[date]:
    if end < start:
        start, end = end, start
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def parse_date_range(s: str) -> List[date]:
    a, b = s.split(":", 1)
    return date_range(parse_date(a), parse_date(b))


def stable_key(parts: Dict[str, Any]) -> str:
    blob = json.dumps(parts, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


# -------------------------
# Elite hub builder
# -------------------------
@dataclass
class OurAirportRow:
    iata: str
    continent: str
    airport_type: str
    scheduled_service: str
    name: str
    iso_country: str


def download(url: str, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with urllib.request.urlopen(url) as r, open(path, "wb") as f:
        f.write(r.read())


def read_ourairports_airports_csv(path: str) -> Dict[str, OurAirportRow]:
    out: Dict[str, OurAirportRow] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            iata = (row.get("iata_code") or "").strip().upper()
            if not iata:
                continue
            continent = (row.get("continent") or "").strip().upper()
            airport_type = (row.get("type") or "").strip()
            scheduled = (row.get("scheduled_service") or "").strip().lower()
            name = (row.get("name") or "").strip()
            iso_country = (row.get("iso_country") or "").strip().upper()
            out[iata] = OurAirportRow(
                iata=iata,
                continent=continent,
                airport_type=airport_type,
                scheduled_service=scheduled,
                name=name,
                iso_country=iso_country,
            )
    return out


def parse_openflights_airports_dat(path: str) -> Dict[int, str]:
    id_to_iata: Dict[int, str] = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 5:
                continue
            try:
                airport_id = int(row[0])
            except ValueError:
                continue
            iata = (row[4] or "").strip().upper()
            if iata and iata != r"\N":
                id_to_iata[airport_id] = iata
    return id_to_iata


def compute_degree_from_routes(path: str) -> Dict[int, int]:
    deg: Dict[int, int] = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 6:
                continue
            src_id = row[3].strip()
            dst_id = row[5].strip()
            if not src_id.isdigit() or not dst_id.isdigit():
                continue
            s = int(src_id)
            d = int(dst_id)
            deg[s] = deg.get(s, 0) + 1
            deg[d] = deg.get(d, 0) + 1
    return deg


def airport_score(our: OurAirportRow, degree: int, include_medium: bool) -> Optional[int]:
    if our.continent not in CONTINENT_NAME:
        return None
    if our.scheduled_service != "yes":
        return None
    if our.airport_type == "large_airport":
        pass
    elif our.airport_type == "medium_airport":
        if not include_medium:
            return None
    else:
        return None
    return int(degree) + TYPE_WEIGHT.get(our.airport_type, 0)


def build_destinations_for_continent(
    continent: str,
    data_dir: str,
    top_n: int,
    min_degree: int,
    include_medium: bool,
    no_download: bool,
    export_path: Optional[str] = None,
    verbose: bool = False,
) -> List[str]:
    continent = continent.upper()
    if continent not in CONTINENT_NAME or continent == "AN":
        raise ValueError(f"Unsupported continent code: {continent} (use one of AF, AS, EU, NA, SA, OC)")

    os.makedirs(data_dir, exist_ok=True)
    our_path = os.path.join(data_dir, "ourairports_airports.csv")
    of_airports_path = os.path.join(data_dir, "openflights_airports.dat")
    of_routes_path = os.path.join(data_dir, "openflights_routes.dat")

    if not no_download:
        if verbose:
            print("[elite] downloading datasets...")
        download(OURAIRPORTS_AIRPORTS_CSV, our_path)
        download(OPENFLIGHTS_AIRPORTS_DAT, of_airports_path)
        download(OPENFLIGHTS_ROUTES_DAT, of_routes_path)
    else:
        for p in (our_path, of_airports_path, of_routes_path):
            if not os.path.exists(p):
                raise FileNotFoundError(f"--no-download set but missing file: {p}")

    if verbose:
        print("[elite] loading OurAirports...")
    our_by_iata = read_ourairports_airports_csv(our_path)

    if verbose:
        print("[elite] loading OpenFlights airports.dat...")
    id_to_iata = parse_openflights_airports_dat(of_airports_path)

    if verbose:
        print("[elite] computing degree from routes.dat...")
    degree_by_id = compute_degree_from_routes(of_routes_path)

    # Map IATA -> degree (max over IDs)
    degree_by_iata: Dict[str, int] = {}
    for airport_id, deg in degree_by_id.items():
        iata = id_to_iata.get(airport_id)
        if not iata:
            continue
        if iata not in degree_by_iata or deg > degree_by_iata[iata]:
            degree_by_iata[iata] = deg

    scored: List[Tuple[str, int, str]] = []
    for iata, our in our_by_iata.items():
        if our.continent != continent:
            continue
        deg = degree_by_iata.get(iata, 0)
        if deg < min_degree:
            continue
        sc = airport_score(our, deg, include_medium=include_medium)
        if sc is None:
            continue
        comment = f"{our.name} ({our.iso_country}) [{our.airport_type}] deg={deg}"
        scored.append((iata, sc, comment))

    scored.sort(key=lambda x: x[1], reverse=True)
    picked = scored[:top_n]
    destinations = [iata for iata, _, _ in picked]

    if export_path:
        os.makedirs(os.path.dirname(export_path) or ".", exist_ok=True)
        with open(export_path, "w", encoding="utf-8", newline="\n") as f:
            f.write("# Auto-generated hub list (IATA)\n")
            f.write("# Columns: IATA  # score  name/country/type/deg\n")
            for iata, sc, comment in picked:
                f.write(f"{iata}  # {sc}  {comment}\n")
        if verbose:
            print(f"[elite] exported hubs: {export_path}")

    if verbose:
        print(f"[elite] built {len(destinations)} destinations for {continent} ({CONTINENT_NAME[continent]})")

    return destinations


# -------------------------
# Skiplagged jobs
# -------------------------
@dataclass(frozen=True)
class SearchJob:
    origin: str
    dest: str
    departure_date: str
    limit: int
    max_stops: str


async def run_mcporter_search(job: SearchJob) -> Dict[str, Any]:
    cmd = [
        "mcporter",
        "call",
        MCP_TOOL,
        f"origin={job.origin}",
        f"destination={job.dest}",
        f"departureDate={job.departure_date}",
        "sort=price",
        f"limit={job.limit}",
        f"maxStops={job.max_stops}",
        "--output",
        "json",
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"mcporter failed (code={proc.returncode}) "
            f"dest={job.dest} date={job.departure_date}\n"
            f"stderr: {stderr.decode('utf-8', errors='replace')}"
        )
    text = stdout.decode("utf-8", errors="replace").strip()
    return json.loads(text) if text else {}


def extract_itineraries(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    for k in ("itineraries", "results", "data", "flights"):
        v = payload.get(k)
        if isinstance(v, list):
            return v
    return []


def normalize_hits(origin: str, dest: str, departure_date: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    hits = []
    for it in extract_itineraries(payload):
        if not isinstance(it, dict):
            continue
        price = it.get("price") or it.get("bestPrice") or it.get("amount")
        duration = it.get("duration") or it.get("totalDuration")
        url = it.get("bookingLink") or it.get("url") or it.get("deepLink")
        hits.append(
            {
                "origin": origin,
                "dest": dest,
                "date": departure_date,
                "price": price,
                "duration": duration,
                "url": url,
                "raw": it,
            }
        )
    return hits


async def worker(
    name: str,
    queue: "asyncio.Queue[SearchJob]",
    limiter: TokenBucket,
    cache: Optional[SQLiteCache],
    ttl_seconds: int,
    results: List[Dict[str, Any]],
    errors: List[str],
    verbose: bool,
) -> None:
    while True:
        job = await queue.get()
        try:
            cache_key = None
            if cache:
                cache_key = stable_key(
                    {
                        "tool": "sk_flights_search",
                        "origin": job.origin,
                        "dest": job.dest,
                        "departureDate": job.departure_date,
                        "limit": job.limit,
                        "maxStops": job.max_stops,
                        "sort": "price",
                    }
                )
                cached = cache.get(cache_key, ttl_seconds=ttl_seconds)
                if cached is not None:
                    if verbose:
                        print(f"[{name}] cache hit: {job.dest} {job.departure_date}")
                    results.extend(normalize_hits(job.origin, job.dest, job.departure_date, cached))
                    continue

            await limiter.acquire()
            if verbose:
                print(f"[{name}] requesting: {job.dest} {job.departure_date}")

            payload = await run_mcporter_search(job)
            if cache and cache_key:
                cache.set(cache_key, payload)

            results.extend(normalize_hits(job.origin, job.dest, job.departure_date, payload))

        except Exception as e:
            msg = f"{job.dest} {job.departure_date}: {e}"
            errors.append(msg)
            if verbose:
                print(f"[{name}] ERROR {msg}", file=sys.stderr)
        finally:
            queue.task_done()


def build_jobs(origin: str, destinations: List[str], dates: List[date], limit: int, max_stops: str) -> List[SearchJob]:
    jobs: List[SearchJob] = []
    for d in destinations:
        for dt in dates:
            jobs.append(
                SearchJob(
                    origin=origin,
                    dest=d,
                    departure_date=dt.strftime("%Y-%m-%d"),
                    limit=limit,
                    max_stops=max_stops,
                )
            )
    return jobs


def sort_hits(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def price_key(x: Dict[str, Any]) -> float:
        p = x.get("price")
        try:
            return float(p)
        except Exception:
            return float("inf")

    out = [h for h in hits if h.get("price") is not None]
    out.sort(key=price_key)
    return out


# -------------------------
# Main
# -------------------------
async def async_main(args: argparse.Namespace) -> int:
    origin = args.origin.upper()

    # Resolve destinations:
    destinations: List[str] = []
    used_mode = None

    if args.continent:
        used_mode = "continent"
        export_path = None
        if args.export_hubs:
            export_path = os.path.join(args.export_hubs, f"{CONTINENT_NAME[args.continent.upper()]}_hubs.txt")
        destinations = build_destinations_for_continent(
            continent=args.continent,
            data_dir=args.data_dir,
            top_n=args.top_per_continent,
            min_degree=args.min_degree,
            include_medium=args.include_medium,
            no_download=args.no_download,
            export_path=export_path,
            verbose=args.verbose,
        )
    else:
        # explicit destinations
        if args.destinations and args.destinations_file:
            print("Choose either --destinations OR --destinations-file, not both.", file=sys.stderr)
            return 2
        if args.destinations:
            used_mode = "destinations"
            destinations = parse_iata_list(args.destinations)
        elif args.destinations_file:
            used_mode = "destinations-file"
            destinations = read_iata_file(args.destinations_file)
        else:
            print("Provide --continent OR --destinations OR --destinations-file.", file=sys.stderr)
            return 2

    if not destinations:
        print("No destinations resolved. Try lowering --min-degree or increasing --top-per-continent.", file=sys.stderr)
        return 2

    # Dates
    if args.date and args.date_range:
        print("Choose either --date OR --date-range, not both.", file=sys.stderr)
        return 2
    if args.date:
        dates = [parse_date(args.date)]
    elif args.date_range:
        dates = parse_date_range(args.date_range)
    else:
        print("You must provide --date or --date-range.", file=sys.stderr)
        return 2

    if args.no_search:
        if args.verbose:
            print(f"[main] no-search enabled. Mode={used_mode}, destinations={len(destinations)}")
        return 0

    workers = max(1, int(args.workers))
    limiter = TokenBucket(rps=args.rps, burst=args.burst)

    cache = SQLiteCache(args.cache_db) if args.cache_db else None
    ttl_seconds = int(max(0, args.ttl_hours) * 3600)

    jobs = build_jobs(origin, destinations, dates, limit=args.limit, max_stops=args.max_stops)

    if args.verbose:
        print(f"[main] mode={used_mode} destinations={len(destinations)} dates={len(dates)} jobs={len(jobs)}")
        print(f"[main] workers={workers} rps={args.rps} burst={args.burst} cache={'on' if cache else 'off'} ttl_hours={args.ttl_hours}")

    queue: asyncio.Queue[SearchJob] = asyncio.Queue()
    for j in jobs:
        queue.put_nowait(j)

    results: List[Dict[str, Any]] = []
    errors: List[str] = []

    tasks = [
        asyncio.create_task(worker(f"w{i+1}", queue, limiter, cache, ttl_seconds, results, errors, args.verbose))
        for i in range(workers)
    ]

    await queue.join()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    sorted_hits = sort_hits(results)

    print("\n=== TOP RESULTS ===")
    for h in sorted_hits[: args.top]:
        print(
            f"{h['origin']} -> {h['dest']}  {h['date']}  price={h.get('price')}  "
            f"duration={h.get('duration')}  url={h.get('url')}"
        )

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(sorted_hits, f, ensure_ascii=False, indent=2)
        print(f"\nWrote JSON: {args.json_out}")

    if errors:
        print(f"\n=== ERRORS ({len(errors)}) ===", file=sys.stderr)
        for e in errors[:50]:
            print(e, file=sys.stderr)
        if len(errors) > 50:
            print(f"... +{len(errors)-50} more", file=sys.stderr)

    return 0


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Skiplagged search with elite continent hub-building + rate limiting + cache.")

    # search inputs
    p.add_argument("--origin", required=True, help="Origin IATA, e.g. ARN")
    p.add_argument("--date", help="Single departure date: YYYY-MM-DD")
    p.add_argument("--date-range", help="Inclusive date range: YYYY-MM-DD:YYYY-MM-DD")

    # destination modes
    p.add_argument("--continent", choices=["AF", "AS", "EU", "NA", "SA", "OC"], help="Elite mode: build destinations for a continent")
    p.add_argument("--destinations", help="Comma-separated IATA list, e.g. HND,ICN,BKK")
    p.add_argument("--destinations-file", help="Text file with one IATA per line (comments with # allowed).")

    # elite tuning
    p.add_argument("--data-dir", default="data_airports", help="Where to store downloaded datasets")
    p.add_argument("--top-per-continent", type=int, default=220, help="How many airports to keep for the chosen continent")
    p.add_argument("--min-degree", type=int, default=15, help="Minimum OpenFlights route-degree")
    p.add_argument("--include-medium", action="store_true", help="Include medium_airport airports (default: only large_airport)")
    p.add_argument("--no-download", action="store_true", help="Do not download datasets; expect files already in data-dir")
    p.add_argument("--export-hubs", help="Directory to export generated hub file for the continent")
    p.add_argument("--no-search", action="store_true", help="Only build/export destinations, do not call Skiplagged")

    # skiplagged params
    p.add_argument("--limit", type=int, default=5, help="Skiplagged limit per query")
    p.add_argument("--max-stops", default="many", choices=["none", "one", "many"], help="Max stops filter")
    p.add_argument("--top", type=int, default=25, help="How many results to print")

    # performance controls
    p.add_argument("--workers", type=int, default=6, help="Parallel workers (concurrent requests)")
    p.add_argument("--rps", type=float, default=2.0, help="Rate limit requests per second (0 = unlimited)")
    p.add_argument("--burst", type=int, default=4, help="Rate limiter burst capacity")

    # cache
    p.add_argument("--cache-db", default="cache.sqlite", help="SQLite cache path (set empty to disable)")
    p.add_argument("--ttl-hours", type=float, default=12.0, help="Cache TTL in hours")

    # output
    p.add_argument("--json-out", help="Write all hits to this JSON file")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")

    args = p.parse_args(argv)
    if args.cache_db == "":
        args.cache_db = None
    return args


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())