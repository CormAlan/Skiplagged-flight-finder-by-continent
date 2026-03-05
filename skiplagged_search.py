import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from urllib.parse import urlparse

OURAIRPORTS_AIRPORTS_CSV = "https://ourairports.com/airports.csv"
OPENFLIGHTS_AIRPORTS_DAT = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
OPENFLIGHTS_ROUTES_DAT = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat"

CONTINENT_NAME = {
    "AF": "africa",
    "AS": "asia",
    "EU": "europe",
    "NA": "na",
    "SA": "sa",
    "OC": "oceania",
    "AN": "antarctica",
}

TYPE_WEIGHT = {
    "large_airport": 1000,
    "medium_airport": 200,
    "small_airport": 0,
}

CONTINENT_IATA_OVERRIDE = {
    "IST": "EU",
    "SAW": "EU",
}


def stable_key(obj):
    b = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def parse_iata_list(s):
    items = [x.strip().upper() for x in (s or "").split(",") if x.strip()]
    out = []
    seen = set()
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def read_iata_file(path):
    out = []
    seen = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            raw = raw.split("#", 1)[0].strip()
            if not raw:
                continue
            code = raw.split()[0].strip().upper()
            if code and code not in seen:
                out.append(code)
                seen.add(code)
    return out


def parse_date(s):
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def date_range(a, b):
    if b < a:
        a, b = b, a
    n = (b - a).days
    return [a + timedelta(days=i) for i in range(n + 1)]


def parse_date_range(s):
    a, b = s.split(":", 1)
    return date_range(parse_date(a), parse_date(b))


class TokenBucket:
    def __init__(self, rps, burst):
        self.rps = float(rps)
        self.cap = int(burst) if int(burst) > 0 else 1
        self.tokens = float(self.cap)
        self.t0 = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        if self.rps <= 0:
            return
        while True:
            async with self.lock:
                now = time.monotonic()
                dt = now - self.t0
                self.t0 = now
                self.tokens = min(self.cap, self.tokens + dt * self.rps)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                missing = 1.0 - self.tokens
                wait_s = missing / self.rps if self.rps > 0 else 0.25
            await asyncio.sleep(max(0.0, wait_s))


class SQLiteCache:
    def __init__(self, path):
        self.path = path
        self._init()

    def _init(self):
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with sqlite3.connect(self.path) as con:
            con.execute(
                "CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, created_at INTEGER NOT NULL, payload TEXT NOT NULL)"
            )
            con.execute("CREATE INDEX IF NOT EXISTS idx_cache_created_at ON cache(created_at)")
            con.commit()

    def _now(self):
        return int(time.time())

    def get(self, key, ttl_seconds):
        cutoff = self._now() - int(ttl_seconds)
        with sqlite3.connect(self.path) as con:
            row = con.execute("SELECT created_at, payload FROM cache WHERE key=?", (key,)).fetchone()
            if not row:
                return None
            created_at, payload = row
            if int(created_at) < cutoff:
                return None
            try:
                return json.loads(payload)
            except Exception:
                return None

    def set(self, key, payload):
        with sqlite3.connect(self.path) as con:
            con.execute(
                "INSERT OR REPLACE INTO cache(key, created_at, payload) VALUES (?, ?, ?)",
                (key, self._now(), json.dumps(payload)),
            )
            con.commit()


def download(url, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with urllib.request.urlopen(url) as r, open(path, "wb") as f:
        f.write(r.read())


def read_ourairports_airports_csv(path):
    out = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            iata = (row.get("iata_code") or "").strip().upper()
            if not iata:
                continue
            out[iata] = {
                "iata": iata,
                "continent": (row.get("continent") or "").strip().upper(),
                "type": (row.get("type") or "").strip(),
                "scheduled_service": (row.get("scheduled_service") or "").strip().lower(),
                "name": (row.get("name") or "").strip(),
                "iso_country": (row.get("iso_country") or "").strip().upper(),
            }
    return out


def parse_openflights_airports_dat(path):
    id_to_iata = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 5:
                continue
            try:
                airport_id = int(row[0])
            except Exception:
                continue
            iata = (row[4] or "").strip().upper()
            if iata and iata != r"\N":
                id_to_iata[airport_id] = iata
    return id_to_iata


def compute_degree_from_routes(path):
    deg = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 6:
                continue
            s = row[3].strip()
            d = row[5].strip()
            if not s.isdigit() or not d.isdigit():
                continue
            s = int(s)
            d = int(d)
            deg[s] = deg.get(s, 0) + 1
            deg[d] = deg.get(d, 0) + 1
    return deg


def airport_ok(our, include_medium):
    if our["continent"] not in CONTINENT_NAME:
        return False
    if our["scheduled_service"] in ("no", "n", "false", "0"):
        return False
    t = our["type"]
    if t == "large_airport":
        return True
    if t == "medium_airport":
        return bool(include_medium)
    return False


def build_destinations_for_continent(continent, data_dir, top_n, min_degree, include_medium, no_download, export_path, verbose):
    continent = continent.upper()
    if continent not in CONTINENT_NAME or continent == "AN":
        raise ValueError("bad continent")

    os.makedirs(data_dir, exist_ok=True)
    our_path = os.path.join(data_dir, "ourairports_airports.csv")
    of_airports_path = os.path.join(data_dir, "openflights_airports.dat")
    of_routes_path = os.path.join(data_dir, "openflights_routes.dat")

    if not no_download:
        if verbose:
            print("[elite] download")
        download(OURAIRPORTS_AIRPORTS_CSV, our_path)
        download(OPENFLIGHTS_AIRPORTS_DAT, of_airports_path)
        download(OPENFLIGHTS_ROUTES_DAT, of_routes_path)
    else:
        for p in (our_path, of_airports_path, of_routes_path):
            if not os.path.exists(p):
                raise FileNotFoundError(p)

    our_by_iata = read_ourairports_airports_csv(our_path)
    id_to_iata = parse_openflights_airports_dat(of_airports_path)
    deg_by_id = compute_degree_from_routes(of_routes_path)

    deg_by_iata = {}
    for airport_id, deg in deg_by_id.items():
        iata = id_to_iata.get(airport_id)
        if not iata:
            continue
        if iata not in deg_by_iata or deg > deg_by_iata[iata]:
            deg_by_iata[iata] = deg

    scored = []
    for iata, our in our_by_iata.items():
        eff_cont = CONTINENT_IATA_OVERRIDE.get(iata, our["continent"])
        if eff_cont != continent:
            continue
        if not airport_ok(our, include_medium):
            continue
        deg = int(deg_by_iata.get(iata, 0))
        if deg < int(min_degree):
            continue
        score = deg + TYPE_WEIGHT.get(our["type"], 0)
        comment = "{} ({}) [{}] deg={}".format(our["name"], our["iso_country"], our["type"], deg)
        scored.append((iata, score, comment))

    scored.sort(key=lambda x: x[1], reverse=True)
    picked = scored[: int(top_n)]
    destinations = [x[0] for x in picked]

    if export_path:
        os.makedirs(os.path.dirname(export_path) or ".", exist_ok=True)
        with open(export_path, "w", encoding="utf-8", newline="\n") as f:
            f.write("# iata  # score  comment\n")
            for iata, sc, c in picked:
                f.write("{}  # {}  {}\n".format(iata, sc, c))
        if verbose:
            print("[elite] exported {}".format(export_path))

    if verbose:
        print("[elite] destinations={}".format(len(destinations)))
    return destinations


def _decode_mcporter_markdown(js_out):
    if not js_out:
        return ""
    tpos = js_out.find("text:")
    if tpos == -1:
        return ""
    i = tpos + len("text:")
    while i < len(js_out) and js_out[i].isspace():
        i += 1
    end = js_out.find("structuredContent:", i)
    if end == -1:
        end = len(js_out)
    expr = js_out[i:end]
    parts = re.findall(r"'((?:\\.|[^'\\])*)'", expr)
    if not parts:
        return ""
    out = []
    for p in parts:
        out.append(bytes(p, "utf-8").decode("unicode_escape", errors="replace"))
    return "".join(out)


def extract_itineraries(payload):
    js_out = ""

    if isinstance(payload, dict):
        mcpt = payload.get("mcporter_text")
        if isinstance(mcpt, str) and mcpt.strip():
            js_out = mcpt
        else:
            for k in ("text", "stdout", "raw", "output", "message"):
                v = payload.get(k)
                if isinstance(v, str) and v.strip():
                    js_out = v
                    break
        if not js_out:
            js_out = str(payload)
    elif isinstance(payload, str):
        js_out = payload
    else:
        js_out = str(payload)

    if "\n|" in js_out and "|price|" in js_out.lower():
        md = js_out
    else:
        md = _decode_mcporter_markdown(js_out)

    if not md:
        return []

    lines = [ln.rstrip("\n") for ln in md.splitlines() if ln.strip()]
    if not lines:
        return []

    def split_row(row):
        r = row.strip()
        if r.startswith("|"):
            r = r[1:]
        if r.endswith("|"):
            r = r[:-1]
        return [c.strip() for c in r.split("|")]

    header_idx = None
    header_cols = None
    for i, ln in enumerate(lines):
        if not ln.lstrip().startswith("|"):
            continue
        cols = split_row(ln)
        cols_l = [c.lower().strip() for c in cols]
        if any("price" in c for c in cols_l):
            header_idx = i
            header_cols = cols_l
            break

    if header_idx is None:
        return []

    def find_col(pred):
        for idx, c in enumerate(header_cols):
            if pred(c):
                return idx
        return None

    idx_price = find_col(lambda c: "price" in c or "cost" in c or "fare" in c)
    idx_duration = find_col(lambda c: "duration" in c or "time" in c or "travel" in c)
    idx_stops = find_col(lambda c: "stops" in c or "stop" in c or "layover" in c)
    idx_link = find_col(lambda c: "booking" in c or "link" in c or "book" in c or "url" in c)

    if idx_price is None:
        return []

    start = header_idx + 2 if header_idx + 2 < len(lines) else header_idx + 1

    def extract_url(cell):
        m = re.search(r"\((https?://[^)]+)\)", cell)
        if m:
            return m.group(1)
        m2 = re.search(r"(https?://\S+)", cell)
        if m2:
            return m2.group(1).rstrip(").,")
        return None

    out = []
    for ln in lines[start:]:
        if not ln.lstrip().startswith("|"):
            break
        cols = split_row(ln)
        if len(cols) <= idx_price:
            continue

        price_cell = cols[idx_price]
        dur_cell = cols[idx_duration] if (idx_duration is not None and idx_duration < len(cols)) else None
        stops_cell = cols[idx_stops] if (idx_stops is not None and idx_stops < len(cols)) else None

        url = None
        if idx_link is not None and idx_link < len(cols):
            url = extract_url(cols[idx_link])
        if not url:
            for c in cols:
                url = extract_url(c)
                if url:
                    break

        m_price = re.search(r"\$([0-9][0-9,]*(?:\.[0-9]+)?)", price_cell)
        if not m_price:
            continue
        price = float(m_price.group(1).replace(",", ""))

        out.append({"price": price, "duration": dur_cell, "stops": stops_cell, "url": url})

    return out


def _min_priced_itinerary(itins):
    best = None
    best_price = float("inf")
    for it in itins:
        p = it.get("price")
        if p is None:
            continue
        try:
            pf = float(p)
        except Exception:
            continue
        if pf < best_price:
            best_price = pf
            best = it
    return best


def _url_matches_route(url, origin, dest):
    if not url:
        return True
    try:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        if len(parts) < 3:
            return True
        if parts[0] != "flights":
            return True
        return parts[1].upper() == origin.upper() and parts[2].upper() == dest.upper()
    except Exception:
        return True


async def run_mcporter_search(job, npx_path):
    tool_url = "https://mcp.skiplagged.com/mcp.sk_flights_search"

    args_obj = {
        "origin": job["origin"],
        "destination": job["dest"],
        "departureDate": job["departure_date"],
        "sort": "price",
        "limit": int(job["limit"]),
        "maxStops": job["max_stops"],
        "offset": 0,
    }
    if job.get("return_date"):
        args_obj["returnDate"] = job["return_date"]

    cmd = [
        npx_path,
        "--yes",
        "mcporter",
        "call",
        tool_url,
        "--args",
        json.dumps(args_obj, separators=(",", ":")),
        "--raw",
        "--output",
        "json",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    out_text = stdout.decode("utf-8", errors="replace")
    err_text = stderr.decode("utf-8", errors="replace")

    if proc.returncode != 0:
        raise RuntimeError("mcporter failed: {}".format(err_text[:300]))

    return {"mcporter_text": out_text, "mcporter_stderr": err_text}


def normalize_hits(origin, dest, departure_date, return_date, payload):
    hits = []
    for it in extract_itineraries(payload):
        url = it.get("url")
        if not _url_matches_route(url, origin, dest):
            continue
        hits.append(
            {
                "origin": origin,
                "dest": dest,
                "departure_date": departure_date,
                "return_date": return_date,
                "price": it.get("price"),
                "duration": it.get("duration"),
                "stops": it.get("stops"),
                "url": url,
            }
        )
    return hits


def build_jobs(origin, destinations, dep_return_pairs, limit, max_stops):
    jobs = []
    for d in destinations:
        for dep, ret in dep_return_pairs:
            jobs.append(
                {
                    "origin": origin,
                    "dest": d,
                    "departure_date": dep,
                    "return_date": ret,
                    "limit": int(limit),
                    "max_stops": max_stops,
                }
            )
    return jobs


def sort_hits(hits):
    out = [h for h in hits if h.get("price") is not None]
    out.sort(key=lambda x: float(x.get("price", 1e18)))
    return out


async def worker(name, queue, limiter, cache, ttl_seconds, results, errors, verbose, npx_path):
    while True:
        job = await queue.get()
        try:
            async def fetch(j):
                cache_key = None
                if cache:
                    cache_key = stable_key(
                        {
                            "tool": "sk_flights_search",
                            "origin": j["origin"],
                            "dest": j["dest"],
                            "departureDate": j["departure_date"],
                            "returnDate": j.get("return_date"),
                            "limit": j["limit"],
                            "maxStops": j["max_stops"],
                            "sort": "price",
                        }
                    )
                    cached = cache.get(cache_key, ttl_seconds)
                    if cached is not None:
                        if verbose:
                            print("[{}] cache {}".format(name, j["dest"]))
                        return cached

                last_err = None
                for attempt in range(3):
                    await limiter.acquire()
                    if verbose:
                        print("[{}] req {} {}".format(name, j["dest"], j["departure_date"]))
                    try:
                        payload = await run_mcporter_search(j, npx_path)
                    except Exception as e:
                        last_err = str(e)
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue

                    if cache and cache_key:
                        cache.set(cache_key, payload)
                    return payload

                raise RuntimeError(last_err or "fetch failed")

            # round-trip: sum min one-way out + min one-way back
            if job.get("return_date"):
                j_out = dict(job)
                j_out["return_date"] = None
                j_back = {
                    "origin": job["dest"],
                    "dest": job["origin"],
                    "departure_date": job["return_date"],
                    "return_date": None,
                    "limit": job["limit"],
                    "max_stops": job["max_stops"],
                }

                payload_out = await fetch(j_out)
                payload_back = await fetch(j_back)

                it_out = _min_priced_itinerary(extract_itineraries(payload_out))
                it_back = _min_priced_itinerary(extract_itineraries(payload_back))
                if not it_out or not it_back:
                    queue.task_done()
                    continue

                total = float(it_out["price"]) + float(it_back["price"])
                results.append(
                    {
                        "origin": job["origin"],
                        "dest": job["dest"],
                        "departure_date": job["departure_date"],
                        "return_date": job["return_date"],
                        "price": total,
                        "price_outbound": float(it_out["price"]),
                        "price_return": float(it_back["price"]),
                        "duration": "Outbound: {} / Return: {}".format(it_out.get("duration"), it_back.get("duration")),
                        "stops": "Outbound: {} / Return: {}".format(it_out.get("stops"), it_back.get("stops")),
                        "url_outbound": it_out.get("url"),
                        "url_return": it_back.get("url"),
                    }
                )
                queue.task_done()
                continue

            payload = await fetch(job)
            results.extend(
                normalize_hits(job["origin"], job["dest"], job["departure_date"], job.get("return_date"), payload)
            )

        except Exception as e:
            errors.append("{} {}: {}".format(job.get("dest"), job.get("departure_date"), str(e)))
            if verbose:
                print("[{}] err {}".format(name, str(e)[:200]))
        finally:
            queue.task_done()


async def async_main(args):
    origin = args.origin.upper()

    # destinations
    destinations = []
    used_mode = None

    if args.continent:
        used_mode = "continent"
        export_path = None
        if args.export_hubs:
            export_path = os.path.join(args.export_hubs, "{}_hubs.txt".format(CONTINENT_NAME[args.continent.upper()]))
        destinations = build_destinations_for_continent(
            args.continent,
            args.data_dir,
            args.top_per_continent,
            args.min_degree,
            args.include_medium,
            args.no_download,
            export_path,
            args.verbose,
        )
    else:
        if args.destinations and args.destinations_file:
            print("choose one: --destinations or --destinations-file", file=sys.stderr)
            return 2
        if args.destinations:
            used_mode = "destinations"
            destinations = parse_iata_list(args.destinations)
        elif args.destinations_file:
            used_mode = "destinations-file"
            destinations = read_iata_file(args.destinations_file)
        else:
            print("need --continent or --destinations or --destinations-file", file=sys.stderr)
            return 2

    if not destinations:
        print("no destinations", file=sys.stderr)
        return 2

    # dates
    if args.date and args.date_range:
        print("choose one: --date or --date-range", file=sys.stderr)
        return 2
    if args.date:
        dep_dates = [parse_date(args.date)]
    elif args.date_range:
        dep_dates = parse_date_range(args.date_range)
    else:
        print("need --date or --date-range", file=sys.stderr)
        return 2

    # return pairs
    if args.return_date and args.return_date_range:
        print("choose one: --return-date or --return-date-range", file=sys.stderr)
        return 2

    dep_return_pairs = []
    if args.return_date:
        r = parse_date(args.return_date)
        for d in dep_dates:
            if r < d:
                continue
            if (r - d).days > int(args.max_return_span):
                continue
            dep_return_pairs.append((d.strftime("%Y-%m-%d"), r.strftime("%Y-%m-%d")))
    elif args.return_date_range:
        ret_dates = parse_date_range(args.return_date_range)
        for d in dep_dates:
            for r in ret_dates:
                if r < d:
                    continue
                if (r - d).days > int(args.max_return_span):
                    continue
                dep_return_pairs.append((d.strftime("%Y-%m-%d"), r.strftime("%Y-%m-%d")))
    else:
        for d in dep_dates:
            dep_return_pairs.append((d.strftime("%Y-%m-%d"), None))

    if args.no_search:
        if args.verbose:
            print("[main] no-search mode={}".format(used_mode))
        return 0

    jobs = build_jobs(origin, destinations, dep_return_pairs, args.limit, args.max_stops)

    workers = max(1, int(args.workers))
    limiter = TokenBucket(args.rps, args.burst)

    cache = SQLiteCache(args.cache_db) if args.cache_db else None
    ttl_seconds = int(max(0.0, float(args.ttl_hours)) * 3600)

    if args.verbose:
        print("[main] mode={} dest={} pairs={} jobs={}".format(used_mode, len(destinations), len(dep_return_pairs), len(jobs)))
        print("[main] workers={} rps={} burst={} cache={} ttl_h={}".format(workers, args.rps, args.burst, "on" if cache else "off", args.ttl_hours))

    queue = asyncio.Queue()
    for j in jobs:
        queue.put_nowait(j)

    results = []
    errors = []

    tasks = []
    for i in range(workers):
        tasks.append(asyncio.create_task(worker("w{}".format(i + 1), queue, limiter, cache, ttl_seconds, results, errors, args.verbose, args.npx_path)))

    await queue.join()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    hits = sort_hits(results)

    print("\n=== TOP RESULTS ===")
    for h in hits[: int(args.top)]:
        o = h.get("origin")
        d = h.get("dest")
        dep = h.get("departure_date")
        ret = h.get("return_date")
        price = h.get("price")

        if ret and h.get("price_outbound") is not None and h.get("price_return") is not None:
            price_str = "{:.0f} ({:.0f}+{:.0f})".format(float(price), float(h["price_outbound"]), float(h["price_return"]))
        else:
            price_str = "{:.0f}".format(float(price)) if price is not None else "None"

        print("{} -> {}  {} -> {}  price={}  duration={}  stops={}".format(o, d, dep, ret, price_str, h.get("duration"), h.get("stops")))
        if h.get("url_outbound"):
            print("  outbound: {}".format(h.get("url_outbound")))
        if h.get("url_return"):
            print("  return:   {}".format(h.get("url_return")))
        if h.get("url") and not h.get("url_outbound"):
            print("  url:      {}".format(h.get("url")))

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(hits, f, ensure_ascii=False, indent=2)
        print("\nWrote JSON: {}".format(args.json_out))

    if errors:
        print("\n=== ERRORS ({}) ===".format(len(errors)), file=sys.stderr)
        for e in errors[:50]:
            print(e, file=sys.stderr)
        if len(errors) > 50:
            print("... +{} more".format(len(errors) - 50), file=sys.stderr)

    return 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Skiplagged search (simple).")
    p.add_argument("--npx-path", default="npx", help="path to npx (windows: npx.cmd)")
    p.add_argument("--origin", required=True)
    p.add_argument("--date", help="YYYY-MM-DD")
    p.add_argument("--date-range", help="YYYY-MM-DD:YYYY-MM-DD")
    p.add_argument("--return-date", help="YYYY-MM-DD")
    p.add_argument("--return-date-range", help="YYYY-MM-DD:YYYY-MM-DD")
    p.add_argument("--max-return-span", type=int, default=30)

    p.add_argument("--continent", choices=["AF", "AS", "EU", "NA", "SA", "OC"])
    p.add_argument("--destinations")
    p.add_argument("--destinations-file")

    p.add_argument("--data-dir", default="data_airports")
    p.add_argument("--top-per-continent", type=int, default=220)
    p.add_argument("--min-degree", type=int, default=15)
    p.add_argument("--include-medium", action="store_true")
    p.add_argument("--no-download", action="store_true")
    p.add_argument("--export-hubs")
    p.add_argument("--no-search", action="store_true")

    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--max-stops", default="many", choices=["none", "one", "many"])
    p.add_argument("--top", type=int, default=25)

    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--rps", type=float, default=2.0)
    p.add_argument("--burst", type=int, default=4)

    p.add_argument("--cache-db", default="cache.sqlite")
    p.add_argument("--ttl-hours", type=float, default=12.0)

    p.add_argument("--json-out")
    p.add_argument("--verbose", action="store_true")

    args = p.parse_args(argv)
    if args.cache_db == "":
        args.cache_db = None
    return args


def main():
    args = parse_args()
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())