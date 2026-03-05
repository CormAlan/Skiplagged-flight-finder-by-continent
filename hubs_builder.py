import argparse
import csv
import os
import urllib.request

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

TYPE_WEIGHT = {
    "large_airport": 1000,
    "medium_airport": 200,
    "small_airport": 0,
}

CONTINENT_IATA_OVERRIDE = {
    "IST": "EU",
    "SAW": "EU",
}

def download(url, path, verbose=False):
    if verbose:
        print("[download]", url, "->", path, flush=True)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with urllib.request.urlopen(url) as r, open(path, "wb") as f:
        f.write(r.read())

def read_ourairports_airports_csv(path):
    out = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            iata = (row.get("iata_code") or "").strip().upper()
            if not iata:
                continue
            out[iata] = {
                "iata": iata,
                "continent": (row.get("continent") or "").strip().upper(),
                "type": (row.get("type") or "").strip(),
                "scheduled_service": (row.get("scheduled_service") or "").strip().lower(),
                "iso_country": (row.get("iso_country") or "").strip().upper(),
                "name": (row.get("name") or "").strip(),
            }
    return out

def read_openflights_routes_degree(path):
    deg = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = list(csv.reader([line]))[0]
            if len(parts) < 5:
                continue
            src = (parts[2] or "").strip().upper()
            dst = (parts[4] or "").strip().upper()
            if len(src) == 3:
                deg[src] = deg.get(src, 0) + 1
            if len(dst) == 3:
                deg[dst] = deg.get(dst, 0) + 1
    return deg

def build_hubs_for_continent(our, deg, continent, top_n, min_degree, include_medium):
    continent = continent.strip().upper()
    candidates = []
    for iata, row in our.items():
        c = CONTINENT_IATA_OVERRIDE.get(iata, row.get("continent", ""))
        if c != continent:
            continue
        if row.get("scheduled_service") != "yes":
            continue
        t = row.get("type")
        if t == "small_airport":
            continue
        if (not include_medium) and t != "large_airport":
            continue
        d = deg.get(iata, 0)
        if d < min_degree:
            continue
        score = d + TYPE_WEIGHT.get(t, 0)
        candidates.append((score, d, iata))

    candidates.sort(reverse=True)
    return [iata for _, _, iata in candidates[:top_n]]

def parse_top_map(s):
    # AS=180,EU=140,...
    out = {}
    if not s:
        return out
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip().upper()
        v = v.strip()
        try:
            out[k] = int(v)
        except Exception:
            pass
    return out

def build_arg_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--outdir", type=str, default=".")
    p.add_argument("--data-dir", type=str, default="data")
    p.add_argument("--top", type=int, default=120)
    p.add_argument("--top-map", type=str, help="Per continent top counts, like AS=180,EU=140")
    p.add_argument("--min-degree", type=int, default=50)
    p.add_argument("--include-medium", action="store_true")
    p.add_argument("--no-download", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--continents", type=str, default="AS,EU,NA,SA,AF,OC")
    return p

def main():
    args = build_arg_parser().parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs(args.data_dir, exist_ok=True)

    our_path = os.path.join(args.data_dir, "ourairports_airports.csv")
    routes_path = os.path.join(args.data_dir, "openflights_routes.dat")
    airports_path = os.path.join(args.data_dir, "openflights_airports.dat")

    if not args.no_download and not os.path.exists(our_path):
        download(OURAIRPORTS_AIRPORTS_CSV, our_path, verbose=args.verbose)
    if not args.no_download and not os.path.exists(airports_path):
        download(OPENFLIGHTS_AIRPORTS_DAT, airports_path, verbose=args.verbose)
    if not args.no_download and not os.path.exists(routes_path):
        download(OPENFLIGHTS_ROUTES_DAT, routes_path, verbose=args.verbose)

    if not os.path.exists(our_path) or not os.path.exists(routes_path):
        raise SystemExit("missing data files (run without --no-download once)")

    our = read_ourairports_airports_csv(our_path)
    deg = read_openflights_routes_degree(routes_path)

    top_map = parse_top_map(args.top_map)
    continents = [x.strip().upper() for x in args.continents.split(",") if x.strip()]

    for c in continents:
        if c not in CONTINENT_NAME:
            continue
        top_n = top_map.get(c, args.top)
        hubs = build_hubs_for_continent(our, deg, c, top_n, args.min_degree, args.include_medium)
        out_path = os.path.join(args.outdir, f"{CONTINENT_NAME[c]}_hubs.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            for iata in hubs:
                f.write(iata + "\n")
        if args.verbose:
            print("[hubs]", c, "candidates=", len(hubs), "wrote", out_path, flush=True)

if __name__ == "__main__":
    main()
