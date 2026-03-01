#!/usr/bin/env python3
"""
elite_hubs_builder.py

Build continent hub lists using:
- OurAirports airports.csv (continent, scheduled_service, type, iata_code, etc.)
- OpenFlights airports.dat + routes.dat (to compute hubness from route graph)

Outputs:
  asia_hubs.txt, europe_hubs.txt, NA_hubs.txt, SA_hubs.txt, africa_hubs.txt, oceania_hubs.txt

Usage:
  python elite_hubs_builder.py --outdir . --top 120 --min-degree 50
  python elite_hubs_builder.py --top AS=180,EU=140,NA=160,SA=90,AF=90,OC=80 --min-degree 20 --include-medium
"""

from __future__ import annotations
import argparse
import csv
import os
import sys
import urllib.request
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

# Sources (URLs in code are OK)
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

# OurAirports airport "type" values commonly include:
# large_airport, medium_airport, small_airport, heliport, seaplane_base, closed, etc.
TYPE_WEIGHT = {
    "large_airport": 1000,
    "medium_airport": 200,
    "small_airport": 0,
}

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
    """
    Keyed by IATA code (uppercase).
    """
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
    """
    Returns mapping: airport_id -> IATA (uppercase), for rows with valid IATA.
    OpenFlights airports.dat is a CSV-ish file with quoted fields.
    """
    id_to_iata: Dict[int, str] = {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            # Format: Airport ID, Name, City, Country, IATA, ICAO, Lat, Lon, Alt, TZ, DST, Tz DB, type, source
            if len(row) < 5:
                continue
            try:
                airport_id = int(row[0])
            except ValueError:
                continue
            iata = (row[4] or "").strip().upper()
            # OpenFlights uses \N for missing
            if iata and iata != r"\N":
                id_to_iata[airport_id] = iata
    return id_to_iata

def compute_degree_from_routes(path: str) -> Dict[int, int]:
    """
    Compute undirected-ish degree by counting appearances as source or destination airport ID
    where IDs are numeric.
    routes.dat format:
      Airline, Airline ID, Source airport, Source airport ID, Destination airport, Destination airport ID, Codeshare, Stops, Equipment
    """
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

def score_airport(iata: str, our: OurAirportRow, degree: int, include_medium: bool) -> Optional[int]:
    """
    Combine route-degree + airport type weight.
    Filter out non-scheduled-service and (by default) medium airports unless include_medium.
    """
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
        # exclude small/heliport/etc
        return None

    return int(degree) + TYPE_WEIGHT.get(our.airport_type, 0)

def write_list(path: str, items: List[Tuple[str, int, str]]) -> None:
    """
    items: (IATA, score, comment)
    """
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write("# Auto-generated hub list (IATA)\n")
        f.write("# Columns: IATA  # score  name/country/type\n")
        for iata, score, comment in items:
            f.write(f"{iata}  # {score}  {comment}\n")

def parse_top_overrides(s: Optional[str]) -> Dict[str, int]:
    """
    Parse: AS=180,EU=140,NA=160,...
    """
    if not s:
        return {}
    out: Dict[str, int] = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        k, v = part.split("=", 1)
        out[k.strip().upper()] = int(v.strip())
    return out

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data_airports", help="Where to store downloaded datasets")
    ap.add_argument("--outdir", default=".", help="Output directory for *_hubs.txt")
    ap.add_argument("--top", default="120", help="Top N per continent (int) OR overrides like AS=180,EU=140,...")
    ap.add_argument("--min-degree", type=int, default=30, help="Minimum route-degree to include (after filters)")
    ap.add_argument("--include-medium", action="store_true", help="Include medium airports (otherwise only large)")
    ap.add_argument("--no-download", action="store_true", help="Do not download; expect files already in data-dir")
    args = ap.parse_args(argv)

    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.outdir, exist_ok=True)

    our_path = os.path.join(args.data_dir, "ourairports_airports.csv")
    of_airports_path = os.path.join(args.data_dir, "openflights_airports.dat")
    of_routes_path = os.path.join(args.data_dir, "openflights_routes.dat")

    if not args.no_download:
        print("Downloading datasets...")
        download(OURAIRPORTS_AIRPORTS_CSV, our_path)
        download(OPENFLIGHTS_AIRPORTS_DAT, of_airports_path)
        download(OPENFLIGHTS_ROUTES_DAT, of_routes_path)

    print("Loading OurAirports airports.csv ...")
    our_by_iata = read_ourairports_airports_csv(our_path)

    print("Loading OpenFlights airports.dat ...")
    id_to_iata = parse_openflights_airports_dat(of_airports_path)

    print("Computing route degrees from routes.dat ...")
    degree_by_id = compute_degree_from_routes(of_routes_path)

    # Map IATA -> degree (max over possible IDs that share same IATA, to be robust)
    degree_by_iata: Dict[str, int] = {}
    for airport_id, deg in degree_by_id.items():
        iata = id_to_iata.get(airport_id)
        if not iata:
            continue
        if iata not in degree_by_iata or deg > degree_by_iata[iata]:
            degree_by_iata[iata] = deg

    # Determine top N per continent
    top_overrides = {}
    default_top = None
    if args.top.isdigit():
        default_top = int(args.top)
    else:
        top_overrides = parse_top_overrides(args.top)
        default_top = 120

    by_continent: Dict[str, List[Tuple[str, int, str]]] = {c: [] for c in CONTINENT_NAME.keys()}

    for iata, our in our_by_iata.items():
        deg = degree_by_iata.get(iata, 0)
        if deg < args.min_degree:
            continue
        sc = score_airport(iata, our, deg, include_medium=args.include_medium)
        if sc is None:
            continue
        comment = f"{our.name} ({our.iso_country}) [{our.airport_type}]"
        by_continent[our.continent].append((iata, sc, comment))

    # Sort and write
    for cont, items in by_continent.items():
        if cont not in CONTINENT_NAME or cont == "AN":
            continue
        items.sort(key=lambda x: x[1], reverse=True)
        n = top_overrides.get(cont, default_top)
        picked = items[:n]

        fname = f"{CONTINENT_NAME[cont]}_hubs.txt"
        out_path = os.path.join(args.outdir, fname)
        write_list(out_path, picked)
        print(f"Wrote {out_path} ({len(picked)} airports)")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())