# Skiplagged Continent Search 

This project lets you do Avionero-style searches like:

- departure: ARN (Stockholm)
- arrival: AS (Asia) / EU / NA / SA / AF / OC

By:

1. Building smart continent hub lists automatically from real airport + route data
2. Querying Skiplagged via MCP using mcporter
3. Supporting rate limiting, parallelization, caching, and date ranges

You have two scripts:

- hubs_builder.py
- skiplagged_search.py


# OVERVIEW


#### hubs_builder.py

Downloads airport datasets and builds ranked hub lists per continent.

Data sources:
- OurAirports (airport metadata incl. continent)
- OpenFlights (routes graph → route-degree = hubness)

Output:
- asia_hubs.txt
- europe_hubs.txt
- NA_hubs.txt
- SA_hubs.txt
- africa_hubs.txt
- oceania_hubs.txt

Each file contains IATA codes sorted by hub strength.


#### skiplagged_search.py

Searches Skiplagged using:

- A continent (auto-build mode)
OR
- A destinations file
OR
- A comma-separated list

Supports:
- Parallel workers
- Rate limiting (token bucket)
- SQLite caching
- Date ranges
- JSON export
- Max stops filter


## REQUIREMENTS


Python:
- Python 3.10+ recommended

System:
- mcporter must be installed and in PATH

Example test:
mcporter call https://mcp.skiplagged.com/mcp.sk_flights_search origin=ARN destination=HND departureDate=2026-03-20 --output json

No external Python packages required (stdlib only).

Internet required for:
- Downloading airport datasets
- Skiplagged queries


## STEP 1 — BUILD HUB LISTS

Basic strong continent lists:

```
python hubs_builder.py \
  --outdir hubs \
  --top AS=220,EU=180,NA=200,SA=120,AF=120,OC=90 \
  --min-degree 15 \
  --include-medium
```

This creates:

```
hubs/asia_hubs.txt
hubs/europe_hubs.txt
hubs/NA_hubs.txt
hubs/SA_hubs.txt
hubs/africa_hubs.txt
hubs/oceania_hubs.txt
```

Each line looks like:
```
HND  # 12345  Tokyo Haneda (JP) [large_airport] deg=987
```
Only the first token (IATA) is used by the search script.



## STEP 2 — SEARCH USING A HUB FILE


Example:
Stockholm → Asia, single date:
```
python skiplagged_search.py \
  --origin ARN \
  --destinations-file hubs/asia_hubs.txt \
  --date 2026-03-20 \
  --workers 6 --rps 2.0 --burst 4 \
  --cache-db cache.sqlite --ttl-hours 12 \
  --max-stops many --limit 5 --top 30
```

Date range (inclusive):
```
python skiplagged_search.py \
  --origin ARN \
  --destinations-file hubs/asia_hubs.txt \
  --date-range 2026-03-15:2026-03-25 \
  --workers 6 --rps 2.0 --burst 4 \
  --cache-db cache.sqlite --ttl-hours 12 \
  --max-stops many --limit 5 --top 30 \
  --json-out results_AS.json
```


## AUTO BUILD INSIDE SEARCH SCRIPT

You can skip hubs_builder.py entirely:
```
python skiplagged_search.py \
  --origin ARN \
  --continent AS \
  --date-range 2026-03-15:2026-03-25 \
  --top-per-continent 220 \
  --min-degree 15 \
  --include-medium \
  --workers 6 --rps 2.0 --burst 4 \
  --cache-db cache.sqlite --ttl-hours 12 \
  --max-stops many --limit 5 --top 30 \
  --export-hubs hubs \
  --json-out results_AS.json
```
What happens:

1. Downloads datasets (unless --no-download)
2. Builds ranked hub list for AS
3. Optionally exports hubs/asia_hubs.txt
4. Searches Skiplagged
5. Prints cheapest results


## COMMON WORKFLOWS

Find absolute cheapest in Asia next 14 days:
```
python skiplagged_search.py \
  --origin ARN \
  --continent AS \
  --date-range 2026-03-15:2026-03-29 \
  --top-per-continent 250 \
  --min-degree 10 \
  --include-medium \
  --workers 6 --rps 2.0 --burst 4 \
  --cache-db cache.sqlite --ttl-hours 12 \
  --max-stops many \
  --limit 5 \
  --top 50
```

Only nonstop flights to Europe:
```
python skiplagged_search.py \
  --origin ARN \
  --continent EU \
  --date-range 2026-04-01:2026-04-30 \
  --max-stops none \
  --workers 4 --rps 1.2 --burst 2 \
  --cache-db cache.sqlite --ttl-hours 24 \
  --limit 5 --top 40
```

Export hubs only (no search):
```
python skiplagged_search.py \
  --origin ARN \
  --continent EU \
  --export-hubs hubs \
  --no-search \
  --verbose
```

## PERFORMANCE TUNING


Key flags:
```
--workers N
  Number of parallel async workers
```
```
--rps X
  Requests per second refill rate
  0 = unlimited (not recommended)
```
```
--burst B
  Max burst size (token bucket capacity)
```
```
Safe starting values:
--workers 4 --rps 1.5 --burst 3
```

## CACHING

Enable SQLite caching:
```
--cache-db cache.sqlite
--ttl-hours 12
```
Same (origin, dest, date, filters) will hit cache within TTL.

Disable cache:
```
--cache-db ""
```

## HOW HUB SCORING WORKS

Airport score ≈

route_degree (from OpenFlights graph)
+ airport_type_weight

Where:
large_airport > medium_airport > small_airport

This produces realistic major hubs instead of tiny regionals.


## TROUBLESHOOTING

mcporter not found:
- Install it and ensure it's in PATH.

Too many errors / rate-limited:
- Reduce --workers
- Reduce --rps
- Increase cache TTL

Too few airports:
- Lower --min-degree
- Add --include-medium
- Increase --top-per-continent


## DISCLAIMER

This is for personal use and exploration.
Use polite rate limits and caching.
Respect service limits.
