# Skiplagged Continent Flight Scanner

This project allows you to scan entire continents for cheap flights using:

- Dynamic hub generation (OurAirports + OpenFlights)
- Skiplagged via MCP (mcporter)
- Async workers + rate limiting
- SQLite caching
- One-way and round-trip searches
- Date ranges
- JSON export

Example use case:

Departure: ARN (Stockholm)  
Arrival: AS (Asia)

The script builds realistic hub lists automatically and scans them.



## REQUIREMENTS

Python:
- Python 3.10+ (3.12 recommended)

Node:
- Node.js installed

IMPORTANT:  
You **must** provide `--npx-path` when running the script.

On Windows this is typically:

```
--npx-path "C:\Program Files\nodejs\npx.cmd"
```

The script internally runs:

```
npx mcporter call https://mcp.skiplagged.com/mcp.sk_flights_search
```

If `--npx-path` is missing or incorrect, you will get:

```
[WinError 2] The system cannot find the file specified
```

Internet is required for dataset downloads and Skiplagged queries.



## SCRIPTS

1) hubs_builder.py  
   Builds ranked continent hub lists only.

2) skiplagged_search.py  
   Full scanner with hub auto-build + live Skiplagged search.



## HUB GENERATION

Data sources:
- OurAirports (airport metadata, continent, airport type)
- OpenFlights (routes graph → degree = hub strength)

Airport score is roughly:

```
score ≈ route_degree + airport_type_weight
```

Where:
- route_degree = number of known connections in OpenFlights graph
- airport_type_weight boosts large_airport over medium_airport

This ensures major hubs rank higher than small regional airports.

Special rule:
Istanbul airports (IST and SAW) are force-classified as EU.
They will never appear when scanning Asia.



## DESTINATION MODES (Choose One)

```
--continent AS | EU | NA | SA | AF | OC
```

Auto-build hubs for that continent.

What happens internally:
- Downloads airport + route datasets (unless --no-download)
- Ranks airports by hub score
- Keeps top N (see --top-per-continent)
- Applies continent override rules (IST/SAW → EU only)



```
--destinations HND,ICN,BKK
```

Manual comma-separated IATA list.  
No hub-building logic is used.



```
--destinations-file hubs/asia_hubs.txt
```

One IATA per line.  
Lines may contain comments after `#`.  
Only the first token (IATA) is used.



## DEPARTURE MODES (Choose One)

```
--date YYYY-MM-DD
```

Runs exactly one departure date per destination.



```
--date-range YYYY-MM-DD:YYYY-MM-DD
```

Inclusive range.  
Each day becomes a separate job.

Example:

```
--date-range 2026-03-01:2026-03-05
```

Generates 5 departure searches per destination.

Important scaling rule:

jobs ≈ destinations × departure_dates × return_dates



## ROUND-TRIP BEHAVIOR

Enable round-trip using:

```
--return-date YYYY-MM-DD
```

OR

```
--return-date-range YYYY-MM-DD:YYYY-MM-DD
```

Round-trip pricing logic:

```
MIN(outbound one-way) + MIN(return one-way)
```

This means:

- The script does NOT trust bundled RT results.
- It queries two independent one-way searches.
- It sums the cheapest outbound and cheapest return.

Output example:

```
price=617 (302+315)
```

Two URLs are printed:
- outbound URL
- return URL

This guarantees transparency.

Optional limiter:

```
--max-return-span 10
```

Meaning:
If departure is March 1, return must be within 10 days (≤ March 11).

This prevents unrealistic long-stay combinations when using date ranges.



## RATE LIMITING (Very Important)

The scanner uses a token bucket limiter.

```
--workers N
```

Number of concurrent async workers.

Meaning:
- 1 → strictly sequential execution
- 2 → two parallel searches
- 6 → up to six simultaneous MCP calls

Higher workers:
- Faster scanning
- Higher risk of bot protection



```
--rps X
```

Requests per second refill rate.

Meaning:
- 1.0 → 1 new token per second
- 2.0 → 2 new tokens per second
- 0 → unlimited (NOT recommended)

This controls sustained throughput.



```
--burst B
```

Token bucket capacity.

Meaning:
- burst=2 → 2 requests can fire instantly
- burst=5 → 5 immediate requests allowed

After burst is consumed, refill speed is controlled by --rps.

Safe starting values:

```
--workers 2 --rps 1.0 --burst 2
```

If you see:

```
Streamable HTTP error ... <!DOCTYPE html>
```

You are being throttled.  
Reduce `--workers` or `--rps`.



## CACHING

```
--cache-db cache.sqlite
```

Enables SQLite caching.

Cache key includes:
- origin
- destination
- departure date
- return date
- max-stops
- limit



```
--ttl-hours 12
```

Time-to-live in hours.

Meaning:
- 12 → identical searches reuse cached result for 12 hours
- Higher TTL → less API traffic
- Lower TTL → fresher prices

Disable cache:

```
--cache-db ""
```



## HUB PARAMETERS (continent mode only)

```
--top-per-continent 220
```

Keep only top N ranked airports.

Effect:
- Higher → more coverage, more jobs, slower scans
- Lower → focus on strongest hubs, faster scans



```
--min-degree 15
```

Minimum route-degree threshold.

Degree = number of connections in OpenFlights graph.

- 5 → many secondary airports
- 20 → mostly major hubs
- 50 → mega hubs only



```
--include-medium
```

Includes medium_airport in addition to large_airport.

Without this flag:
Only large_airport is used.



```
--no-download
```

Skip dataset download.  
Use local cached data only.



```
--export-hubs hubs
```

Exports generated ranked hub list to a folder.



```
--no-search
```

Build hub list only.  
Do not call Skiplagged.

## SEARCH PARAMETERS

```
--limit 5
```

Number of itineraries fetched per MCP call.

Meaning:
- 1 → only cheapest
- 5 → top 5 returned by Skiplagged
- 10 → larger payload, more parsing

Does NOT change total destinations scanned.



```
--max-stops none | one | many
```

Stop filter:

- none → nonstop only
- one → max 1 stop
- many → unlimited stops



```
--top 25
```

Number of cheapest results printed after sorting.

This does NOT affect API calls.  
It only limits terminal output.



```
--json-out results.json
```

Exports full sorted results to JSON file.

Useful for:
- Automation
- Custom deal ranking
- Further analysis



```
--verbose
```

Enables debug output:
- Worker activity
- Cache hits
- Retry attempts
- Parsed itinerary counts
- MCP errors



## EXAMPLES

Scan Asia, one-way:

```
python skiplagged_search.py \
  --origin ARN \
  --continent AS \
  --date-range 2026-03-15:2026-03-22 \
  --workers 2 --rps 1.0 --burst 2 \
  --npx-path "C:\Program Files\nodejs\npx.cmd" \
  --cache-db cache.sqlite --ttl-hours 12
```

Weekend round-trip scan:

```
python skiplagged_search.py \
  --origin ARN \
  --continent EU \
  --date-range 2026-04-01:2026-04-10 \
  --return-date-range 2026-04-03:2026-04-15 \
  --max-return-span 5 \
  --workers 2 --rps 1.0 --burst 2 \
  --npx-path "C:\Program Files\nodejs\npx.cmd" \
  --cache-db cache.sqlite --ttl-hours 12
```

## DISCLAIMER

This tool is for personal exploration.

Use polite rate limits.  
Use caching.  
Respect Skiplagged service limits.