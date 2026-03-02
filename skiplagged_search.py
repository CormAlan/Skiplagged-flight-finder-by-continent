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
import urllib.error
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


# -------------------------
# MCP / Skiplagged (HTTP JSON-RPC)
# -------------------------
MCP_ENDPOINT = "https://mcp.skiplagged.com/mcp"
SK_TOOL_NAME = "sk_flights_search"


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

# Istanbul airports: force EU (never AS)
CONTINENT_IATA_OVERRIDE = {
    "IST": "EU",
    "SAW": "EU",
}

def parse_mcporter_js_object(text: str) -> dict:
    """
    mcporter sometimes prints a JS object literal, not JSON.
    We extract structuredContent and convert it to JSON-ish.
    """
    # Grab "structuredContent: { ... }" including nested braces (best-effort)
    m = re.search(r"structuredContent:\s*\{", text)
    if not m:
        raise ValueError("No structuredContent found in mcporter output")

    i = m.start()
    # Find the first '{' after 'structuredContent:'
    start = text.find("{", m.end() - 1)
    if start == -1:
        raise ValueError("structuredContent opening brace not found")

    # Brace matching to find the end of structuredContent object
    depth = 0
    end = None
    for idx in range(start, len(text)):
        ch = text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = idx + 1
                break
    if end is None:
        raise ValueError("structuredContent closing brace not found")

    obj = text[start:end]

    # Convert JS-ish to JSON-ish:
    # 1) Quote unquoted keys: searchUrl: -> "searchUrl":
    obj = re.sub(r"(\{|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", r'\1 "\2":', obj)

    # 2) Convert single quotes to double quotes
    obj = obj.replace("'", '"')

    # 3) Remove trailing commas before } or ]
    obj = re.sub(r",\s*([}\]])", r"\1", obj)

    return json.loads(obj)

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
    if our.scheduled_service in ("no", "n", "false", "0"):
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
        # Apply continent override first (e.g. IST/SAW => EU)
        eff_cont = CONTINENT_IATA_OVERRIDE.get(iata, our.continent)

        if eff_cont != continent:
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
    return_date: Optional[str]
    limit: int
    max_stops: str

# -------------------------
# MCP / Skiplagged (HTTP JSON-RPC + session init)
# -------------------------
MCP_ENDPOINT = "https://mcp.skiplagged.com/mcp"
SK_TOOL_NAME = "sk_flights_search"

# MCP protocol version: many servers accept 2024-06-18; if Skiplagged requires a newer one, change here.
MCP_PROTOCOL_VERSION = "2024-06-18"

# Simple global request id counter (safe enough; to_thread calls are serialized by a lock below)
_mcp_next_id = 1


def _jsonrpc_request(method: str, params: dict | None, request_id: int | None) -> dict:
    req = {"jsonrpc": "2.0", "method": method}
    if request_id is not None:
        req["id"] = request_id
    if params is not None:
        req["params"] = params
    return req


def _bump_request_id() -> int:
    global _mcp_next_id
    _mcp_next_id += 1
    return _mcp_next_id


class MCPClient:
    """
    Minimal MCP Streamable-HTTP client:
    - POST initialize
    - capture MCP-Session-Id response header (if provided)
    - POST notifications/initialized
    - then tools/list / tools/call with MCP-Session-Id header
    """
    def __init__(self) -> None:
        self.session_id: str | None = None
        self._initialized: bool = False
        self._lock = asyncio.Lock()

    def _post(self, payload: dict, timeout: int = 60) -> dict:
        data = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            # Some servers are picky: include both JSON + SSE
            "Accept": "application/json, text/event-stream",
        }
        if self.session_id:
            headers["MCP-Session-Id"] = self.session_id

        req = urllib.request.Request(
            MCP_ENDPOINT,
            data=data,
            headers=headers,
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                # Capture session id if server provides one (spec header name)
                sid = resp.headers.get("MCP-Session-Id")
                if sid and not self.session_id:
                    self.session_id = sid

                raw = resp.read().decode("utf-8", errors="replace")
                ct = (resp.headers.get("Content-Type") or "").lower()

                if not raw.strip():
                    # If the server responds with empty body, show useful debugging info
                    raise RuntimeError(
                        f"MCP empty response body (status={getattr(resp, 'status', 'unknown')}, content-type={ct}, session={self.session_id})"
                    )

                # Try JSON first
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    pass

                # Try SSE parsing: look for lines like "data: {...json...}"
                if "text/event-stream" in ct or "data:" in raw:
                    last_obj = None
                    for line in raw.splitlines():
                        line = line.strip()
                        if not line.startswith("data:"):
                            continue
                        payload = line[len("data:"):].strip()
                        if not payload:
                            continue
                        try:
                            last_obj = json.loads(payload)
                        except json.JSONDecodeError:
                            # sometimes servers send non-json data events; ignore
                            continue
                    if last_obj is not None:
                        return last_obj

                # If we get here, we didn't understand the response
                snippet = raw[:500].replace("\r", "\\r").replace("\n", "\\n")
                raise RuntimeError(f"MCP non-JSON response (content-type={ct}): {snippet}")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"MCP HTTPError {e.code}: {body}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"MCP connection error: {e}") from e

    async def ensure_initialized(self, timeout: int = 60) -> None:
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            # 1) initialize
            init_id = _bump_request_id()
            init_payload = _jsonrpc_request(
                "initialize",
                {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "skiplagged-flight-finder", "version": "0.1"},
                },
                request_id=init_id,
            )

            init_resp = await asyncio.to_thread(self._post, init_payload, timeout)

            # 2) notifications/initialized (no id)
            # Some servers want this after initialize; safe to always send.
            notif_payload = _jsonrpc_request(
                "notifications/initialized",
                {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "skiplagged-flight-finder", "version": "0.1"},
                },
                request_id=None,
            )
            await asyncio.to_thread(self._post, notif_payload, timeout)

            self._initialized = True

    async def tools_list(self, timeout: int = 60) -> dict:
        await self.ensure_initialized(timeout)
        rid = _bump_request_id()
        payload = _jsonrpc_request("tools/list", {}, request_id=rid)
        return await asyncio.to_thread(self._post, payload, timeout)

    async def call_tool(self, name: str, arguments: dict, timeout: int = 60) -> dict:
        await self.ensure_initialized(timeout)
        rid = _bump_request_id()
        payload = _jsonrpc_request(
            "tools/call",
            {"name": name, "arguments": arguments},
            request_id=rid,
        )
        resp = await asyncio.to_thread(self._post, payload, timeout)
        # Usually result is under "result"
        if isinstance(resp, dict) and "result" in resp:
            r = resp["result"]
            return r if isinstance(r, dict) else {"result": r}
        return resp


_mcp_client = MCPClient()


def _extract_mcporter_text(payload: Dict[str, Any]) -> str:
    """
    Pulls the human-readable markdown text from a mcporter JS-ish envelope.
    Works for both:
      - JSON parsed dict
      - our fallback {"mcporter_text": "..."}
    """
    if not isinstance(payload, dict):
        return ""
    if isinstance(payload.get("mcporter_text"), str):
        return payload["mcporter_text"]

    content = payload.get("content")
    if isinstance(content, list) and content:
        first = content[0]
        if isinstance(first, dict) and isinstance(first.get("text"), str):
            return first["text"]
    return ""


def _parse_table_from_mcporter_text(text: str) -> List[Dict[str, Any]]:
    """
    Parses the markdown table rows that look like:
      | $314 | 37h 30m | 2 stops | ... | ... | ... | [Book](https://...#trip=...) |

    Returns list of dicts with at least: price, duration, stops, url.
    """
    if not text:
        return []

    lines = text.splitlines()

    # Find header line containing "| Price | Duration |"
    header_idx = None
    for i, ln in enumerate(lines):
        l = ln.lower().replace(" ", "")
        # Kräver bara price+duration. "booking/link" varierar i one-way.
        if l.startswith("|") and "|price|" in l and "|duration|" in l:
            header_idx = i
            break
    if header_idx is None:
        return []

    # Skip separator line after header (| --- | --- | ...)
    start = header_idx + 2
    rows: List[Dict[str, Any]] = []

    # Parse subsequent lines that start with '|'
    for ln in lines[start:]:
        ln = ln.strip()
        if not ln.startswith("|"):
            break
        # Split cells; markdown rows start/end with '|'
        parts = [c.strip() for c in ln.strip("|").split("|")]
        if len(parts) < 7:
            continue

        price_cell = parts[0]
        duration_cell = parts[1]
        stops_cell = parts[2]
        booking_cell = parts[-1]

        # price like "$314"
        m_price = re.search(r"\$([0-9]+(?:\.[0-9]+)?)", price_cell)
        price = float(m_price.group(1)) if m_price else None

        # booking link like [Book](https://...)
        m_url = re.search(r"\((https?://[^)]+)\)", booking_cell)
        if m_url:
            url = m_url.group(1)
        else:
            url = booking_cell if booking_cell.startswith("http") else None

        rows.append(
            {
                "price": price,
                "duration": duration_cell if duration_cell else None,
                "stops": stops_cell if stops_cell else None,
                "url": url,
                "raw_row": parts,
            }
        )

    return rows

async def run_mcporter_search(job: "SearchJob", npx_path: str) -> Dict[str, Any]:
    MCP_TOOL_URL = "https://mcp.skiplagged.com/mcp.sk_flights_search"

    args_obj = {
        "origin": job.origin,
        "destination": job.dest,
        "departureDate": job.departure_date,
        "sort": "price",
        "limit": int(job.limit),
        "maxStops": job.max_stops,
        "offset": 0,
    }
    if getattr(job, "return_date", None):
        args_obj["returnDate"] = job.return_date
    args_json = json.dumps(args_obj, separators=(",", ":"))

    cmd = [
        npx_path,
        "--yes",
        "mcporter",
        "call",
        MCP_TOOL_URL,
        "--args",
        args_json,
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
        raise RuntimeError(
            f"mcporter failed (code={proc.returncode}) dest={job.dest} date={job.departure_date}\n"
            f"stderr: {err_text}"
        )

    # mcporter output is JS-ish; keep it as text so we can parse the markdown table
    return {"mcporter_text": out_text, "mcporter_stderr": err_text}

def _decode_mcporter_markdown(js_out: str) -> str:
    """
    Robustare: extraherar ENDAST text:-fältets strängkonkatenering,
    istället för att plocka alla '...' i hela objektet.
    """
    if not js_out:
        return ""

    tpos = js_out.find("text:")
    if tpos == -1:
        return ""

    # Hitta början efter "text:"
    i = tpos + len("text:")
    # Skip whitespace
    while i < len(js_out) and js_out[i].isspace():
        i += 1

    # Vi vill parsa ett uttryck som typiskt ser ut så här:
    # text: '...' + '\n' + '...' + ...
    # Stoppa när vi når "structuredContent:" (säkert ankare) eller när vi når slutet.
    end = js_out.find("structuredContent:", i)
    if end == -1:
        end = len(js_out)

    expr = js_out[i:end]

    # Begränsa ytterligare: ta bara fram till första "\nstructuredContent" eller "structuredContent"
    # (done ovan). Nu extraherar vi endast string literals i expr.
    parts = re.findall(r"'((?:\\.|[^'\\])*)'", expr)
    if not parts:
        return ""

    # Viktigt: tolka \n, \t osv
    return "".join(bytes(p, "utf-8").decode("unicode_escape", errors="replace") for p in parts)

def extract_itineraries(payload: Any) -> List[Dict[str, Any]]:
    # ---- Normalize payload into a raw text blob we can parse ----
    js_out = ""

    if isinstance(payload, dict):
        # 1) If mcporter_text exists, it's usually a JSON string with { text, structuredContent, error, ... }
        mcpt = payload.get("mcporter_text")
        if isinstance(mcpt, str) and mcpt.strip():
            try:
                inner = json.loads(mcpt)
            except Exception:
                inner = None

            if isinstance(inner, dict):
                # Prefer an actual text field if present
                for k in ("text", "stdout", "output", "raw"):
                    v = inner.get(k)
                    if isinstance(v, str) and v.strip():
                        js_out = v
                        break
                # If no direct text, fall back to stringified inner dict (often contains "text:" envelope)
                if not js_out:
                    js_out = mcpt
            else:
                js_out = mcpt

        # 2) Fallbacks if mcporter_text wasn't present
        if not js_out:
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

    # ---- Decode markdown (either already markdown, or inside mcporter "text:" envelope) ----
    if "\n|" in js_out and "| price |" in js_out.lower():
        md = js_out
    else:
        md = _decode_mcporter_markdown(js_out)

    if not md:
        return []

    lines = [ln.rstrip("\n") for ln in md.splitlines() if ln.strip()]
    if not lines:
        return []

    # Find the first markdown table header that has a Price column.
    header_idx = None
    header_cols: List[str] = []

    def split_row(row: str) -> List[str]:
        # strip leading/trailing pipes and split
        r = row.strip()
        if r.startswith("|"):
            r = r[1:]
        if r.endswith("|"):
            r = r[:-1]
        return [c.strip() for c in r.split("|")]

    for i, ln in enumerate(lines):
        if not ln.lstrip().startswith("|"):
            continue
        cols = split_row(ln)
        if len(cols) < 2:
            continue
        cols_l = [c.lower().strip() for c in cols]
        if any("price" in c for c in cols_l):
            # Require the separator line below (---) to confirm it's a table header
            if i + 1 < len(lines) and set(lines[i + 1].replace("|", "").replace(":", "").replace("-", "").strip()) == set():
                # sometimes separator line detection fails; accept anyway if next line looks like |...|
                pass
            header_idx = i
            header_cols = cols_l
            break

    if header_idx is None:
        return []

    # Map columns by fuzzy names
    def find_col(pred) -> Optional[int]:
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

    # Data rows start after header + separator row (usually header_idx+2)
    start = header_idx + 2 if header_idx + 2 < len(lines) else header_idx + 1

    out: List[Dict[str, Any]] = []

    # Helper: extract first URL from text
    def extract_url(cell: str) -> Optional[str]:
        # markdown link: [text](url)
        m = re.search(r"\((https?://[^)]+)\)", cell)
        if m:
            return m.group(1)
        # plain url
        m2 = re.search(r"(https?://\S+)", cell)
        if m2:
            return m2.group(1).rstrip(").,")
        return None

    for ln in lines[start:]:
        if not ln.lstrip().startswith("|"):
            # stop when table ends
            break
        cols = split_row(ln)
        if len(cols) <= idx_price:
            continue

        price_cell = cols[idx_price]
        dur_cell = cols[idx_duration] if (idx_duration is not None and idx_duration < len(cols)) else None
        stops_cell = cols[idx_stops] if (idx_stops is not None and idx_stops < len(cols)) else None

        # Prefer link column if it exists; otherwise scan all cells
        url = None
        if idx_link is not None and idx_link < len(cols):
            url = extract_url(cols[idx_link])
        if not url:
            for c in cols:
                url = extract_url(c)
                if url:
                    break

        # Price: keep as numeric USD if present
        m_price = re.search(r"\$([0-9][0-9,]*(?:\.[0-9]+)?)", price_cell)
        price = float(m_price.group(1).replace(",", "")) if m_price else None

        out.append(
            {
                "price": price,
                "duration": dur_cell,
                "stops": stops_cell,
                "url": url,
            }
        )

    # Filter out rows with no price at all
    out = [x for x in out if x.get("price") is not None]
    return out
    
from urllib.parse import urlparse
def _url_matches_route(url: str | None, origin: str, dest: str) -> bool:
    if not url:
        return True  # inget att validera
    try:
        path = urlparse(url).path.strip("/")
        # förväntat: flights/ARN/PEK/2026-03-21 eller flights/ARN/PEK/2026-03-21/2026-03-30
        parts = path.split("/")
        if len(parts) < 4:
            return True
        if parts[0] != "flights":
            return True
        u_origin = parts[1].upper()
        u_dest = parts[2].upper()
        return (u_origin == origin.upper()) and (u_dest == dest.upper())
    except Exception:
        return True
    
def normalize_hits(
    origin: str,
    dest: str,
    departure_date: str,
    return_date: Optional[str],
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    hits: List[Dict[str, Any]] = []
    for it in extract_itineraries(payload):
        if not isinstance(it, dict):
            continue
        stops = it.get("stops")
        price = it.get("price") or it.get("bestPrice") or it.get("amount")
        duration = it.get("duration") or it.get("totalDuration")
        url = it.get("bookingLink") or it.get("url") or it.get("deepLink")
        # drop or flag corrupted rows
        if not _url_matches_route(url, origin, dest):
            continue
        hits.append(
            {
                "origin": origin,
                "dest": dest,
                "departure_date": departure_date,
                "return_date": return_date,
                "price": price,
                "duration": duration,
                "url": url,
                "stops": stops,
                "raw": it,
            }
        )
    return hits




def _min_priced_itinerary(itins: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Return the itinerary dict with the lowest numeric 'price' (ties: first)."""
    best: Optional[Dict[str, Any]] = None
    best_price: float = float("inf")
    for it in itins:
        if not isinstance(it, dict):
            continue
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

def _payload_error(payload: Any) -> str | None:
    if isinstance(payload, dict):
        # mcporter wrapper
        if isinstance(payload.get("mcporter_text"), str) and '"error"' in payload["mcporter_text"]:
            try:
                j = json.loads(payload["mcporter_text"])
                if isinstance(j, dict) and j.get("error"):
                    return str(j["error"])
            except Exception:
                # fall back: substring
                return "mcporter_text contains error"
        # direct dict error
        if payload.get("error"):
            return str(payload["error"])
    return None

async def worker(
    name: str,
    queue: "asyncio.Queue[SearchJob]",
    limiter: TokenBucket,
    cache: Optional[SQLiteCache],
    ttl_seconds: int,
    results: List[Dict[str, Any]],
    errors: List[str],
    verbose: bool,
    npx_path: str,
) -> None:
    """
    Worker that consumes SearchJob items.

    Behavior:
      - One-way (return_date is None): fetch MCP payload for (origin->dest, dep_date) and normalize hits.
      - Round-trip (return_date is set): ALWAYS compute total price as:
            min_price(origin->dest on dep_date) + min_price(dest->origin on return_date)
        and emit a single combined hit per (origin, dest, dep_date, return_date).
    """
    while True:
        job = await queue.get()
        try:
            async def fetch_payload(j: SearchJob) -> Dict[str, Any]:
                """
                Fetch via mcporter with:
                - proper cache key (includes returnDate)
                - retry on MCP/server errors
                - exponential backoff
                """

                cache_key_local = None
                cached_local = None

                if cache:
                    cache_key_local = stable_key(
                        {
                            "tool": "sk_flights_search",
                            "origin": j.origin,
                            "dest": j.dest,
                            "departureDate": j.departure_date,
                            "returnDate": j.return_date,
                            "limit": j.limit,
                            "maxStops": j.max_stops,
                            "sort": "price",
                        }
                    )

                    cached_local = cache.get(cache_key_local, ttl_seconds=ttl_seconds)
                    if cached_local is not None:
                        if verbose:
                            tag = f" ret={j.return_date}" if j.return_date else ""
                            print(f"[{name}] cache hit: {j.dest} {j.departure_date}{tag}")
                        return cached_local

                # Retry loop
                last_error = None

                for attempt in range(3):
                    await limiter.acquire()

                    if verbose:
                        tag = f" ret={j.return_date}" if j.return_date else ""
                        print(f"[{name}] requesting: {j.dest} {j.departure_date}{tag} (attempt {attempt+1}/3)")

                    payload_local = await run_mcporter_search(j, npx_path)

                    # Detect MCP error wrapper
                    err = None
                    if isinstance(payload_local, dict):
                        if "error" in payload_local and payload_local["error"]:
                            err = str(payload_local["error"])

                        # mcporter_text sometimes contains JSON with error
                        mcpt = payload_local.get("mcporter_text")
                        if isinstance(mcpt, str) and '"error"' in mcpt:
                            try:
                                parsed = json.loads(mcpt)
                                if isinstance(parsed, dict) and parsed.get("error"):
                                    err = str(parsed["error"])
                            except Exception:
                                err = "mcporter_text contained error"

                    if not err:
                        # Success
                        if cache and cache_key_local:
                            cache.set(cache_key_local, payload_local)
                        return payload_local

                    last_error = err

                    if verbose:
                        print(f"[{name}] MCP error: {err[:150]}")

                    # exponential backoff
                    await asyncio.sleep(1.5 * (attempt + 1))

                # If we reach here: all retries failed
                if verbose:
                    print(f"[{name}] FAILED after retries: {last_error}")

                raise RuntimeError(f"MCP error after retries: {last_error}")

            # --- Round-trip: total = min(one-way out) + min(one-way back) ---
            if job.return_date:
                # Outbound one-way
                j_out = SearchJob(
                    origin=job.origin,
                    dest=job.dest,
                    departure_date=job.departure_date,
                    return_date=None,
                    limit=job.limit,
                    max_stops=job.max_stops,
                )

                # Return one-way
                j_back = SearchJob(
                    origin=job.dest,
                    dest=job.origin,
                    departure_date=job.return_date,
                    return_date=None,
                    limit=job.limit,
                    max_stops=job.max_stops,
                )

                payload_out = await fetch_payload(j_out)
                payload_back = await fetch_payload(j_back)

                # 👇 HÄR lägger du debuggen
                out_list = extract_itineraries(payload_out)
                back_list = extract_itineraries(payload_back)

                if verbose:
                    print(f"[{name}] one-way parsed: out={len(out_list)} back={len(back_list)}")

                it_out = _min_priced_itinerary(out_list)
                it_back = _min_priced_itinerary(back_list)

                if not it_out or not it_back:
                    continue

                try:
                    p_out = float(it_out.get("price"))
                    p_back = float(it_back.get("price"))
                except Exception:
                    continue

                total = p_out + p_back

                dur_out = it_out.get("duration")
                dur_back = it_back.get("duration")
                stops_out = it_out.get("stops")
                stops_back = it_back.get("stops")

                combined_duration = None
                if dur_out or dur_back:
                    combined_duration = f"Outbound: {dur_out}<br/>Return: {dur_back}"

                combined_stops = None
                if stops_out or stops_back:
                    combined_stops = f"Outbound: {stops_out} / Return: {stops_back}"

                out_url = it_out.get("url")
                back_url = it_back.get("url")


                results.append(
                    {
                        "origin": job.origin,
                        "dest": job.dest,
                        "departure_date": job.departure_date,
                        "return_date": job.return_date,
                        "price": total,
                        "price_outbound": p_out,
                        "price_return": p_back,
                        "duration": combined_duration,
                        "stops": combined_stops,
                        # skriv inte en "skum" RT-url som primary längre
                        "url_outbound": out_url,
                        "url_return": back_url,
                        "raw": {
                            "rt_total_mode": "sum_oneways",
                            "price_outbound_min": p_out,
                            "price_return_min": p_back,
                        },
                    }
                )
                continue

            payload = await fetch_payload(job)
            results.extend(normalize_hits(job.origin, job.dest, job.departure_date, job.return_date, payload))

        except Exception as e:
            msg = f"{job.dest} {job.departure_date}: {e}"
            errors.append(msg)
            if verbose:
                print(f"[{name}] error: {msg}")
        finally:
            queue.task_done()


def build_jobs(
    origin: str,
    destinations: List[str],
    dep_return_pairs: List[Tuple[str, Optional[str]]],
    limit: int,
    max_stops: str,
) -> List[SearchJob]:
    """
    Builds SearchJob list for:
      origin -> each destination,
      for each (departure_date, return_date) pair.

    return_date=None => one-way.
    """
    jobs: List[SearchJob] = []
    for d in destinations:
        for dep, ret in dep_return_pairs:
            jobs.append(
                SearchJob(
                    origin=origin,
                    dest=d,
                    departure_date=dep,
                    return_date=ret,
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

    # Dates (departures)
    if args.date and args.date_range:
        print("Choose either --date OR --date-range, not both.", file=sys.stderr)
        return 2
    if args.date:
        dep_dates = [parse_date(args.date)]
    elif args.date_range:
        dep_dates = parse_date_range(args.date_range)
    else:
        print("You must provide --date or --date-range.", file=sys.stderr)
        return 2

    # Returns: one-way by default. For round-trips, use either --return-date or --return-date-range.
    if args.return_date and args.return_date_range:
        print("Choose either --return-date OR --return-date-range, not both.", file=sys.stderr)
        return 2

    dep_return_pairs: List[Tuple[str, Optional[str]]] = []

    if args.return_date:
        ret = parse_date(args.return_date)
        for d in dep_dates:
            if ret < d:
                continue
            if (ret - d).days > int(args.max_return_span):
                continue
            dep_return_pairs.append((d.strftime("%Y-%m-%d"), ret.strftime("%Y-%m-%d")))
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
            print(f"[main] no-search enabled. Mode={used_mode}, destinations={len(destinations)}")
        return 0

    workers = max(1, int(args.workers))
    limiter = TokenBucket(rps=args.rps, burst=args.burst)

    cache = SQLiteCache(args.cache_db) if args.cache_db else None
    ttl_seconds = int(max(0, args.ttl_hours) * 3600)

    jobs = build_jobs(origin, destinations, dep_return_pairs, limit=args.limit, max_stops=args.max_stops)

    if args.verbose:
        print(f"[main] mode={used_mode} destinations={len(destinations)} dep_return_pairs={len(dep_return_pairs)} jobs={len(jobs)}")
        print(f"[main] workers={workers} rps={args.rps} burst={args.burst} cache={'on' if cache else 'off'} ttl_hours={args.ttl_hours}")

    queue: asyncio.Queue[SearchJob] = asyncio.Queue()
    for j in jobs:
        queue.put_nowait(j)

    results: List[Dict[str, Any]] = []
    errors: List[str] = []

    tasks = [
        asyncio.create_task(worker(f"w{i+1}", queue, limiter, cache, ttl_seconds, results, errors, args.verbose, args.npx_path))
        for i in range(workers)
    ]

    await queue.join()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    sorted_hits = sort_hits(results)

    print("\n=== TOP RESULTS ===")
    for h in sorted_hits[: args.top]:
        o = h["origin"]
        d = h["dest"]
        dep = h["departure_date"]
        ret = h.get("return_date")

        price_total = h.get("price")
        p_out = h.get("price_outbound")
        p_back = h.get("price_return")

        dur = h.get("duration")
        stops = h.get("stops")

        out_url = h.get("url_outbound")
        back_url = h.get("url_return")

        # 1) Prisformat: TOTAL (OUT+BACK)
        if ret and (p_out is not None) and (p_back is not None):
            price_str = f"{price_total:.0f} ({p_out:.0f}+{p_back:.0f})"
        else:
            # one-way fallback
            price_str = f"{price_total:.0f}" if price_total is not None else "None"

        # 2) Första raden: summary
        print(f"{o} -> {d}  {dep} -> {ret}  price={price_str}  duration={dur}  stops={stops}")

        # 3) Andra/tredje raden: 2 URL:er
        if out_url:
            print(f"  outbound: {out_url}")
        if back_url:
            print(f"  return:   {back_url}")

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
    p.add_argument("--npx-path", default="npx", help="Path to npx (Windows: often npx.cmd). Example: C:\\Program Files\\nodejs\\npx.cmd")
    # search inputs
    p.add_argument("--origin", required=True, help="Origin IATA, e.g. ARN")
    p.add_argument("--date", help="Single departure date: YYYY-MM-DD")
    p.add_argument("--date-range", help="Inclusive date range: YYYY-MM-DD:YYYY-MM-DD")
    # Return date handling: either fixed return date for all departures, or a range of return dates to pair with each departure (with optional max span limit).
    p.add_argument("--return-date", help="Fixed return date (YYYY-MM-DD). If provided, searches round trips with this return date for each departure.")
    p.add_argument("--return-date-range", help="Return date range in format YYYY-MM-DD:YYYY-MM-DD. Generates all return dates in the closed interval and pairs each departure with every return >= departure.")
    # safety: maximum number of return days to consider per departure
    p.add_argument("--max-return-span", type=int, default=30, help="Max days between departure and return to consider (default 30) to limit combinatorial explosion.")
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