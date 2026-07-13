"""justhodl-symbol-dictionary v1.0 — ops 3180.

Khalid: "put a full name for every ticker (the full name and FRED ticker)
so we can fuse them in the engines."

Right — an engine cannot reason about `PRAWMINDEXM`, but it can reason
about "Global Price Index of All Commodities". Names are the semantic
layer that makes his 6,500 indicators fusable.

Authoritative sources only (no invented names):
  FRED       /fred/series → official title, units, frequency, seasonal
             adjustment, and the exact history window
  POLYGON    /v3/reference/tickers → registered company / fund name
  WORLDBANK  /v2/indicator + /v2/country → indicator name x country name
             ("Zimbabwe — Deposit interest rate (%)")
  CURATED    indices, FX crosses and commodity futures
  DECODER    even for symbols we cannot yet PRICE, TradingView's
             ECONOMICS:{ISO2}{IND} codes are decoded to a human name, so
             the page never shows a bare code

Output: data/symbol-dictionary.json  {SYMBOL: {name, source, source_id,
        units, frequency, history, category}}
The dictionary is cached and only fills the gaps on each run.
"""

import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import series_source as SS  # bundled from aws/shared

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
MAP_KEY = "data/symbol-map.json"
LISTS_KEY = "data/tv-watchlists.json"
OUT_KEY = "data/symbol-dictionary.json"
POLY = os.environ.get("POLYGON_KEY", "")
FRED = (os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY")
        or SS.FRED_FALLBACK)
BUDGET_S = 780

S3 = boto3.client("s3", region_name="us-east-1")

CURATED = {
    "^GSPC": ("S&P 500 Index", "index"),
    "^NDX": ("Nasdaq-100 Index", "index"),
    "^DJI": ("Dow Jones Industrial Average", "index"),
    "^VIX": ("CBOE Volatility Index (VIX)", "index"),
    "^N225": ("Nikkei 225 Index", "index"),
    "^GDAXI": ("DAX 40 Index (Germany)", "index"),
    "^FTSE": ("FTSE 100 Index (UK)", "index"),
    "^HSI": ("Hang Seng Index (Hong Kong)", "index"),
    "^MOVE": ("ICE BofA MOVE Index (bond volatility)", "index"),
    "DX-Y.NYB": ("US Dollar Index (DXY)", "fx"),
    "GC=F": ("Gold Futures (COMEX)", "commodity"),
    "SI=F": ("Silver Futures (COMEX)", "commodity"),
    "CL=F": ("WTI Crude Oil Futures (NYMEX)", "commodity"),
    "SPY": ("SPDR S&P 500 ETF Trust", "etf"),
}

# TradingView ECONOMICS indicator suffix → human name
ECON_NAMES = {
    "GDPYY": "GDP Growth Rate (YoY)", "GDP": "Gross Domestic Product",
    "GDG": "Government Debt to GDP", "BOT": "Balance of Trade",
    "FER": "Foreign Exchange Reserves", "DIR": "Deposit Interest Rate",
    "LIR": "Bank Lending Rate", "IRYY": "Inflation Rate (YoY)",
    "CPI": "Consumer Price Index", "FI": "Food Inflation",
    "UR": "Unemployment Rate", "CS": "Consumer Spending",
    "CAG": "Current Account to GDP", "EXP": "Exports", "IMP": "Imports",
    "MS": "Money Supply", "FDI": "Foreign Direct Investment",
    "INTR": "Interest Rate", "INBR": "Interbank Rate",
    "CBBS": "Central Bank Balance Sheet", "BCOI": "Business Confidence",
    "CCI": "Consumer Confidence", "IPRI": "Import Prices",
    "EPRI": "Export Prices", "IP": "Industrial Production",
    "NO": "New Orders", "PMI": "Purchasing Managers Index",
    "M2": "Money Supply M2", "M3": "Money Supply M3",
    "CLI": "Composite Leading Indicator", "POP": "Population",
    "GS": "Gross Savings", "PROD": "Productivity",
    "RS": "Retail Sales", "HS": "Housing Starts", "BP": "Building Permits",
    "WG": "Wage Growth", "GBY": "Government Bond Yield",
    "CCR": "Consumer Credit", "PSC": "Private Sector Credit",
    "BLR": "Bank Lending Rate", "CBR": "Central Bank Rate",
}

COUNTRY = {}          # ISO2 → country name (filled from the World Bank)
WB_IND = {}           # indicator code → official name


def s3_get(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def http(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "jh-dict/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


# ── source-specific lookups ──────────────────────────────────────────
def fred_meta(sid, tries=3):
    """FRED caps at ~120 req/min. ops 3180 ran 10 workers at it, every call
    429'd, and the fallback wrote junk names ('DGS10 (FRED)'). Throttled +
    retried, and a failure now returns None so the symbol is RETRIED next
    pass instead of being cemented with a bad name."""
    for i in range(tries):
        try:
            d = http("https://api.stlouisfed.org/fred/series"
                     f"?series_id={urllib.parse.quote(sid)}&api_key={FRED}"
                     "&file_type=json")
            s = (d.get("seriess") or [None])[0]
            if not s:
                return None
            break
        except Exception:
            time.sleep(0.8 * (i + 1))
    else:
        return None
    try:
        return {
            "name": s.get("title"),
            "units": s.get("units_short") or s.get("units"),
            "frequency": s.get("frequency_short") or s.get("frequency"),
            "seasonal": s.get("seasonal_adjustment_short"),
            "history": f"{s.get('observation_start')} → "
                       f"{s.get('observation_end')}",
            "category": "macro",
        }
    except Exception:
        return None


def poly_meta(tk):
    if not POLY or tk.startswith("^") or "=" in tk or "." in tk:
        return None
    try:
        d = http(f"https://api.polygon.io/v3/reference/tickers/"
                 f"{urllib.parse.quote(tk)}?apiKey={POLY}")
        r = d.get("results") or {}
        if not r.get("name"):
            return None
        return {
            "name": r["name"],
            "units": r.get("currency_name", "usd").upper(),
            "frequency": "D",
            "category": ("etf" if str(r.get("type")) in ("ETF", "ETN", "FUND")
                         else "equity"),
            "exchange": r.get("primary_exchange"),
        }
    except Exception:
        return None


def wb_catalog(codes):
    """indicator names + country names — a handful of calls, not thousands."""
    try:
        d = http("https://api.worldbank.org/v2/country"
                 "?format=json&per_page=400")
        for c in (d[1] if isinstance(d, list) and len(d) > 1 else []):
            if c.get("iso2Code"):
                COUNTRY[c["iso2Code"].upper()] = c.get("name")
    except Exception:
        pass
    for code in codes:
        try:
            d = http(f"https://api.worldbank.org/v2/indicator/{code}"
                     "?format=json")
            rows = d[1] if isinstance(d, list) and len(d) > 1 else []
            if rows:
                WB_IND[code] = rows[0].get("name")
        except Exception:
            continue


def decode_econ(code):
    """ECONOMICS:{ISO2}{IND} → human name even when we cannot price it."""
    m = re.match(r"^([A-Z]{2})([A-Z0-9]+)$", code)
    if not m:
        return None
    i2, ind = m.groups()
    country = COUNTRY.get(SS.TV_WB.get(i2, i2)) or COUNTRY.get(i2) or i2
    ind_name = ECON_NAMES.get(ind)
    if not ind_name:
        return f"{country} — {ind} (TradingView economics code)"
    return f"{country} — {ind_name}"


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    smap = (s3_get(MAP_KEY) or {}).get("map") or {}
    wl = s3_get(LISTS_KEY) or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    universe = sorted({s.upper() for l in lists
                       for s in (l.get("symbols") or [])})
    if not universe:
        return {"ok": False, "error": "no watchlists"}

    dic = (s3_get(OUT_KEY) or {}).get("dictionary") or {}
    wb_catalog(sorted({v["id"].split("|")[1] for v in smap.values()
                       if v.get("source") == "WORLDBANK"
                       and "|" in str(v.get("id"))}))
    print(f"[dict] catalog: {len(COUNTRY)} countries, {len(WB_IND)} "
          f"WB indicators, {round(time.time()-t0)}s")

    todo = [s for s in universe
            if s not in dic or not dic[s].get("name")
            or dic[s].get("provisional")]
    print(f"[dict] {len(universe)} symbols, {len(todo)} need a name")

    def resolve(sym):
        m = smap.get(sym) or {}
        src, sid = m.get("source"), m.get("id")
        base = {"source": src, "source_id": sid,
                "confidence": m.get("confidence")}
        # 1. curated (indices / FX / commodities)
        if sid in CURATED:
            n, cat = CURATED[sid]
            return sym, {**base, "name": n, "category": cat, "frequency": "D"}
        # 2. FRED — the authoritative macro name + the FRED ticker itself
        if src == "FRED":
            meta = fred_meta(sid)
            if meta:
                return sym, {**base, **meta}
        # 3. Polygon reference — registered company / fund name
        if src == "MARKET":
            meta = poly_meta(sid)
            if meta:
                return sym, {**base, **meta}
            if sid in CURATED:
                n, cat = CURATED[sid]
                return sym, {**base, "name": n, "category": cat}
        # 4. World Bank — indicator x country
        if src == "WORLDBANK" and "|" in str(sid):
            iso2, code = sid.split("|", 1)
            ind = WB_IND.get(code, code)
            ctry = COUNTRY.get(iso2, iso2)
            return sym, {**base, "name": f"{ctry} — {ind}",
                         "units": "varies", "frequency": "A",
                         "category": "macro"}
        # 5. formula — describe it
        if src == "FORMULA":
            return sym, {**base, "name": f"Composite: {sid}",
                         "category": "formula", "frequency": "D"}
        # 6. crypto
        if src == "COINGECKO":
            return sym, {**base, "name": f"{str(sid).upper()} (crypto)",
                         "category": "crypto", "frequency": "D"}
        # a MAPPED symbol whose authoritative lookup failed is PROVISIONAL:
        # give him something readable now, retry it on the next pass
        if src in ("FRED", "MARKET"):
            ex, t = (sym.split(":", 1) if ":" in sym else ("", sym))
            return sym, {**base, "name": f"{t or sym} ({src}: {sid})",
                         "category": "macro" if src == "FRED" else "equity",
                         "provisional": True}
        # 7. UNMAPPED — still give him a human name
        if sym.startswith("ECONOMICS:"):
            n = decode_econ(sym.split(":", 1)[1])
            if n:
                return sym, {**base, "name": n, "category": "macro",
                             "note": "decoded; not yet priced"}
        if ":" in sym:
            ex, t = sym.split(":", 1)
            return sym, {**base, "name": f"{t} ({ex})",
                         "category": "unmapped"}
        return sym, {**base, "name": sym, "category": "unmapped"}

    fred_todo = [s for s in todo if (smap.get(s) or {}).get("source") == "FRED"]
    rest = [s for s in todo if s not in set(fred_todo)]
    filled = 0
    # FRED: 3 workers, well inside its 120/min ceiling
    with ThreadPoolExecutor(max_workers=3) as ex:
        for sym, meta in ex.map(resolve, fred_todo):
            if meta and meta.get("name"):
                dic[sym] = meta
                filled += 1
            if time.time() - t0 > BUDGET_S:
                break
    print(f"[dict] FRED pass done ({filled}) at {round(time.time()-t0)}s")
    with ThreadPoolExecutor(max_workers=10) as ex:
        for sym, meta in ex.map(resolve, rest):
            if meta and meta.get("name"):
                dic[sym] = meta
                filled += 1
            if time.time() - t0 > BUDGET_S:
                break

    named = sum(1 for s in universe if dic.get(s, {}).get("name")
                and not dic.get(s, {}).get("provisional"))
    provisional = sum(1 for s in universe
                      if dic.get(s, {}).get("provisional"))
    priced = sum(1 for s in universe
                 if dic.get(s, {}).get("source") not in (None, ""))
    doc = {
        "generated_at": now.isoformat(), "version": "1.0",
        "n_symbols": len(universe), "n_named": named,
        "n_provisional": provisional,
        "n_priced": priced, "named_pct": round(100 * named / len(universe), 1),
        "filled_this_run": filled,
        "sources": {"fred": sum(1 for s in universe
                                if dic.get(s, {}).get("source") == "FRED"),
                    "market": sum(1 for s in universe
                                  if dic.get(s, {}).get("source") == "MARKET"),
                    "worldbank": sum(1 for s in universe
                                     if dic.get(s, {}).get("source")
                                     == "WORLDBANK"),
                    "formula": sum(1 for s in universe
                                   if dic.get(s, {}).get("source")
                                   == "FORMULA")},
        "how_to_read": ("Every symbol in Khalid's watchlists with its "
                        "AUTHORITATIVE name and the exact source ticker "
                        "(e.g. FRED:DGS10 = '10-Year Treasury Constant "
                        "Maturity Rate'). Symbols we cannot price yet are "
                        "still decoded to a human name so nothing on the "
                        "page is a bare code."),
        "dictionary": dic,
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json")
    print(json.dumps({"ok": True, "named": named, "of": len(universe),
                      "filled": filled, "elapsed": doc["elapsed_s"]}))
    return {"ok": True, "n_named": named, "n_symbols": len(universe),
            "filled_this_run": filled}
