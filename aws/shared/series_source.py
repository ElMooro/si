"""aws/shared/series_source.py — universal FREE-source series resolver.

Khalid's TradingView symbols are mostly public data behind proprietary
codes. This module maps them to free sources with DEEP history (1990+,
often 1960+), and fetches them.

Sources, in priority order:
  FRED      — US + OECD macro. Deep (1950s+), keyed, reliable.
              Also used as an auto-mapper via its /series/search endpoint.
  STOOQ     — equities, indices, FX, commodities as CSV, no key, back to
              the 1990s (Polygon only reaches ~5y, which is why the first
              thesis study was history-starved).
  DBNOMICS  — OECD/IMF/Eurostat/BIS/World Bank aggregator, no key.
  COINGECKO — crypto, 2013+.

ECONOMICS:* codes are {ISO2}{INDICATOR} (JPGDPYY = Japan GDP YoY). The
curated templates below cover the common indicators against FRED's OECD
mirrors; anything else falls back to a FRED search, cached with its
title + confidence so every mapping stays auditable.
"""

import csv
import io
import json
import os
import re
import urllib.parse
import urllib.request

FRED_KEY = (os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY")
            or "2f057499936072679d8843d7fce99989")

ISO2_ISO3 = {
    "US": "USA", "EU": "EA19", "JP": "JPN", "CN": "CHN", "GB": "GBR",
    "DE": "DEU", "FR": "FRA", "IT": "ITA", "ES": "ESP", "CA": "CAN",
    "AU": "AUS", "IN": "IND", "BR": "BRA", "RU": "RUS", "KR": "KOR",
    "MX": "MEX", "ZA": "ZAF", "TR": "TUR", "CH": "CHE", "SE": "SWE",
    "NO": "NOR", "DK": "DNK", "FI": "FIN", "NL": "NLD", "BE": "BEL",
    "AT": "AUT", "PL": "POL", "PT": "PRT", "GR": "GRC", "IE": "IRL",
    "NZ": "NZL", "ID": "IDN", "TH": "THA", "MY": "MYS", "PH": "PHL",
    "VN": "VNM", "AR": "ARG", "CL": "CHL", "CO": "COL", "PE": "PER",
    "SA": "SAU", "IL": "ISR", "HK": "HKG", "SG": "SGP", "TW": "TWN",
    "CZ": "CZE", "HU": "HUN", "RO": "ROU", "SZ": "CHE", "SY": "SYR",
}

# indicator suffix → FRED series template (OECD Main Economic Indicators
# mirrors carry 1960+ history for most members)
FRED_TEMPLATES = {
    "GDPYY": "NAEXKP01{i3}Q657S",       # real GDP growth YoY
    "IRYY": "CPALTT01{i3}M659N",        # CPI YoY
    "CPI": "CPALTT01{i3}M657N",
    "INTR": "IR3TIB01{i3}M156N",        # short rate
    "IR": "IRLTLT01{i3}M156N",          # long rate (10y)
    "UR": "LRHUTTTT{i3}M156S",          # unemployment
    "M2": "MABMM301{i3}M189S",          # broad money
    "M3": "MABMM301{i3}M189S",
    "CLI": "{i3}LOLITOAASTSAM",         # composite leading indicator
    "BCOI": "BSCICP03{i3}M665S",        # business confidence
    "CCI": "CSCICP03{i3}M665S",         # consumer confidence
    "PROD": "PRINTO01{i3}M657S",        # industrial production
    "EXP": "XTEXVA01{i3}M667S",         # exports
    "IMP": "XTIMVA01{i3}M667S",         # imports
    "BOT": "XTNTVA01{i3}M667S",         # trade balance
    "SP": "SPASTT01{i3}M657N",          # share prices
    "HOU": "HSN1F",
}

# US-specific and market codes that map cleanly
DIRECT = {
    "ECONOMICS:USCBBS": ("FRED", "WALCL"),
    "ECONOMICS:USBBS": ("FRED", "WALCL"),
    "ECONOMICS:EUCBBS": ("FRED", "ECBASSETSW"),
    "ECONOMICS:JPCBBS": ("FRED", "JPNASSETS"),
    "ECONOMICS:USINTR": ("FRED", "FEDFUNDS"),
    "ECONOMICS:USM2": ("FRED", "M2SL"),
    "ECONOMICS:USINBR": ("FRED", "TOTRESNS"),
    "ECONOMICS:USNFP": ("FRED", "PAYEMS"),
    "ECONOMICS:USRRP": ("FRED", "RRPONTSYD"),
    "ECONOMICS:USUR": ("FRED", "UNRATE"),
    "TVC:US02Y": ("FRED", "DGS2"), "TVC:US03MY": ("FRED", "DTB3"),
    "TVC:US10Y": ("FRED", "DGS10"), "TVC:US30Y": ("FRED", "DGS30"),
    "TVC:US05Y": ("FRED", "DGS5"), "TVC:US01Y": ("FRED", "DGS1"),
    "TVC:US03Y": ("FRED", "DGS3"), "TVC:US07Y": ("FRED", "DGS7"),
    "TVC:US06MY": ("FRED", "DGS6MO"), "TVC:US01MY": ("FRED", "DGS1MO"),
    "TVC:VIX": ("FRED", "VIXCLS"),          # 1990+
    "TVC:DXY": ("STOOQ", "^dxy"),           # 1990+ (FRED DTWEXBGS starts 2006)
    "TVC:GOLD": ("STOOQ", "xauusd"),
    "TVC:SILVER": ("STOOQ", "xagusd"),
    "TVC:USOIL": ("FRED", "DCOILWTICO"),
    "TVC:UKOIL": ("FRED", "DCOILBRENTEU"),
    "TVC:SPX": ("STOOQ", "^spx"), "TVC:NDX": ("STOOQ", "^ndx"),
    "TVC:DJI": ("STOOQ", "^dji"), "TVC:NI225": ("STOOQ", "^nkx"),
    "TVC:DAX": ("STOOQ", "^dax"), "TVC:UKX": ("STOOQ", "^ukx"),
    "TVC:HSI": ("STOOQ", "^hsi"), "TVC:SHCOMP": ("STOOQ", "^shc"),
    "TVC:MOVE": ("STOOQ", "^move"),
    "TVC:DE10Y": ("FRED", "IRLTLT01DEM156N"),
    "TVC:JP10Y": ("FRED", "IRLTLT01JPM156N"),
    "TVC:GB10Y": ("FRED", "IRLTLT01GBM156N"),
    "TVC:IT10Y": ("FRED", "IRLTLT01ITM156N"),
    "TVC:FR10Y": ("FRED", "IRLTLT01FRM156N"),
    "CRYPTOCAP:TOTAL": ("COINGECKO", "total"),
    "CRYPTOCAP:BTC.D": ("COINGECKO", "btc_dominance"),
    "INDEX:BTCUSD": ("COINGECKO", "bitcoin"),
}

EQ_EX = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS", "CBOE", "OTC"}
FX_EX = {"FX", "OANDA", "FOREXCOM", "FX_IDC", "SAXO"}
OPS_RE = re.compile(r"[+\-*/()]")
NUM_RE = re.compile(r"^[\d.]+$")


def _http(url, timeout=25, raw=False):
    req = urllib.request.Request(url, headers={"User-Agent": "jh-series/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    return b.decode("utf-8", "replace") if raw else json.loads(b.decode())


# ── mapping ──────────────────────────────────────────────────────────
def map_symbol(sym, fred_search=None):
    """→ (source, id, confidence, note). fred_search: callable for fallback."""
    s = str(sym).strip().upper()
    if not s:
        return None, None, 0, "empty"
    if OPS_RE.search(s):
        return "FORMULA", s, 1.0, "arithmetic over members"
    if s in DIRECT:
        src, sid = DIRECT[s]
        return src, sid, 1.0, "curated"
    if ":" not in s:
        return "STOOQ", f"{s.lower()}.us", 0.9, "bare ticker → US equity"
    ex, t = s.split(":", 1)
    if ex == "FRED":
        return "FRED", t, 1.0, "native"
    if ex in EQ_EX:
        return "STOOQ", f"{t.lower()}.us", 0.9, "US listing"
    if ex in FX_EX:
        return "STOOQ", t.lower(), 0.7, "fx pair"
    if ex == "ECONOMICS":
        m = re.match(r"^([A-Z]{2})([A-Z0-9]+)$", t)
        if m:
            i2, ind = m.groups()
            i3 = ISO2_ISO3.get(i2)
            tpl = FRED_TEMPLATES.get(ind)
            if i3 and tpl:
                return "FRED", tpl.format(i3=i3), 0.85, f"OECD template {ind}"
        if fred_search:
            hit = fred_search(t)
            if hit:
                return "FRED", hit[0], 0.5, f"fred-search: {hit[1][:60]}"
        return None, None, 0, "econ_unmapped"
    if ex == "TVC":
        if fred_search:
            hit = fred_search(t)
            if hit:
                return "FRED", hit[0], 0.5, f"fred-search: {hit[1][:60]}"
        return None, None, 0, "tvc_unmapped"
    if ex in ("CRYPTOCAP", "BINANCE", "COINBASE", "BITSTAMP", "BITFINEX"):
        return "COINGECKO", t.replace("USDT", "").replace("USD", "").lower(), \
            0.6, "crypto"
    return None, None, 0, f"exchange_unsupported:{ex}"


def fred_search_factory(cache):
    """FRED /series/search → best long-history match, memoised."""
    def search(term):
        if term in cache:
            return cache[term] or None
        try:
            q = urllib.parse.quote(term)
            d = _http("https://api.stlouisfed.org/fred/series/search"
                      f"?search_text={q}&api_key={FRED_KEY}&file_type=json"
                      "&limit=5&order_by=popularity&sort_order=desc")
            best = None
            for s in d.get("seriess") or []:
                if s.get("frequency_short") in ("D", "W", "M", "Q"):
                    best = (s["id"], s.get("title", ""))
                    break
            cache[term] = best
            return best
        except Exception:
            cache[term] = None
            return None
    return search


# ── fetchers ─────────────────────────────────────────────────────────
def fetch(source, sid, start="1990-01-01"):
    """→ {ISO date: float}. Never raises."""
    try:
        if source == "FRED":
            d = _http("https://api.stlouisfed.org/fred/series/observations"
                      f"?series_id={sid}&api_key={FRED_KEY}&file_type=json"
                      f"&observation_start={start}")
            out = {}
            for o in d.get("observations") or []:
                v = o.get("value")
                if v not in (".", "", None):
                    try:
                        out[o["date"]] = float(v)
                    except Exception:
                        pass
            return out
        if source == "STOOQ":
            txt = _http(f"https://stooq.com/q/d/l/?s={sid}&i=d", raw=True)
            out = {}
            for row in csv.DictReader(io.StringIO(txt)):
                d_, c = row.get("Date"), row.get("Close")
                if d_ and c and d_ >= start:
                    try:
                        out[d_] = float(c)
                    except Exception:
                        pass
            return out
        if source == "DBNOMICS":
            d = _http(f"https://api.db.nomics.world/v22/series/{sid}"
                      "?observations=1")
            docs = (d.get("series") or {}).get("docs") or []
            if not docs:
                return {}
            per = docs[0].get("period") or []
            val = docs[0].get("value") or []
            return {p: float(v) for p, v in zip(per, val)
                    if isinstance(v, (int, float)) and p >= start[:len(p)]}
        if source == "COINGECKO":
            if sid in ("total", "btc_dominance"):
                return {}
            d = _http(f"https://api.coingecko.com/api/v3/coins/{sid}"
                      "/market_chart?vs_currency=usd&days=max&interval=daily")
            import datetime as _dt
            return {_dt.datetime.utcfromtimestamp(t / 1000).date().isoformat():
                    float(p) for t, p in (d.get("prices") or [])}
    except Exception as e:
        print(f"[series_source] {source}:{sid} failed: {str(e)[:90]}")
    return {}
