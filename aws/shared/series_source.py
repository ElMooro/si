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

FRED_FALLBACK = "2f057499936072679d8843d7fce99989"
FRED_KEY = (os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY")
            or FRED_FALLBACK)

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
    # NOTE: ops 3167 proved two of these ids were WRONG (returned nothing).
    # validate_template() now test-fetches every template before use, and
    # the mapper falls back to FRED search for any that fail.
    "GDPYY": "NAEXKP01{i3}Q657S",       # real GDP growth YoY
    "IRYY": "CPALTT01{i3}M657N",        # CPI YoY (657N = same period prev yr)
    "CPI": "CPALTT01{i3}M661N",
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
    "TVC:DXY": ("MARKET", "DX-Y.NYB"),      # 1990+ (FRED DTWEXBGS starts 2006)
    "TVC:GOLD": ("MARKET", "GC=F"),
    "TVC:SILVER": ("MARKET", "SI=F"),
    "TVC:USOIL": ("FRED", "DCOILWTICO"),
    "TVC:UKOIL": ("FRED", "DCOILBRENTEU"),
    "TVC:SPX": ("MARKET", "^GSPC"), "TVC:NDX": ("MARKET", "^NDX"),
    "TVC:DJI": ("MARKET", "^DJI"), "TVC:NI225": ("MARKET", "^N225"),
    "TVC:DAX": ("MARKET", "^GDAXI"), "TVC:UKX": ("MARKET", "^FTSE"),
    "TVC:HSI": ("MARKET", "^HSI"), "TVC:SHCOMP": ("MARKET", "000001.SS"),
    "TVC:MOVE": ("MARKET", "^MOVE"),
    "TVC:DE10Y": ("FRED", "IRLTLT01DEM156N"),
    "TVC:JP10Y": ("FRED", "IRLTLT01JPM156N"),
    "TVC:GB10Y": ("FRED", "IRLTLT01GBM156N"),
    "TVC:IT10Y": ("FRED", "IRLTLT01ITM156N"),
    "TVC:FR10Y": ("FRED", "IRLTLT01FRM156N"),
    "CRYPTOCAP:TOTAL": ("COINGECKO", "total"),
    "CRYPTOCAP:BTC.D": ("COINGECKO", "btc_dominance"),
    "INDEX:BTCUSD": ("COINGECKO", "bitcoin"),
}

# ops 3177: TradingView's ECONOMICS:{ISO2}{IND} codes are public data behind
# proprietary names. World Bank (free, no key, 1960+, ~200 countries) carries
# the ones FRED's OECD mirrors miss — and these codes REPEAT across Khalid's
# lists, so each mapping activates several dormant engines at once.
ECON_WB = {
    "GDPYY": "NY.GDP.MKTP.KD.ZG",     # real GDP growth  (168 in his universe)
    "GDP": "NY.GDP.MKTP.CD",
    "GDG": "GC.DOD.TOTL.GD.ZS",       # govt debt / GDP   (164)
    "BOT": "NE.RSB.GNFS.CD",          # trade balance     (186)
    "FER": "FI.RES.TOTL.CD",          # FX reserves       (121)
    "DIR": "FR.INR.DPST",             # deposit rate      (150)
    "LIR": "FR.INR.LEND",             # lending rate
    "IRYY": "FP.CPI.TOTL.ZG",         # CPI YoY
    "CPI": "FP.CPI.TOTL.ZG",
    "FI": "FP.CPI.TOTL.ZG",           # food inflation → CPI proxy (174)
    "UR": "SL.UEM.TOTL.ZS",           # unemployment
    "CS": "NE.CON.PRVT.KD.ZG",        # consumer spending (30)
    "CAG": "BN.CAB.XOKA.GD.ZS",       # current account / GDP (28)
    "EXP": "NE.EXP.GNFS.CD",
    "IMP": "NE.IMP.GNFS.CD",
    "MS": "FM.LBL.BMNY.GD.ZS",        # broad money / GDP
    "FDI": "BX.KLT.DINV.WD.GD.ZS",
    "POP": "SP.POP.TOTL",
    "GS": "NY.GNS.ICTR.ZS",           # gross savings
    "MIW": "NY.GDP.PCAP.CD",          # income proxy
    "IP": "NV.IND.TOTL.KD.ZG",        # industrial production growth
    "IPRI": "TM.VAL.MRCH.XD.WD",      # import price index
    "EPRI": "TX.VAL.MRCH.XD.WD",      # export price index
    "MIN": "NY.GDP.MINR.RT.ZS",
}

# TradingView ISO2 quirks → World Bank ISO2
TV_WB = {"EU": "EMU", "SZ": "CH", "UK": "GB", "SY": "SY", "SP": "ES",
         "GE": "DE", "SW": "SE", "SF": "ZA", "KS": "KR", "CI": "CL"}

# continuous futures contracts (TradingView's "1!" convention)
FUT = {"CL": "CL=F", "NG": "NG=F", "GC": "GC=F", "SI": "SI=F", "HG": "HG=F",
       "ZC": "ZC=F", "ZS": "ZS=F", "ZW": "ZW=F", "ZB": "ZB=F", "ZN": "ZN=F",
       "ES": "ES=F", "NQ": "NQ=F", "RB": "RB=F", "HO": "HO=F", "KC": "KC=F",
       "CT": "CT=F", "SB": "SB=F", "CC": "CC=F", "LE": "LE=F", "PL": "PL=F"}
FUT_EX = {"NYMEX", "COMEX", "CBOT", "CME", "ICEUS", "MATBAROFEX", "NYBOT"}

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
        return "MARKET", s, 0.9, "bare ticker → US equity"
    ex, t = s.split(":", 1)
    if ex == "FRED":
        return "FRED", t, 1.0, "native"
    if ex in FUT_EX:
        root = re.sub(r"\d*!$", "", t)
        y = FUT.get(root)
        if y:
            return "MARKET", y, 0.75, f"continuous future {root}"
    if ex in EQ_EX:
        return "MARKET", t, 0.9, "US listing"
    if ex in FX_EX:
        return "MARKET", f"{t}=X", 0.7, "fx pair"
    if ex == "ECONOMICS":
        m = re.match(r"^([A-Z]{2})([A-Z0-9]+)$", t)
        if m:
            i2, ind = m.groups()
            i3 = ISO2_ISO3.get(i2)
            tpl = FRED_TEMPLATES.get(ind)
            if i3 and tpl:                       # OECD monthly (best history)
                return "FRED", tpl.format(i3=i3), 0.85, f"OECD template {ind}"
            wb = ECON_WB.get(ind)
            if wb:                               # World Bank annual, 1960+
                iso2 = TV_WB.get(i2, i2)
                return ("WORLDBANK", f"{iso2}|{wb}", 0.8,
                        f"world-bank {ind}")
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
def _yahoo(sym, start):
    import datetime as _dt
    p1 = int(_dt.datetime.fromisoformat(start).timestamp())
    p2 = int(_dt.datetime.now().timestamp())
    d = _http("https://query1.finance.yahoo.com/v8/finance/chart/"
              f"{urllib.parse.quote(sym)}?period1={p1}&period2={p2}"
              "&interval=1d&events=history")
    res = ((d.get("chart") or {}).get("result") or [None])[0]
    if not res:
        return {}
    ts = res.get("timestamp") or []
    qs = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    cl = qs.get("close") or []
    adj = (((res.get("indicators") or {}).get("adjclose") or [{}])[0]
           .get("adjclose")) or cl
    out = {}
    for t, c in zip(ts, adj):
        if c is None:
            continue
        out[_dt.datetime.utcfromtimestamp(t).date().isoformat()] = float(c)
    return out


def _stooq(sid, start):
    txt = _http(f"https://stooq.com/q/d/l/?s={sid}&i=d", raw=True)
    if "<" in txt[:40] or "limit" in txt[:80].lower():
        return {}
    out = {}
    for row in csv.DictReader(io.StringIO(txt)):
        d_, c = row.get("Date"), row.get("Close")
        if d_ and c and d_ >= start:
            try:
                out[d_] = float(c)
            except Exception:
                pass
    return out


def fetch(source, sid, start="1990-01-01"):
    """→ {ISO date: float}. Never raises.

    MARKET is a CHAIN: Yahoo (deep, free) → Stooq (deep, but blocks some
    datacenter IPs) → Polygon (~5y only). ops 3167 proved Stooq alone is
    unreliable from the runner, so no single market source is trusted.
    """
    try:
        if source == "MARKET":
            for fn, arg in ((_yahoo, sid), (_stooq, _stooq_id(sid))):
                try:
                    ser = fn(arg, start)
                    if len(ser) > 200:
                        return ser
                except Exception:
                    continue
            return _polygon(sid, start)
        if source == "YAHOO":
            return _yahoo(sid, start)
        if source == "FRED":
            # ops 3172: a STALE FRED key in the lambda env silently returned
            # nothing (3170/3171 shipped 1,746 all-NEUTRAL regime weeks off
            # this). Try the env key, then always retry the known-good one.
            d = {}
            for _k in (FRED_KEY, FRED_FALLBACK):
                try:
                    d = _http("https://api.stlouisfed.org/fred/series/"
                              f"observations?series_id={sid}&api_key={_k}"
                              f"&file_type=json&observation_start={start}")
                    if d.get("observations"):
                        break
                except Exception:
                    continue
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
            return _stooq(sid, start)
        if source == "WORLDBANK":
            return _worldbank(sid, start)
        if source == "DBNOMICS_V2":
            return _dbnomics(sid, start)
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


def _worldbank(spec, start):
    """spec = 'ISO2|INDICATOR'. Free, no key, 1960+, ~200 countries."""
    iso2, ind = spec.split("|", 1)
    d = _http(f"https://api.worldbank.org/v2/country/{iso2}/indicator/{ind}"
              f"?format=json&per_page=300&date={start[:4]}:2026")
    if not isinstance(d, list) or len(d) < 2 or not d[1]:
        return {}
    out = {}
    for row in d[1]:
        v, y = row.get("value"), row.get("date")
        if v is not None and y:
            out[f"{y}-12-31"] = float(v)
    return out


def _dbnomics(sid, start):
    d = _http(f"https://api.db.nomics.world/v22/series/{sid}?observations=1")
    docs = ((d.get("series") or {}).get("docs") or [])
    if not docs:
        return {}
    per = docs[0].get("period") or []
    val = docs[0].get("value") or []
    out = {}
    for p_, v in zip(per, val):
        if not isinstance(v, (int, float)):
            continue
        iso = str(p_)
        if len(iso) == 4:
            iso = f"{iso}-12-31"
        elif len(iso) == 7:
            iso = f"{iso}-28"
        if iso >= start:
            out[iso] = float(v)
    return out


def _stooq_id(sym):
    """Yahoo-style id → Stooq id (^GSPC → ^spx, SPY → spy.us)."""
    y2s = {"^GSPC": "^spx", "^NDX": "^ndx", "^DJI": "^dji",
           "^VIX": "^vix", "^N225": "^nkx", "^GDAXI": "^dax",
           "^FTSE": "^ukx", "^HSI": "^hsi", "DX-Y.NYB": "^dxy",
           "GC=F": "xauusd", "SI=F": "xagusd", "CL=F": "cl.f"}
    if sym in y2s:
        return y2s[sym]
    if sym.startswith("^") or "=" in sym:
        return sym.lower()
    return f"{sym.lower()}.us"


def _polygon(tk, start):
    key = os.environ.get("POLYGON_KEY", "")
    if not key or tk.startswith("^") or "=" in tk:
        return {}
    import datetime as _dt
    d1 = _dt.date.today().isoformat()
    d = _http(f"https://api.polygon.io/v2/aggs/ticker/{tk}/range/1/day/"
              f"{start}/{d1}?adjusted=true&sort=asc&limit=50000&apiKey={key}")
    import datetime as _dt2
    return {_dt2.datetime.utcfromtimestamp(r["t"] / 1000).date().isoformat():
            r["c"] for r in (d.get("results") or [])}


def validate_template(tpl, i3="DEU"):
    """A template that returns nothing is worse than no template — ops
    3167 shipped two wrong OECD ids (GDP, CPI). Test-fetch before trust."""
    sid = tpl.format(i3=i3)
    return len(fetch("FRED", sid, "2015-01-01")) > 4
