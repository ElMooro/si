"""justhodl-symdir v1.0.1 -- symbol directory, search and series service (ops 5116/5118).

Khalid: "every single ticker and data (every single one) with its entire
history should be searchable on chart-pro search bar and watchlist and can be
added to the watchlist exactly as in tradingview".

TradingView's symbol search is a server-side directory: symbol, description,
exchange/type, ranked by prefix and popularity, ~100ms. This engine is that
directory for the whole platform:

  mode=build  (daily 05:40 UTC + on demand)
      One document per searchable thing, across every catalog we hold:
        instruments : Polygon reference (stocks/otc/indices/crypto/fx) merged
                      with finviz names/sector/mcap, symbology master,
                      Khalid's TradingView symbol dictionary
        fred        : full FRED catalog (series-meta pages: title/units/freq/
                      popularity) + which of them are banked and WHERE
        eurostat    : 8,152 dataflows (names) + exact Tier-0 series counts;
                      the 564M series themselves are reached through Tier-1
        ecb         : 214 dataflows + Tier-0 counts (3.24M series via Tier-1)
        oecd / bis / imf / statcan : dataflow + cube catalogs (names)
        boj         : every series code + name from the api parts (120k)
        worldbank   : indicator catalog (live API, names) -> countries
        nyfed / ofr / ofr-hfm / ofr-bsrm / te-mirror / archived-fred /
        treasury / fiscaldata / boe / census-us / bls(series titles) / cboe
      Written to data/symdir/: instruments.json.gz (client-side instant
      ticker search), docs.pkl.gz + index.pkl.gz (server search), manifest.

  /search?q=   token search with word-prefix expansion, synonyms, exact and
               prefix id matching, popularity + provider ranking. Series-level
               ids of eurostat/ecb (prov:FLOW:KEY...) are resolved through the
               Tier-1 block maps (one Range read), so every one of the 567M
               series is reachable by id even though only flows are in the
               token index.
  /browse?ds=  series inside a dataset: Tier-1/pages for eurostat+ecb (with
               dimension facets from the scanned sample), cube vectors for
               statcan, countries for a World Bank indicator, DB series for BOJ,
               parent-linked docs for census/treasury/ofr.
  /series?id=  full-history observations for any id, per-provider resolver,
               S3 result cache (data/series-cache/) so repeat opens are one GET.
  /quote?ids=  last / previous / change for watchlist rows (cache-first).
  mode=warm    keep-warm ping: loads the index so the next search is warm.

Doctrine: real data only; the warehouse is the primary source and the live
API is the fallback (or the primary where the warehouse holds only metadata
and the raw file is a multi-hundred-MB scan); never fabricate a name -- a
series without a title shows its code.
"""
import bisect
import csv
import gzip
import hashlib
import io
import json
import math
import os
import pickle
import re
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from array import array
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.config import Config

VERSION = "1.8.1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLYGON_KEY = os.environ.get("POLYGON_KEY", "")
FRED_KEY = os.environ.get("FRED_KEY", "")
BLS_KEY = os.environ.get("BLS_API_KEY", "")
SD = "data/symdir/"
CACHE_PREFIX = "data/series-cache/"
s3 = boto3.client("s3", region_name="us-east-1",
                  config=Config(max_pool_connections=64, retries={"max_attempts": 4}))
UA = "justhodl-symdir/%s (+https://justhodl.ai)" % VERSION

# ---------------------------------------------------------------- helpers

def _now():
    return datetime.now(timezone.utc)


def _iso():
    return _now().isoformat(timespec="seconds")


def _get(key, rng=None):
    kw = {"Bucket": BUCKET, "Key": key}
    if rng:
        kw["Range"] = rng
    return s3.get_object(**kw)["Body"].read()


def _get_json(key, default=None):
    try:
        b = _get(key)
        if key.endswith(".gz") or b[:2] == b"\x1f\x8b":
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:  # noqa: BLE001
        return default


def _put_json(key, obj, cache="public, max-age=300", gz=False):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode()
    if gz:
        body = gzip.compress(body, 6)
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json",
                  CacheControl=cache, **({"ContentEncoding": "gzip"} if gz and not key.endswith(".gz") else {}))
    return len(body)


def _list(prefix, max_keys=None):
    tok, out = None, []
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        d = s3.list_objects_v2(**kw)
        out.extend(d.get("Contents") or [])
        tok = d.get("NextContinuationToken")
        if not tok or (max_keys and len(out) >= max_keys):
            break
    return out


def _http(url, timeout=25, headers=None, data=None, retries=2):
    h = {"User-Agent": UA}
    if headers:
        h.update(headers)
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h, data=data)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read(), r.status, dict(r.headers)
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (429, 500, 502, 503, 504) and i < retries:
                time.sleep(1.2 * (i + 1))
                continue
            raise
        except Exception as e:  # noqa: BLE001
            last = e
            if i < retries:
                time.sleep(0.8 * (i + 1))
                continue
            raise
    raise last


def _http_json(url, timeout=25, headers=None):
    b, _, _ = _http(url, timeout=timeout, headers=headers)
    return json.loads(b.decode("utf-8", "replace"))


_FRED_LAST = [0.0]


def fred_get(url, timeout=30, tries=4):
    """Every FRED call goes through here: >=0.35s spacing and 429 backoff (2s/6s/15s)."""
    for attempt in range(tries):
        wait = 0.35 - (time.time() - _FRED_LAST[0])
        if wait > 0:
            time.sleep(wait)
        try:
            _FRED_LAST[0] = time.time()
            return _http_json(url, timeout=timeout)
        except Exception as e:  # noqa: BLE001
            if "429" in str(e) and attempt < tries - 1:
                time.sleep((2, 6, 15)[min(attempt, 2)])
                continue
            raise


def _f(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v) if math.isfinite(float(v)) else None
        s = str(v).strip().replace(",", "")
        if s in ("", ".", ":", "NA", "N/A", "null", "None", "-", "..", "x", "F"):
            return None
        s = s.split(" ")[0]
        return float(s)
    except Exception:  # noqa: BLE001
        return None


def norm_period(p):
    """Any statistical period string -> ISO YYYY-MM-DD (period start)."""
    if p is None:
        return None
    s = str(p).strip().upper().replace("_", "-")
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) >= 10 and s[4] == "-" and s[7] == "-" and s[10:11] in ("T", " "):
        return s[:10]
    m = re.match(r"^(\d{4})-?W(\d{1,2})$", s)
    if m:
        y, w = int(m.group(1)), int(m.group(2))
        jan4 = date(y, 1, 4)
        mon = jan4 - timedelta(days=jan4.weekday())
        return (mon + timedelta(weeks=w - 1)).isoformat()
    m = re.match(r"^(\d{4})-?Q(\d)$", s)
    if m:
        return "%s-%02d-01" % (m.group(1), (int(m.group(2)) - 1) * 3 + 1)
    m = re.match(r"^(\d{4})-?S(\d)$", s)
    if m:
        return "%s-%02d-01" % (m.group(1), (int(m.group(2)) - 1) * 6 + 1)
    m = re.match(r"^(\d{4})-?M?(\d{2})$", s)
    if m:
        return "%s-%s-01" % (m.group(1), m.group(2))
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", s)
    if m:
        return "%s-%s-%s" % m.groups()
    m = re.match(r"^(\d{4})$", s)
    if m:
        return s + "-01-01"
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)          # dd/mm/yyyy (banxico)
    if m:
        return "%s-%02d-%02d" % (m.group(3), int(m.group(2)), int(m.group(1)))
    m = re.match(r"^(\d{1,2}) ([A-Z]{3}) (\d{4})$", s)          # 31 Jan 2004 (BoE)
    if m:
        mon = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"].index(m.group(2)) + 1
        return "%s-%02d-%02d" % (m.group(3), mon, int(m.group(1)))
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return "%s-%s-%s" % m.groups()
    return None


TOK_RE = re.compile(r"[a-z0-9]+")
STOP = {"the", "of", "and", "in", "for", "to", "by", "a", "an", "on", "at", "from", "all", "vs", "per", "with", "or"}
SYN = {
    "gdp": ["gross", "domestic", "product"], "cpi": ["consumer", "price", "index"], "ppi": ["producer", "price"],
    "unemployment": ["jobless", "unemployed"], "jobless": ["unemployment"], "fedfunds": ["federal", "funds"],
    "fed": ["federal", "reserve"], "treasury": ["treasuries", "government", "bond"], "yield": ["rate"],
    "fx": ["exchange", "rate"], "forex": ["exchange", "rate"], "eur": ["euro"], "usd": ["dollar"], "gbp": ["sterling", "pound"],
    "jpy": ["yen"], "oil": ["crude", "petroleum", "wti", "brent"], "gold": ["xau"], "btc": ["bitcoin"], "eth": ["ethereum"],
    "m2": ["money", "supply"], "payrolls": ["nonfarm", "employment", "payems"], "nfp": ["nonfarm", "payrolls"],
    "inflation": ["cpi", "hicp", "prices"], "hicp": ["inflation", "harmonised"], "housing": ["house", "housing", "homes"],
    "pmi": ["purchasing", "managers"], "ism": ["manufacturing", "pmi"], "vix": ["volatility"], "sofr": ["secured", "overnight"],
    "repo": ["repurchase"], "10y": ["10", "year"], "2y": ["2", "year"], "30y": ["30", "year"], "germany": ["de", "deutschland"],
    "uk": ["united", "kingdom", "gb"], "us": ["united", "states", "usa"], "eu": ["euro", "area", "european", "union"],
    "china": ["cn", "chinese"], "japan": ["jp"], "canada": ["ca"], "retail": ["sales"], "trade": ["exports", "imports"],
    "balance": ["sheet"], "liquidity": ["reserves", "balance"], "spread": ["oas", "spreads"],
}
PROV_WEIGHT = {"instrument": 12, "fred": 8, "ecb": 7, "eurostat": 6, "nyfed": 7, "ofr": 6, "oecd": 5, "bis": 6, "imf": 5,
               "boj": 5, "statcan": 4, "worldbank": 4, "te": 5, "treasury": 5, "fiscaldata": 4, "boe": 5, "census": 4,
               "bls": 6, "cboe": 6, "tv": 9, "ofr-hfm": 4, "ofr-bsrm": 4, "banxico": 4, "snb": 4, "bcb": 4}
PROV_TOKENS = {"statcan": ("canada", "statistic", "canadian"), "boj": ("japan", "bank"), "ecb": ("euro", "european", "central", "bank"),
               "eurostat": ("eu", "europe", "european"), "boe": ("england", "uk", "bank"), "census": ("census", "us"), "bls": ("bls", "labor", "us"),
               "nyfed": ("fed", "new", "york", "us"), "worldbank": ("world", "bank"), "fred": ("fred",), "ofr": ("ofr", "treasury"), "imf": ("imf",),
               "bis": ("bis", "settlement"), "oecd": ("oecd",), "treasury": ("treasury", "us", "fiscal")}
PROV_NAME = {"ustpar": "US Treasury", "official-yields": "Official yields", "fred": "FRED", "ecb": "ECB", "eurostat": "Eurostat", "oecd": "OECD", "bis": "BIS", "imf": "IMF", "boj": "BoJ",
             "statcan": "StatCan", "worldbank": "World Bank", "nyfed": "NY Fed", "ofr": "OFR", "ofr-hfm": "OFR HFM",
             "ofr-bsrm": "OFR BSRM", "te": "TE mirror", "treasury": "FiscalData", "fiscaldata": "FiscalData", "boe": "BoE",
             "census": "Census", "bls": "BLS", "cboe": "Cboe", "tv": "TradingView", "instrument": "Market", "banxico": "Banxico"}


def _stem(t):
    # light plural stemming, applied identically at index and query time: funds->fund, payrolls->payroll,
    # indexes->index; leaves short tokens, digits and -ss/-us/-is words alone
    if len(t) > 3 and t.endswith("s") and not t.endswith(("ss", "us", "is")) and not t.isdigit():
        if t.endswith("ies") and len(t) > 4:
            return t[:-3] + "y"
        if t.endswith("es") and len(t) > 4 and t[-3] in "xsz":
            return t[:-2]
        return t[:-1]
    return t


def tokens(s):
    return [_stem(t) for t in TOK_RE.findall((s or "").lower()) if t not in STOP]


SYN = {_stem(k): [_stem(x) for x in v] for k, v in SYN.items()}      # query and index are both stemmed
PROV_TOKENS = {k: tuple(_stem(x) for x in v) for k, v in PROV_TOKENS.items()}
SYN.setdefault("bitcoin", []).extend(["btc", "btcusd", "xbt"])
SYN.setdefault("ethereum", []).extend(["eth", "ethusd"])
SYN.setdefault("core", []).extend(["less", "excluding", "ex"])
SYN.setdefault("nonfarm", []).extend(["payroll", "payem"])
SYN.setdefault("payroll", []).extend(["nonfarm", "payem", "employee"])
# phrase tags: a title containing the phrase gets the acronym token at index time ("Real Gross Domestic Product" -> gdp)
PHRASE_TAGS = [(("gross", "domestic", "product"), "gdp"), (("consumer", "price", "index"), "cpi"), (("producer", "price", "index"), "ppi"),
               (("purchasing", "manager"), "pmi"), (("federal", "fund"), "fedfund"), (("nonfarm",), "payroll"), (("unemployment", "rate"), "unemployment"),
               (("exchange", "rate"), "fx"), (("balance", "sheet"), "balancesheet"), (("money", "supply"), "m2"), (("treasury",), "treasury"),
               (("harmonised", "index", "consumer", "price"), "hicp"), (("harmonized", "index", "consumer", "price"), "hicp"),
               (("secured", "overnight", "financing"), "sofr"), (("effective", "federal", "fund"), "effr"), (("volatility", "index"), "vix")]
# famous ids whose FRED titles do not say what people call them (search hints, not data): "fed balance sheet" -> WALCL
FAMOUS = {"WALCL": "fed balance sheet total assets federal reserve", "WTREGEN": "treasury general account tga fed", "RRPONTSYD": "reverse repo rrp fed overnight",
          "WRESBAL": "bank reserves fed", "TOTRESNS": "total reserves fed", "BOGMBASE": "monetary base", "DFF": "fed funds effective rate", "FEDFUNDS": "fed funds rate",
          "DGS10": "10 year treasury yield us10y", "DGS2": "2 year treasury yield us02y", "DGS30": "30 year treasury yield us30y", "DGS5": "5 year treasury yield",
          "T10Y2Y": "yield curve 10y 2y spread", "T10Y3M": "yield curve 10y 3m spread", "T10YIE": "10 year breakeven inflation expectations",
          "UNRATE": "unemployment rate jobless", "PAYEMS": "nonfarm payrolls nfp jobs", "ICSA": "initial jobless claims", "CCSA": "continuing jobless claims",
          "CPIAUCSL": "cpi inflation headline", "CPILFESL": "core cpi inflation", "PCEPI": "pce inflation", "PCEPILFE": "core pce inflation",
          "GDPC1": "real gdp", "GDP": "nominal gdp", "M2SL": "m2 money supply", "M1SL": "m1 money supply", "INDPRO": "industrial production",
          "HOUST": "housing starts", "PERMIT": "building permits", "RSAFS": "retail sales", "DGORDER": "durable goods orders", "UMCSENT": "consumer sentiment michigan",
          "DTWEXBGS": "dollar index broad dxy", "DEXUSEU": "eurusd euro dollar", "DEXJPUS": "usdjpy yen", "DEXCHUS": "usdcny yuan", "DEXUSUK": "gbpusd pound",
          "VIXCLS": "vix volatility", "BAMLH0A0HYM2": "high yield spread oas junk", "BAMLC0A0CM": "investment grade spread oas", "DCOILWTICO": "wti crude oil price",
          "DCOILBRENTEU": "brent crude oil price", "GOLDAMGBD228NLBM": "gold price", "SP500": "s&p 500 spx", "NFCI": "financial conditions chicago fed",
          "MORTGAGE30US": "30 year mortgage rate", "TOTALSA": "vehicle sales", "BUSLOANS": "commercial industrial loans", "TB3MS": "3 month t-bill",
          "CPIENGSL": "energy cpi", "PPIACO": "ppi all commodities", "JTSJOL": "jolts job openings", "CIVPART": "labor force participation", "AHETPI": "average hourly earnings wages",
          "PSAVERT": "personal saving rate", "PCE": "personal consumption expenditures", "DSPIC96": "real disposable income", "GFDEBTN": "federal debt total public",
          "FYFSD": "federal deficit surplus", "BOPGSTB": "trade balance goods services", "NETEXP": "net exports", "EXPGS": "exports", "IMPGS": "imports"}


def doc_tokens(d):
    """Tokens indexed for a doc: title + id parts + provider names + phrase tags + famous aliases."""
    ts = set(tokens(d[D_TITLE]))
    idpart = d[D_ID].split(":", 1)[1] if ":" in d[D_ID] and d[D_PROV] != "tv" else d[D_ID]
    ts.update(tokens(idpart))
    ts.add(d[D_PROV])
    ts.update(PROV_TOKENS.get(d[D_PROV], ()))
    tt = tokens(d[D_TITLE])
    tset = set(tt)
    for phrase, tag in PHRASE_TAGS:
        if all(w in tset for w in phrase):
            ts.add(tag)
    if d[D_PROV] == "fred":
        fam = FAMOUS.get(d[D_ID].split(":", 1)[1])
        if fam:
            ts.update(tokens(fam))
    return ts


# doc tuple layout
D_ID, D_PROV, D_TITLE, D_KIND, D_POP, D_UNIT, D_FREQ, D_FIRST, D_LAST, D_KEY, D_N, D_EXTRA = range(12)


def doc(id_, prov, title, kind, pop=0.0, unit=None, freq=None, first=None, last=None, key=None, n=None, extra=None):
    return [id_, prov, (title or "")[:220], kind, round(min(max(pop, 0.0), 1.0), 4), unit, freq, first, last, key, n, extra]


# ================================================================ BUILD

def build(event, context):
    t0 = time.time()
    budget = int(event.get("budget_s") or 600)   # stages; the index build + pickles + uploads need the rest of the 900s
    log = {"version": VERSION, "started_at": _iso(), "sources": {}, "skipped": [], "errors": {}}
    docs, instruments = [], []
    pool = ThreadPoolExecutor(max_workers=32)

    def left():
        return budget - (time.time() - t0)

    def stage(name, fn, optional=True):
        if left() < 40 and optional:
            log["skipped"].append(name)
            return
        ts = time.time()
        n0 = len(docs)
        try:
            info = fn() or {}
            info["docs"] = len(docs) - n0
            info["s"] = round(time.time() - ts, 1)
            log["sources"][name] = info
        except Exception as e:  # noqa: BLE001
            log["errors"][name] = (str(e)[:200] + " | " + traceback.format_exc()[-400:])
            log["sources"][name] = {"docs": len(docs) - n0, "s": round(time.time() - ts, 1), "error": str(e)[:120]}

    # ---------------- instruments: Polygon reference + finviz + symbology + TV dictionary
    def st_instruments():
        fin = (_get_json("data/finviz-universe.json") or {}).get("by_ticker") or {}
        sym = (_get_json("data/symbology/master.json") or {}).get("by_ticker") or {}
        mcap = {t: (v.get("market_cap") or 0) for t, v in fin.items()}
        ranks = {t: i for i, (t, _) in enumerate(sorted(mcap.items(), key=lambda kv: -kv[1]))}
        nfin = max(len(ranks), 1)
        seen = set()
        counts = {}
        for mkt in ("stocks", "otc", "indices", "crypto", "fx"):
            url = ("https://api.polygon.io/v3/reference/tickers?market=%s&active=true&limit=1000&apiKey=%s" % (mkt, POLYGON_KEY))
            pages = 0
            while url and pages < 80 and left() > 60:
                try:
                    d = _http_json(url, timeout=30)
                except Exception as e:  # noqa: BLE001
                    log["errors"]["polygon-" + mkt] = str(e)[:120]
                    break
                pages += 1
                for r in d.get("results") or []:
                    tk = r.get("ticker")
                    if not tk or tk in seen:
                        continue
                    seen.add(tk)
                    name = r.get("name") or ""
                    typ = (r.get("type") or ("index" if mkt == "indices" else mkt)).lower()
                    ex = r.get("primary_exchange") or ("OTC" if mkt == "otc" else mkt.upper())
                    f = fin.get(tk) or {}
                    if f.get("company") and mkt == "stocks":
                        name = f["company"]
                    if mkt == "stocks":
                        pop = 0.35 + 0.65 * (1 - ranks[tk] / nfin) if tk in ranks else 0.25
                    elif mkt == "otc":
                        pop = 0.08
                    elif mkt == "indices":
                        pop = 0.3
                    elif mkt == "crypto":
                        pop = 0.5 if tk.endswith("USD") else 0.3
                    else:
                        pop = 0.3
                    if mkt == "indices" and not name.lower().startswith("dow jones"):
                        pop += 0.05
                    extra = {"ex": ex, "type": typ, "mkt": mkt}
                    if f.get("sector"):
                        extra["sector"] = f["sector"]
                        extra["industry"] = f.get("industry")
                    if f.get("market_cap"):
                        extra["mcap_mm"] = f["market_cap"]
                    if sym.get(tk, {}).get("isin"):
                        extra["isin"] = sym[tk]["isin"]
                    docs.append(doc(tk, "instrument", name, "instrument", pop, extra=extra))
                    instruments.append([tk, name[:80], ex, typ, mkt, round(pop, 3)])
                    counts[mkt] = counts.get(mkt, 0) + 1
                nxt = d.get("next_url")
                url = (nxt + "&apiKey=" + POLYGON_KEY) if nxt else None
        # finviz names for tickers Polygon did not return (e.g. class shares) -> still searchable
        for tk, f in fin.items():
            if tk in seen or not f.get("company"):
                continue
            seen.add(tk)
            pop = 0.35 + 0.65 * (1 - ranks[tk] / nfin) if tk in ranks else 0.25
            docs.append(doc(tk, "instrument", f["company"], "instrument", pop,
                            extra={"ex": "US", "type": "cs", "mkt": "stocks", "sector": f.get("sector"), "industry": f.get("industry")}))
            instruments.append([tk, f["company"][:80], "US", "cs", "stocks", round(pop, 3)])
            counts["finviz-only"] = counts.get("finviz-only", 0) + 1
        # Khalid's TradingView symbol dictionary (his 492 watchlists): EXCHANGE:SYMBOL with the authoritative name
        dic = (_get_json("data/symbol-dictionary.json") or {}).get("dictionary") or {}
        aliases = (_get_json("data/symbol-aliases.json") or {}).get("aliases") or {}       # justhodl-symbol-resolver: bare/full TV symbol -> provider:id
        for full, v in dic.items():
            if not isinstance(v, dict):
                continue
            name = v.get("name") or ""
            src = (v.get("source") or "").upper()
            if src == "FORMULA" or "/" in full or "*" in full or "+" in full:
                continue
            if ":" not in full:
                continue
            ex, symb = full.split(":", 1)
            if ex.upper() == "FRED":
                continue                     # FRED ids come from the catalog with full metadata
            if full in seen:
                continue
            seen.add(full)
            tvpop = 0.58 if ex.upper() in ("TVC", "CBOE", "NASDAQ", "NYSE", "AMEX", "ECONOMICS", "FRED", "INDEX", "CME", "CBOT", "NYMEX", "COMEX", "ICEUS", "ICEEUR", "SP", "DJ") else 0.38
            src_l, src_id = src.lower(), (v.get("source_id") or None)
            if src_l not in ("fred", "ecb", "ofr", "nyfed", "eurostat", "boj", "bls", "worldbank") or not src_id:
                al = aliases.get(full) or aliases.get(symb)
                if isinstance(al, str) and ":" in al:
                    src_l, src_id = al.split(":", 1)[0].lower(), al
            docs.append(doc(full, "tv", name, "instrument", tvpop,
                            extra={"ex": ex, "type": (v.get("category") or "tv").lower(), "mkt": "tv", "src": src_l, "sid": src_id}))
            instruments.append([full, name[:80], ex, (v.get("category") or "tv").lower(), "tv", tvpop])
            counts["tv-dictionary"] = counts.get("tv-dictionary", 0) + 1
        return {"counts": counts, "finviz": len(fin), "symbology": len(sym)}

    # ---------------- FRED: full catalog meta + banked map
    def st_fred():
        keys = _list("data/warm/fred-catalog/series-meta/")
        pages = [k["Key"] for k in keys if k["Key"].endswith(".json")]
        banked = {}
        # 277k banked objects across 19 root folders: list the roots in parallel (one prefix per thread)
        roots = [c["Prefix"] for c in s3.list_objects_v2(Bucket=BUCKET, Prefix="data/warm/fred-scoped/", Delimiter="/").get("CommonPrefixes", [])]
        for objs in pool.map(_list, roots):
            for o in objs:
                parts = o["Key"].split("/")
                if len(parts) == 5 and parts[4].endswith(".json") and not parts[4].startswith("_"):
                    banked[parts[4][:-5]] = parts[3]
        te = {o["Key"].rsplit("/", 1)[-1][:-5] for o in _list("data/warm/te-mirror/") if o["Key"].endswith(".json")}
        arch = {o["Key"].rsplit("/", 1)[-1][:-5] for o in _list("data/warm/archived-fred/") if o["Key"].endswith(".json")}
        best = {}

        def rd(k):
            try:
                return (_get_json(k) or {}).get("rows") or []
            except Exception:  # noqa: BLE001
                return []
        for rows in pool.map(rd, pages):
            for r in rows:
                sid = r.get("id")
                if not sid:
                    continue
                pop = int(r.get("popularity") or 0)
                if sid not in best or pop > best[sid][0]:
                    best[sid] = (pop, r)
        n_b = 0
        for sid, (pop, r) in best.items():
            root = banked.get(sid)
            extra = {}
            if sid in te:
                extra["te"] = 1
            if sid in arch:
                extra["arch"] = 1
            if r.get("seasonal_adj"):
                extra["sa"] = r["seasonal_adj"]
            if root:
                n_b += 1
            docs.append(doc("fred:" + sid, "fred", r.get("title"), "series", 0.15 + 0.85 * min(pop, 100) / 100.0,
                            unit=r.get("units"), freq=r.get("freq"), first=r.get("obs_start"), last=r.get("obs_end"),
                            key=root, extra=extra or None))
        # banked series the category crawl never titled (CPILFESL, PAYEMS, ~1.3k): title from the banked
        # file's meta now, and a real FRED title fetched incrementally (rate-limited ledger, ~150 per build)
        # titles for these come from the ledger that mode=titles fills (hourly, most-popular-first);
        # the build itself never calls the FRED API -- 400 sequential title fetches inside the build
        # pushed it past the 900s Lambda timeout (ops 5119)
        titles = _get_json(SD + "fred-titles.json") or {}
        for sid, t in titles.items():          # ids banked on demand by the resolver (DGS2MO) join the directory
            if t.get("on_demand_root") and sid not in banked:
                banked[sid] = t["on_demand_root"]
        untitled = [sid for sid in banked if sid not in best]
        fetched = 0

        def bank_meta(sid):
            try:
                return (_get_json("data/warm/fred-scoped/%s/%s.json" % (banked[sid], sid)) or {}).get("meta") or {}
            except Exception:  # noqa: BLE001
                return {}
        metas = dict(zip(untitled, pool.map(bank_meta, untitled)))
        for sid in untitled:
            t = titles.get(sid) or {}
            m = metas.get(sid) or {}
            pop = int(t.get("pop") or m.get("popularity") or 0)
            title = t.get("title") or ("%s — %s" % (" · ".join(x for x in (m.get("category"), m.get("root")) if x), sid) if m else sid)
            docs.append(doc("fred:" + sid, "fred", title, "series", 0.15 + 0.85 * min(pop, 100) / 100.0, unit=t.get("units"), freq=t.get("freq"),
                            first=t.get("obs_start"), last=t.get("obs_end"), key=banked[sid],
                            extra={"te": int(sid in te), "arch": int(sid in arch), "untitled": 0 if t.get("title") else 1}))
        # mirrors/archives that are neither catalogued nor banked still get a doc
        for sid in (te | arch) - set(best) - set(banked):
            docs.append(doc("fred:" + sid, "fred", sid, "series", 0.3, extra={"te": int(sid in te), "arch": int(sid in arch)}))
        _put_json(SD + "fred-untitled.json", {"ids": untitled, "banked_root": {sid: banked[sid] for sid in untitled}, "as_of": _iso()}, cache="no-cache")
        return {"meta_pages": len(pages), "catalog": len(best), "banked": n_b, "untitled_banked": len(untitled), "titles_fetched_now": fetched,
                "titles_ledger": sum(1 for v in titles.values() if v.get("title")), "te_mirror": len(te), "archived": len(arch)}

    # ---------------- SDMX dataflow catalogs + Tier-0 counts
    def sdmx_catalog(prov, cat_key, tier0_key, name_fix=None):
        cat = _get_json(cat_key) or {}
        flows = {}
        for f in cat.get("dataflows") or []:
            if f.get("id"):
                flows[f["id"]] = f.get("name") or f["id"]
        if name_fix:
            try:
                name_fix(flows)
            except Exception as e:  # noqa: BLE001
                log["errors"][prov + "-names"] = str(e)[:120]
        t0d = _get_json(tier0_key) or {}
        cnt = {k: (v.get("series") or v.get("est_series_max") or 0) for k, v in (t0d.get("flows") or {}).items()}
        for fid in set(flows) | set(cnt):
            n = cnt.get(fid)
            title = flows.get(fid) or fid
            pop = 0.2 + (min(math.log10(n + 1), 7) / 7) * 0.6 if n else 0.25
            docs.append(doc(prov + ":" + fid, prov, title, "dataset", pop, n=n,
                            extra={"tier1": 1} if n and n > 50000 else None))
        return {"flows": len(flows), "tier0": len(cnt)}

    def ecb_names(flows):
        # catalog names collapsed to ids for many flows; the dataflow endpoint carries the English names
        if sum(1 for k, v in flows.items() if v == k) < 20:
            return
        b, _, _ = _http("https://data-api.ecb.europa.eu/service/dataflow/all?format=sdmx-2.1", timeout=40)
        x = b.decode("utf-8", "replace")
        for m in re.finditer(r'<(?:str:)?Dataflow[^>]*\sid="([^"]+)"[^>]*>(.*?)</(?:str:)?Dataflow>', x, re.S):
            fid = m.group(1)
            nm = re.search(r'<(?:com:)?Name[^>]*xml:lang="en"[^>]*>([^<]+)<', m.group(2)) or re.search(r'<(?:com:)?Name[^>]*>([^<]+)<', m.group(2))
            if nm and fid in flows:
                flows[fid] = nm.group(1).strip()

    def eurostat_names(flows):
        b, _, _ = _http("https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt?lang=en", timeout=60)
        lines = b.decode("utf-8", "replace").split("\n")
        hdr = [h.strip().strip('"').lower() for h in lines[0].split("\t")]
        ti = hdr.index("title") if "title" in hdr else 0
        ci = hdr.index("code") if "code" in hdr else 1
        n = 0
        for ln in lines[1:]:
            p = ln.split("\t")
            if len(p) <= max(ti, ci):
                continue
            code = p[ci].strip().strip('"').strip().upper()
            title = p[ti].strip().strip('"').strip()
            if code in flows and title:
                if flows[code] != title:
                    n += 1
                flows[code] = title
        log["sources"].setdefault("eurostat-toc", {})["renamed"] = n

    def st_eurostat():
        return sdmx_catalog("eurostat", "data/warm/eurostat/catalog.json.gz", "data/index/eurostat/flows.json.gz", eurostat_names)

    def st_ecb():
        return sdmx_catalog("ecb", "data/warm/ecb/catalog.json.gz", "data/index/ecb/flows.json.gz", ecb_names)

    def st_oecd():
        cat = _get_json("data/warm/oecd/catalog.json.gz") or {}
        trip = (_get_json("data/warm/oecd/flow-triplets.json.gz") or {}).get("map") or {}
        n = 0
        for f in cat.get("dataflows") or []:
            if f.get("id"):
                docs.append(doc("oecd:" + f["id"], "oecd", f.get("name") or f["id"], "dataset", 0.3, extra={"triplet": trip.get(f["id"])}))
                n += 1
        return {"flows": n}

    def st_bis():
        cat = _get_json("data/warm/bis/catalog.json.gz") or {}
        n = 0
        for f in cat.get("dataflows") or []:
            if f.get("id"):
                docs.append(doc("bis:" + f["id"], "bis", f.get("name") or f["id"], "dataset", 0.35))
                n += 1
        return {"flows": n}

    def st_imf():
        found = None
        for k in ("data/warm/imf-full/catalog.json.gz", "data/warm/imf-full/catalog.json", "data/warm/imf-full/manifest.json", "data/warm/imf/catalog.json.gz"):
            d = _get_json(k)
            if d:
                found = (k, d)
                break
        n = 0
        if found:
            k, d = found
            items = d.get("dataflows") or d.get("flows") or d.get("datasets") or d.get("items") or []
            if isinstance(items, dict):
                items = [{"id": a, "name": (b.get("name") if isinstance(b, dict) else b)} for a, b in items.items()]
            for f in items:
                fid = f.get("id") or f.get("slug")
                if fid:
                    docs.append(doc("imf:" + fid, "imf", f.get("name") or f.get("title") or fid, "dataset", 0.3))
                    n += 1
        if n == 0:
            for o in _list("data/warm/imf-full/src/"):
                base = o["Key"].rsplit("/", 1)[-1]
                fid = base.split(".")[0]
                if fid and not fid.startswith("_"):
                    docs.append(doc("imf:" + fid, "imf", fid, "dataset", 0.25, key=o["Key"]))
                    n += 1
        return {"flows": n, "catalog": found[0] if found else None}

    def st_statcan():
        cat = _get_json("data/warm/statcan/cube-catalog.json.gz") or {}
        have = {o["Key"].rsplit("/", 1)[-1].split(".")[0] for o in _list("data/warm/statcan/data/")}
        n = 0
        for c in cat.get("payload") or []:
            pid = str(c.get("productId") or "")
            if not pid:
                continue
            docs.append(doc("statcan:" + pid, "statcan", c.get("cubeTitleEn") or pid, "dataset", 0.4,
                            first=str(c.get("cubeStartDate") or "")[:10] or None, last=str(c.get("cubeEndDate") or "")[:10] or None,
                            key=("data/warm/statcan/data/%s.dat.gz" % pid) if pid in have else None,
                            extra={"cansim": c.get("cansimId"), "freq": c.get("frequencyCode")}))
            n += 1
        return {"cubes": n, "banked": len(have)}

    def st_boj():
        objs = [o["Key"] for o in _list("data/warm/boj-full/api/") if o["Key"].endswith(".json.gz")]
        seen = {}

        def rd(k):
            try:
                d = _get_json(k) or {}
                out = []
                db = ((d.get("PARAMETER") or {}).get("DB")) or k.split("/")[4]
                for r in d.get("RESULTSET") or []:
                    sc = r.get("SERIES_CODE")
                    if sc:
                        vals = r.get("VALUES") or {}
                        dts = vals.get("SURVEY_DATES") or []
                        out.append((db, sc, r.get("NAME_OF_TIME_SERIES"), r.get("UNIT"), r.get("FREQUENCY"), r.get("CATEGORY"),
                                    str(dts[0]) if dts else None, str(dts[-1]) if dts else None, len(dts)))
                return out
            except Exception:  # noqa: BLE001
                return []
        partial = False
        for out in pool.map(rd, objs):
            if left() < 150:
                partial = True
                break
            for db, sc, nm, un, fr, cat, f0, l0, n in out:
                cur = seen.get(sc)
                if cur is None:
                    seen[sc] = [db, nm, un, fr, cat, f0, l0, n]
                else:
                    if f0 and (not cur[5] or f0 < cur[5]):
                        cur[5] = f0
                    if l0 and (not cur[6] or l0 > cur[6]):
                        cur[6] = l0
                    cur[7] += n
        dbs = set()
        for sc, (db, nm, un, fr, cat, f0, l0, n) in seen.items():
            dbs.add(db)
            fq = {"MONTHLY": "M", "QUARTERLY": "Q", "ANNUAL": "A", "DAILY": "D", "WEEKLY": "W", "SEMIANNUAL": "S"}.get((fr or "").upper(), fr)
            docs.append(doc("boj:%s:%s" % (db, sc), "boj", nm or sc, "series", 0.3, unit=un, freq=fq,
                            first=norm_period(f0[:6] if f0 and len(f0) >= 6 else f0), last=norm_period(l0[:6] if l0 and len(l0) >= 6 else l0),
                            key=db, n=n, extra={"cat": (cat or "")[:60]}))
        for db in sorted(dbs):
            docs.append(doc("boj:" + db, "boj", "Bank of Japan database " + db, "dataset", 0.3, n=sum(1 for v in seen.values() if v[0] == db)))
        return {"parts": len(objs), "series": len(seen), "dbs": len(dbs), "partial": partial}

    def st_worldbank():
        cat = None
        for k in ("data/warm/worldbank-full/indicators.json.gz", "data/warm/worldbank-full/catalog.json.gz", "data/warm/worldbank/catalog.json.gz"):
            cat = _get_json(k)
            if cat:
                break
        items = []
        if isinstance(cat, dict):
            items = cat.get("indicators") or cat.get("dataflows") or cat.get("items") or []
        if not items:
            d = _http_json("https://api.worldbank.org/v2/indicator?format=json&per_page=30000", timeout=60)
            items = d[1] if isinstance(d, list) and len(d) > 1 else []
            _put_json(SD + "wb-indicators.json.gz", items, gz=True)
        try:
            cs = _http_json("https://api.worldbank.org/v2/country?format=json&per_page=400", timeout=40)
            countries = [{"id": c["id"], "iso2": c.get("iso2Code"), "name": c.get("name"), "region": (c.get("region") or {}).get("value")}
                         for c in (cs[1] if isinstance(cs, list) and len(cs) > 1 else [])]
            _put_json(SD + "wb-countries.json", countries)
        except Exception as e:  # noqa: BLE001
            log["errors"]["wb-countries"] = str(e)[:120]
            countries = _get_json(SD + "wb-countries.json") or []
        n = 0
        for it in items:
            iid = it.get("id")
            if not iid:
                continue
            src = it.get("source") or {}
            topics = ", ".join(t.get("value") or "" for t in (it.get("topics") or []) if t.get("value"))
            docs.append(doc("worldbank:" + iid, "worldbank", it.get("name") or iid, "dataset", 0.28,
                            n=len(countries) or None, extra={"src": (src.get("value") if isinstance(src, dict) else src), "topics": topics[:80] or None}))
            n += 1
        return {"indicators": n, "countries": len(countries)}

    def st_nyfed():
        n = 0
        for rate, nm in (("sofr", "Secured Overnight Financing Rate (SOFR)"), ("effr", "Effective Federal Funds Rate (EFFR)"),
                         ("obfr", "Overnight Bank Funding Rate (OBFR)"), ("tgcr", "Tri-Party General Collateral Rate (TGCR)"),
                         ("bgcr", "Broad General Collateral Rate (BGCR)")):
            docs.append(doc("nyfed:" + rate, "nyfed", nm, "series", 0.95, unit="Percent", freq="D", key="data/warm/nyfed/%s.json.gz" % rate))
            docs.append(doc("nyfed:%s:volume" % rate, "nyfed", nm.split(" (")[0] + " volume ($bn)", "series", 0.6, unit="$bn", freq="D",
                            key="data/warm/nyfed/%s.json.gz" % rate, extra={"field": "volume_bn"}))
            n += 2
        return {"series": n}

    def st_ofr():
        n = 0
        for o in _list("data/warm/ofr/"):
            k = o["Key"]
            if not (k.startswith("data/warm/ofr/dataset-") and k.endswith(".json.gz")):
                continue
            d = _get_json(k) or {}
            pl = d.get("payload") or {}
            dsn = k.rsplit("/", 1)[-1][len("dataset-"):-8]
            docs.append(doc("ofr:" + dsn, "ofr", pl.get("long_name") or pl.get("short_name") or dsn, "dataset", 0.5,
                            n=len(pl.get("timeseries") or {})))
            for mn, ts in (pl.get("timeseries") or {}).items():
                meta = (ts or {}).get("metadata") or {}
                agg = ((ts or {}).get("timeseries") or {}).get("aggregation") or []
                nm = meta.get("long_name") or meta.get("short_name") or meta.get("name") or mn
                docs.append(doc("ofr:" + mn, "ofr", nm, "series", 0.55, unit=meta.get("unit") or meta.get("units"),
                                freq=meta.get("frequency"), first=agg[0][0] if agg else None, last=agg[-1][0] if agg else None,
                                key=k, n=len(agg) or None, extra={"ds": dsn}))
                n += 1
        for prov, pre in (("ofr-hfm", "data/warm/ofr-hfm/series/"), ("ofr-bsrm", "data/warm/ofr-bsrm/series/")):
            for o in _list(pre):
                base = o["Key"].rsplit("/", 1)[-1]
                if base.endswith(".json.gz"):
                    mn = base[:-8]
                    docs.append(doc(prov + ":" + mn, prov, mn.replace("_", " ").replace("-", " · "), "series", 0.35, key=o["Key"]))
                    n += 1
        return {"series": n}

    def st_treasury():
        n = 0
        for o in _list("data/warm/treasury/"):
            k = o["Key"]
            if not k.endswith(".json.gz"):
                continue
            d = _get_json(k) or {}
            dsn = d.get("dataset") or k.rsplit("/", 1)[-1][:-8]
            obs = d.get("observations")
            if isinstance(obs, list) and obs and isinstance(obs[0], dict) and "date" in obs[0]:
                # the fiscaldata engine already reduced this dataset to one headline series
                vk = next((x for x in obs[0].keys() if x not in ("date", "record_date") and _f(obs[0].get(x)) is not None), "value")
                dates = [o.get("date") for o in obs]
                mixed = len(set(dates)) < 0.9 * len(dates)      # several category rows per date flattened into one list
                docs.append(doc("treasury:" + dsn, "treasury", "FiscalData " + dsn.replace("_", " ") + (" (%s)" % vk.replace("_", " ") if vk != "value" else "") + (" (mixed rows)" if mixed else ""),
                                "series", 0.55 if not mixed else 0.3, unit=d.get("unit"), first=norm_period(min(dates)), last=norm_period(max(dates)),
                                key=k, n=len(obs), extra={"field": vk, "ds": dsn, "mixed": int(mixed)}))
                n += 1
                continue
            rows = d.get("payload") or d.get("data") or d.get("rows") or []
            if isinstance(rows, dict):
                rows = rows.get("data") or []
            if not rows:
                continue
            fields = [f for f in rows[0].keys() if f != "record_date" and _f(rows[0].get(f)) is not None]
            dates = [r.get("record_date") for r in rows]
            unique = len(set(dates)) == len(dates)
            docs.append(doc("treasury:" + dsn, "treasury", "FiscalData " + dsn.replace("_", " "), "dataset", 0.45,
                            n=len(fields) if unique else None, key=k, extra={"unique_dates": unique}))
            if unique:
                for f in fields:
                    docs.append(doc("treasury:%s:%s" % (dsn, f), "treasury", "%s — %s" % (dsn.replace("_", " "), f.replace("_", " ")),
                                    "series", 0.4, unit=d.get("unit"), key=k, n=len(rows), extra={"field": f, "ds": dsn}))
                    n += 1
        return {"series": n}

    def st_boe():
        n = 0
        for o in _list("data/warm/boe-full/iadb/"):
            base = o["Key"].rsplit("/", 1)[-1]
            if base.endswith(".csv.gz"):
                code = base[:-7]
                docs.append(doc("boe:" + code, "boe", "Bank of England IADB " + code, "series", 0.4, key=o["Key"]))
                n += 1
        return {"series": n}

    def st_census():
        cat = _get_json("data/warm/census-us/catalog.json.gz") or {}
        n = 0
        for dsd in cat.get("datasets") or []:
            slug = dsd.get("slug")
            if not slug or dsd.get("family") != "eits":
                continue
            title = (dsd.get("title") or slug).replace("Time Series Economic Indicators Time Series -: ", "")
            docs.append(doc("census:" + slug, "census", title, "dataset", 0.5, key="data/warm/census-us/%s/full.json.gz" % slug))
            rows = _get_json("data/warm/census-us/%s/full.json.gz" % slug) or []
            if not rows or len(rows) < 2:
                continue
            hdr = rows[0]
            ix = {h: i for i, h in enumerate(hdr)}
            need = ("data_type_code", "category_code", "seasonally_adj", "time")
            if not all(x in ix for x in need):
                continue
            combos = {}
            for r in rows[1:]:
                try:
                    key = (r[ix["data_type_code"]], r[ix["category_code"]], r[ix["seasonally_adj"]], r[ix["geo_level_code"]] if "geo_level_code" in ix else "US")
                except Exception:  # noqa: BLE001
                    continue
                t = r[ix["time"]]
                c = combos.get(key)
                if c is None:
                    combos[key] = [t, t, 1]
                else:
                    if t < c[0]:
                        c[0] = t
                    if t > c[1]:
                        c[1] = t
                    c[2] += 1
            for (dt, cc, sa, geo), (f0, l0, cnt) in combos.items():
                sid = "census:%s:%s:%s:%s:%s" % (slug, dt, cc, sa, geo)
                docs.append(doc(sid, "census", "%s · %s · %s · %s" % (title[:90], dt, cc, "SA" if str(sa).lower() == "yes" else "NSA"),
                                "series", 0.38, first=norm_period(f0), last=norm_period(l0), key="data/warm/census-us/%s/full.json.gz" % slug, n=cnt,
                                extra={"ds": slug, "dt": dt, "cc": cc, "sa": sa, "geo": geo}))
                n += 1
        return {"series": n}

    def st_bls():
        n, files = 0, []
        for o in _list("data/warm/bls-full/src/"):
            k = o["Key"]
            if k.endswith(".series") and o["Size"] < 60_000_000:
                files.append((k, o["Size"]))
        for k, sz in sorted(files, key=lambda x: x[1]):
            if left() < 90 or n > 400000:
                log["skipped"].append("bls:" + k.rsplit("/", 1)[-1])
                continue
            try:
                txt = _get(k).decode("utf-8", "replace")
            except Exception:  # noqa: BLE001
                continue
            lines = txt.split("\n")
            if not lines:
                continue
            hdr = [h.strip() for h in lines[0].split("\t")]
            if "series_id" not in hdr:
                continue
            ti = hdr.index("series_title") if "series_title" in hdr else None
            if ti is None:
                log["skipped"].append("bls-no-title:" + k.rsplit("/", 1)[-1])
                continue
            si = hdr.index("series_id")
            by = hdr.index("begin_year") if "begin_year" in hdr else None
            ey = hdr.index("end_year") if "end_year" in hdr else None
            surv = k.split("/")[4]
            for ln in lines[1:]:
                p = ln.split("\t")
                if len(p) <= max(si, ti):
                    continue
                sid = p[si].strip()
                if not sid:
                    continue
                docs.append(doc("bls:" + sid, "bls", p[ti].strip(), "series", 0.4,
                                first=(p[by].strip() + "-01-01") if by is not None and len(p) > by and p[by].strip().isdigit() else None,
                                last=(p[ey].strip() + "-01-01") if ey is not None and len(p) > ey and p[ey].strip().isdigit() else None,
                                extra={"survey": surv}))
                n += 1
        return {"series": n, "files": len(files)}

    def st_cboe():
        n = 0
        d = _get_json("data/vix-curve-history.json")
        if isinstance(d, dict):
            docs.append(doc("cboe:vix-curve", "cboe", "Cboe VIX futures term structure history", "dataset", 0.5, key="data/vix-curve-history.json"))
            n += 1
        return {"docs": n}

    stage("instruments", st_instruments, optional=False)
    stage("fred", st_fred, optional=False)
    stage("eurostat", st_eurostat)
    stage("ecb", st_ecb)
    stage("oecd", st_oecd)
    stage("bis", st_bis)
    stage("imf", st_imf)
    stage("statcan", st_statcan)
    stage("nyfed", st_nyfed)
    stage("ofr", st_ofr)
    stage("treasury", st_treasury)
    stage("boe", st_boe)
    stage("census", st_census)
    stage("cboe", st_cboe)
    stage("boj", st_boj)
    stage("worldbank", st_worldbank)
    stage("bls", st_bls)
    pool.shutdown(wait=False)

    # ---------------- dedupe by id (keep the higher-popularity doc)
    by_id = {}
    for d in docs:
        cur = by_id.get(d[D_ID])
        if cur is None or d[D_POP] > cur[D_POP]:
            by_id[d[D_ID]] = d
    docs = list(by_id.values())
    docs.sort(key=lambda d: (-d[D_POP], d[D_ID]))

    # ---------------- inverted index
    post = defaultdict(lambda: array("I"))
    for i, d in enumerate(docs):
        ts = doc_tokens(d)
        ex = d[D_EXTRA] or {}
        for fld in ("cat", "topics", "ds"):
            if ex.get(fld):
                ts.update(tokens(str(ex[fld])))
        for t in ts:
            post[t].append(i)
    index = dict(post)
    toklist = sorted(index)
    ids = sorted((d[D_ID].upper(), i) for i, d in enumerate(docs))
    bare = sorted((d[D_ID].rsplit(":", 1)[-1].upper(), i) for i, d in enumerate(docs) if ":" in d[D_ID])
    pop = array("f", (d[D_POP] for d in docs))
    built_at = _iso()          # ONE stamp for pickle + manifest: the warm-ping reload check compares them
    blob = pickle.dumps({"version": VERSION, "built_at": built_at, "docs": docs, "pop": pop}, protocol=5)
    blob_i = pickle.dumps({"version": VERSION, "index": index, "toklist": toklist, "ids": ids, "bare": bare}, protocol=5)
    gz1 = gzip.compress(blob, 5)
    gz2 = gzip.compress(blob_i, 5)
    s3.put_object(Bucket=BUCKET, Key=SD + "docs.pkl.gz", Body=gz1, ContentType="application/octet-stream", CacheControl="no-cache")
    s3.put_object(Bucket=BUCKET, Key=SD + "index.pkl.gz", Body=gz2, ContentType="application/octet-stream", CacheControl="no-cache")
    instruments.sort(key=lambda r: -r[5])
    ib = gzip.compress(json.dumps({"built_at": built_at, "n": len(instruments), "cols": ["symbol", "name", "exchange", "type", "market", "pop"],
                                   "rows": instruments}, separators=(",", ":")).encode(), 7)
    s3.put_object(Bucket=BUCKET, Key=SD + "instruments.json.gz", Body=ib, ContentType="application/json", ContentEncoding="gzip",
                  CacheControl="public, max-age=21600")
    prov_counts = defaultdict(lambda: {"series": 0, "dataset": 0, "instrument": 0})
    for d in docs:
        prov_counts[d[D_PROV]][d[D_KIND]] += 1
    man = {"version": VERSION, "built_at": built_at, "finished_at": _iso(), "elapsed_s": round(time.time() - t0, 1), "docs": len(docs), "tokens": len(index),
           "postings": sum(len(v) for v in index.values()), "instruments": len(instruments),
           "bytes": {"docs_pkl_gz": len(gz1), "index_pkl_gz": len(gz2), "instruments_json_gz": len(ib)},
           "providers": {k: dict(v) for k, v in prov_counts.items()}, "sources": log["sources"], "skipped": log["skipped"],
           "errors": log["errors"], "series_level_via_tier1": {"eurostat": 564204235, "ecb": 3240832}}
    _put_json(SD + "manifest.json", man, cache="public, max-age=120")
    return man


def fetch_titles(event, context):
    """mode=titles (hourly): real FRED titles for banked ids the category crawl never titled, most
    popular first, ~500 per run at 0.35s spacing (FRED allows 120 req/min). Idempotent; no-op when
    the ledger covers every untitled id. The next build reads the ledger."""
    limit = int(event.get("limit") or 500)
    todo_doc = _get_json(SD + "fred-untitled.json") or {}
    ids, roots = todo_doc.get("ids") or [], todo_doc.get("banked_root") or {}
    titles = _get_json(SD + "fred-titles.json") or {}
    todo = [sid for sid in ids if not (titles.get(sid) or {}).get("title") and (titles.get(sid) or {}).get("tries", 0) < 3]
    if not todo or not FRED_KEY:
        return {"ok": True, "left": len(todo), "fetched": 0, "ledger": sum(1 for v in titles.values() if v.get("title"))}
    pool = ThreadPoolExecutor(max_workers=32)

    def bank_pop(sid):
        if (titles.get(sid) or {}).get("pop") is not None:
            return int(titles[sid]["pop"] or 0)
        try:
            return int(((_get_json("data/warm/fred-scoped/%s/%s.json" % (roots.get(sid), sid)) or {}).get("meta") or {}).get("popularity") or 0)
        except Exception:  # noqa: BLE001
            return 0
    pops = dict(zip(todo, pool.map(bank_pop, todo)))
    pool.shutdown(wait=False)
    todo.sort(key=lambda sid: -pops.get(sid, 0))
    fetched, t0 = 0, time.time()
    for sid in todo[:limit]:
        if context and context.get_remaining_time_in_millis() < 60000:
            break
        try:
            j = fred_get("https://api.stlouisfed.org/fred/series?series_id=%s&api_key=%s&file_type=json" % (urllib.parse.quote(sid), FRED_KEY), timeout=20)
            ser = (j.get("seriess") or [{}])[0]
            if ser.get("title"):
                titles[sid] = {"title": ser["title"], "units": ser.get("units_short"), "freq": ser.get("frequency_short"), "sa": ser.get("seasonal_adjustment_short"),
                               "pop": ser.get("popularity"), "obs_start": ser.get("observation_start"), "obs_end": ser.get("observation_end")}
                fetched += 1
            else:
                titles[sid] = {"title": None, "tries": (titles.get(sid) or {}).get("tries", 0) + 1, "pop": pops.get(sid)}
        except Exception as e:  # noqa: BLE001
            titles[sid] = {"title": None, "tries": (titles.get(sid) or {}).get("tries", 0) + 1, "err": str(e)[:60], "pop": pops.get(sid)}
            if "429" in str(e):
                time.sleep(3)
        time.sleep(0.35)
        if fetched % 50 == 0:
            _put_json(SD + "fred-titles.json", titles, cache="no-cache")
    _put_json(SD + "fred-titles.json", titles, cache="no-cache")
    return {"ok": True, "fetched": fetched, "left": len(todo) - fetched, "ledger": sum(1 for v in titles.values() if v.get("title")), "elapsed_s": round(time.time() - t0, 1)}


# ================================================================ CODELISTS (Eurostat / ECB dimension labels)

SDMX_STRUCT = {
    "eurostat": ["https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/%s?references=descendants&detail=referencepartial",
                 "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/%s?references=descendants"],
    # ECB: no Accept header (the 406 trap from the data lane) and the proven ?format=sdmx-2.1 from ecb_names
    "ecb": ["https://data-api.ecb.europa.eu/service/dataflow/ECB/%s?references=descendants&detail=referencepartial&format=sdmx-2.1",
            "https://data-api.ecb.europa.eu/service/dataflow/all/%s?references=descendants&format=sdmx-2.1",
            "https://data-api.ecb.europa.eu/service/dataflow/ECB.DISS/%s?references=descendants&format=sdmx-2.1"],
}


def _lname(el):
    return el.tag.split("}")[-1]


def parse_sdmx_structure(xml_bytes):
    """SDMX-ML 2.1 structure message -> (dims [(dim_id, codelist_key)], codelists {key: {id, name, codes{code: label}}}).
    Namespace-agnostic; English names preferred. codelist_key = AGENCY:ID."""
    import xml.etree.ElementTree as ET
    root = ET.fromstring(xml_bytes)
    cls, dims = {}, []

    def name_of(el):
        best = None
        for ch in el:
            if _lname(ch) == "Name":
                lang = ch.attrib.get("{http://www.w3.org/XML/1998/namespace}lang", "")
                if lang == "en":
                    return (ch.text or "").strip()
                if best is None:
                    best = (ch.text or "").strip()
        return best or ""
    for el in root.iter():
        ln = _lname(el)
        if ln == "Codelist":
            key = "%s:%s" % (el.attrib.get("agencyID", ""), el.attrib.get("id", ""))
            codes = {}
            for c in el:
                if _lname(c) == "Code":
                    codes[c.attrib.get("id", "")] = name_of(c)
            cls[key] = {"id": el.attrib.get("id"), "agency": el.attrib.get("agencyID"), "name": name_of(el), "codes": codes}
        elif ln == "DimensionList" and not dims:
            for d in el:
                if _lname(d) != "Dimension":
                    continue
                did = d.attrib.get("id", "")
                clk = None
                for e in d.iter():
                    if _lname(e) == "Enumeration":
                        for ref in e:
                            if _lname(ref) == "Ref":
                                clk = "%s:%s" % (ref.attrib.get("agencyID", ""), ref.attrib.get("id", ""))
                dims.append((did, clk))
    return dims, cls


def codelists_lane(event, context):
    """mode=codelists: fan-out orchestrator (fanout=N) or one shard (shard=i, nshards=N) of the dimension-label
    build for eurostat/ecb. Per flow: ONE structure request -> data/symdir/dsd/{prov}/{FLOW}.json (dimension order
    + codelist keys) and the codes merged into data/symdir/cl/{prov}/{KEY}.json (union across flows, so partial
    codelists from many flows converge on the full list). State per shard; big flows first; resumable."""
    prov = (event.get("provider") or "eurostat").lower()
    if event.get("fanout"):
        # orchestrator: {"fanout": 4} for one provider or {"fanout": {"eurostat": 4, "ecb": 1}}; a shard whose
        # state says nothing is left is not invoked (the 20-minute schedule then costs nothing once complete)
        plan = event["fanout"] if isinstance(event["fanout"], dict) else {prov: int(event["fanout"])}
        out = {}
        lc = boto3.client("lambda", region_name="us-east-1")
        for pv, n in plan.items():
            n = int(n)
            out[pv] = []
            for i in range(n):
                st = _get_json("%s_state/codelists-%s-%d.json" % (SD, pv, i)) or {}
                if st.get("left") == 0 and (st.get("nshards") == n):
                    continue
                lc.invoke(FunctionName=context.function_name, InvocationType="Event",
                          Payload=json.dumps({"mode": "codelists", "provider": pv, "shard": i, "nshards": n}).encode())
                out[pv].append(i)
        return {"ok": True, "fanned_out": out}
    shard, nshards = int(event.get("shard") or 0), int(event.get("nshards") or 1)
    st_key = "%s_state/codelists-%s-%d.json" % (SD, prov, shard)
    st = _get_json(st_key) or {"done": [], "failed": {}, "shard": shard, "nshards": nshards}
    done, failed = set(st.get("done") or []), st.get("failed") or {}
    flows = tier0(prov).get("flows") or {}
    def shard_of(f):        # stable across processes (str hash is randomised per interpreter)
        return int(hashlib.md5(f.encode()).hexdigest(), 16) % nshards
    todo = sorted((f for f in flows if shard_of(f) == shard and f not in done and failed.get(f, {}).get("tries", 0) < 3),
                  key=lambda f: -(flows[f].get("series") or 0))
    t0, n_ok, n_fail, budget = time.time(), 0, 0, int(event.get("budget_s") or 780)
    for f in todo:
        if time.time() - t0 > budget or (context and context.get_remaining_time_in_millis() < 45000):
            break
        got, err = None, "no xml"
        for tmpl in SDMX_STRUCT[prov]:
            try:
                b, status, _ = _http(tmpl % urllib.parse.quote(f), timeout=60, headers=({"Accept": "application/xml"} if prov == "eurostat" else None), retries=1)
                if b and b.lstrip().startswith((b"<", b"\xef\xbb\xbf")):
                    got = b
                    break
                err = "non-xml %d" % status
            except Exception as e:  # noqa: BLE001
                err = str(e)[:80]
        if not got:
            failed[f] = {"tries": failed.get(f, {}).get("tries", 0) + 1, "err": err}
            n_fail += 1
            continue
        try:
            dims, cls = parse_sdmx_structure(got)
        except Exception as e:  # noqa: BLE001
            failed[f] = {"tries": failed.get(f, {}).get("tries", 0) + 1, "err": "parse:" + str(e)[:60]}
            n_fail += 1
            continue
        if not dims:
            failed[f] = {"tries": failed.get(f, {}).get("tries", 0) + 1, "err": "no dimensions"}
            n_fail += 1
            continue
        for key, cl in cls.items():
            ck = "%scl/%s/%s.json" % (SD, prov, key.replace(":", "__"))
            cur = _get_json(ck) or {"id": cl["id"], "agency": cl["agency"], "name": cl["name"], "codes": {}}
            merged = dict(cur.get("codes") or {})
            merged.update({c: n for c, n in cl["codes"].items() if n})
            if len(merged) != len(cur.get("codes") or {}) or not cur.get("name"):
                cur["codes"], cur["n"], cur["name"], cur["as_of"] = merged, len(merged), cur.get("name") or cl["name"], _iso()
                _put_json(ck, cur, cache="public, max-age=3600")
        _put_json("%sdsd/%s/%s.json" % (SD, prov, f), {"flow": f, "provider": prov, "dims": [{"id": d, "cl": c} for d, c in dims], "as_of": _iso()}, cache="public, max-age=3600")
        done.add(f)
        failed.pop(f, None)
        n_ok += 1
        if n_ok % 25 == 0:
            st.update({"done": sorted(done), "failed": failed, "updated_at": _iso()})
            _put_json(st_key, st, cache="no-cache")
        time.sleep(0.25)
    st.update({"done": sorted(done), "failed": failed, "updated_at": _iso(), "left": len([f for f in todo if f not in done and failed.get(f, {}).get("tries", 0) < 3]),
               "total_shard": len([f for f in flows if shard_of(f) == shard]), "last_run": {"ok": n_ok, "fail": n_fail, "s": round(time.time() - t0, 1)}})
    _put_json(st_key, st, cache="no-cache")
    return {"ok": True, "provider": prov, "shard": shard, "nshards": nshards, "done": len(done), "left": st["left"], "ok_now": n_ok, "fail_now": n_fail, "elapsed_s": round(time.time() - t0, 1)}


_DSD_CACHE, _CL_CACHE = {}, {}


def labeler(prov, flow):
    """Return (dims, fn(pos, code) -> label|None) for a flow, or (None, None) when no DSD has been built yet."""
    k = prov + "/" + flow
    if k not in _DSD_CACHE:
        _DSD_CACHE[k] = _get_json("%sdsd/%s/%s.json" % (SD, prov, flow))
        if len(_DSD_CACHE) > 400:
            _DSD_CACHE.pop(next(iter(_DSD_CACHE)))
    dsd = _DSD_CACHE[k]
    if not dsd:
        return None, None
    dims = dsd.get("dims") or []
    maps = []
    for d in dims:
        ck = d.get("cl")
        if ck and ck not in _CL_CACHE:
            _CL_CACHE[ck] = ((_get_json("%scl/%s/%s.json" % (SD, prov, ck.replace(":", "__"))) or {}).get("codes") or {})
            if len(_CL_CACHE) > 600:
                _CL_CACHE.pop(next(iter(_CL_CACHE)))
        maps.append(_CL_CACHE.get(ck) or {})

    def fn(pos, code):
        if pos < len(maps):
            return maps[pos].get(code)
        return None
    return dims, fn


def label_key(prov, flow, key, fn=None, dims=None, max_len=220):
    """'A.CLV05_MEUR.D31.BA' -> 'Annual · Chain linked volumes (2005), million euro · Changes in inventories · Bosnia and Herzegovina'."""
    if fn is None:
        dims, fn = labeler(prov, flow)
    if fn is None:
        return None
    parts = key.split(".")
    if prov == "ecb" and parts and parts[0] == flow:      # ECB keys carry the flow as the first element
        parts = parts[1:]
    out = []
    for i, c in enumerate(parts):
        lab = fn(i, c)
        out.append(lab if lab else c)
    txt = " · ".join(out)
    return txt if len(txt) <= max_len else txt[:max_len - 1] + "…"


def fred_updates(event, context):
    """mode=fredupdates (every 15 min): FRED publishes which series changed (fred/series/updates, ordered by
    last_updated). Pull the window since the last cursor, intersect with the bank, heal those tails only --
    the warehouse follows the source's own release clock instead of a guessed cadence."""
    if not FRED_KEY:
        return {"ok": False, "error": "no FRED_KEY"}
    ix = load_index()
    st = _get_json(SD + "_state/fredupdates.json") or {}
    now = _now()
    start = st.get("cursor") or (now - timedelta(hours=6)).strftime("%Y%m%d%H%M")
    end = now.strftime("%Y%m%d%H%M")
    banked = {}
    for d in ix["docs"]:
        if d[D_PROV] == "fred" and d[D_KEY]:
            banked[d[D_ID].split(":", 1)[1]] = d
    updated, healed, added, offset, pages = [], 0, 0, 0, 0
    t0 = time.time()
    while pages < 25 and (not context or context.get_remaining_time_in_millis() > 60000):
        j = None
        for attempt in range(4):
            try:
                j = fred_get("https://api.stlouisfed.org/fred/series/updates?api_key=%s&file_type=json&start_time=%s&end_time=%s&limit=1000&offset=%d"
                             % (FRED_KEY, start, end, offset), timeout=40)
                break
            except Exception as e:  # noqa: BLE001
                if "429" in str(e) and attempt < 3:
                    time.sleep(20 * (attempt + 1))        # FRED: 120 req/min shared with the hourly rotation and heal-on-open
                    continue
                st.update({"last_error": str(e)[:120], "updated_at": _iso()})
                _put_json(SD + "_state/fredupdates.json", st, cache="no-cache")
                return {"ok": False, "error": str(e)[:120]}
        rows = j.get("seriess") or []
        pages += 1
        for r in rows:
            sid = r.get("id")
            if sid in banked:
                updated.append(sid)
        if len(rows) < 1000:
            break
        offset += 1000
        time.sleep(0.5)
    for sid in updated:
        if context and context.get_remaining_time_in_millis() < 45000:
            break
        d = banked[sid]
        try:
            j = _get_json("data/warm/fred-scoped/%s/%s.json" % (d[D_KEY], sid))
            j, k = _fred_tail_refresh(d[D_KEY], sid, j, d[D_FREQ], force=True)
            if k:
                healed += 1
                added += k
                time.sleep(0.45)
        except Exception:  # noqa: BLE001
            continue
    st.update({"cursor": end, "updated_at": _iso(), "window": [start, end], "fred_updated": len(updated), "healed": healed, "obs_added": added, "pages": pages, "s": round(time.time() - t0, 1)})
    _put_json(SD + "_state/fredupdates.json", st, cache="no-cache")
    return {"ok": True, "window": [start, end], "fred_updated_in_bank": len(updated), "healed": healed, "obs_added": added, "pages": pages}


def fred_fresh(event, context):
    """mode=fredfresh (hourly): keep the headline banked FRED series current without waiting for an open --
    walk banked D/W series most-popular-first (cursor in data/symdir/_state/fredfresh.json), ~limit per run
    at FRED's pace, tail-merge in place. Everything else heals on open (r_fred)."""
    ix = load_index()
    limit = int(event.get("limit") or 400)
    st = _get_json(SD + "_state/fredfresh.json") or {"cursor": 0}
    cands = [d for d in ix["docs"] if d[D_PROV] == "fred" and d[D_KEY] and (d[D_FREQ] or "")[:1].upper() in ("D", "W") and d[D_POP] >= 0.3]
    cands.sort(key=lambda d: (-d[D_POP], d[D_ID]))
    n = len(cands)
    if not n:
        return {"ok": True, "n": 0}
    cur = int(st.get("cursor") or 0) % n
    done, healed, added = 0, 0, 0
    t0 = time.time()
    for i in range(limit):
        if context and context.get_remaining_time_in_millis() < 30000:
            break
        d = cands[(cur + i) % n]
        sid = d[D_ID].split(":", 1)[1]
        try:
            j = _get_json("data/warm/fred-scoped/%s/%s.json" % (d[D_KEY], sid))
            j, k = _fred_tail_refresh(d[D_KEY], sid, j, d[D_FREQ])
            done += 1
            if k:
                healed += 1
                added += k
                time.sleep(0.7)          # only real FRED calls pace the run (120/min shared)
        except Exception:  # noqa: BLE001
            continue
    st.update({"cursor": (cur + done) % n, "n": n, "updated_at": _iso(), "last_run": {"done": done, "healed": healed, "obs_added": added, "s": round(time.time() - t0, 1)}})
    _put_json(SD + "_state/fredfresh.json", st, cache="no-cache")
    return {"ok": True, "n": n, "done": done, "healed": healed, "obs_added": added, "cursor": st["cursor"]}


# ================================================================ RUNTIME INDEX

_IDX = {"loaded_at": None, "docs": None, "pop": None, "index": None, "toklist": None, "ids": None, "bare": None, "built_at": None}
_T0CACHE, _T1CACHE = {}, {}


def load_index(force=False):
    if _IDX["docs"] is not None and not force:
        return _IDX
    if force:
        for k in ("docs", "pop", "index", "toklist", "ids", "bare"):
            _IDX[k] = None
        import gc
        gc.collect()
    t = time.time()
    a = pickle.loads(gzip.decompress(_get(SD + "docs.pkl.gz")))
    b = pickle.loads(gzip.decompress(_get(SD + "index.pkl.gz")))
    _IDX.update({"docs": a["docs"], "pop": a["pop"], "index": b["index"], "toklist": b["toklist"], "ids": b["ids"], "bare": b["bare"],
                 "loaded_at": _iso(), "built_at": a.get("built_at"), "load_s": round(time.time() - t, 2)})
    return _IDX


def _prefix_range(sorted_pairs, prefix, cap=400):
    lo = bisect.bisect_left(sorted_pairs, (prefix,))
    hi = bisect.bisect_right(sorted_pairs, (prefix + "\uffff",))
    return sorted_pairs[lo:min(hi, lo + cap)], hi - lo


def _expand(tok, toklist, index, cap=250):
    lo = bisect.bisect_left(toklist, tok)
    hi = bisect.bisect_right(toklist, tok + "\uffff")
    cands = toklist[lo:hi]
    if len(cands) > cap:
        exact = [tok] if tok in index else []
        cands = exact + sorted((c for c in cands if c != tok), key=lambda c: -len(index[c]))[:cap]
    return cands


def doc_row(d, score=None):
    ex = d[D_EXTRA] or {}
    prov = d[D_PROV]
    kind = d[D_KIND]
    chartable = (kind == "series") or (kind == "instrument")
    if prov in ("oecd", "bis", "imf") and kind == "dataset":
        chartable = False
    row = {"id": d[D_ID], "symbol": d[D_ID].split(":", 1)[1] if (":" in d[D_ID] and prov != "tv" and kind != "instrument") else d[D_ID],
           "name": d[D_TITLE], "provider": prov, "provider_name": PROV_NAME.get(prov, prov.upper()), "kind": kind, "chartable": chartable,
           "unit": d[D_UNIT], "freq": d[D_FREQ], "first": d[D_FIRST], "last": d[D_LAST], "n": d[D_N], "pop": d[D_POP]}
    if kind == "dataset":
        row["browse"] = prov in ("eurostat", "ecb", "statcan", "worldbank", "boj", "census", "treasury", "ofr")
    for k in ("ex", "type", "mkt", "sector", "industry", "sa", "te", "arch", "src", "ds"):
        if ex.get(k) is not None:
            row[k] = ex[k]
    if score is not None:
        row["score"] = round(score, 2)
    return row


def search(q, limit=40, prov=None, kind=None):
    ix = load_index()
    docs, index, toklist, pop = ix["docs"], ix["index"], ix["toklist"], ix["pop"]
    q = (q or "").strip()
    if not q:
        return {"q": q, "rows": [], "total": 0}
    Q = q.upper().replace(" ", "")
    hits = {}
    detail = {"id_exact": 0, "id_prefix": 0, "token_and": 0, "token_or": 0}
    # 1. exact + prefix on ids (with and without provider prefix)
    id_hit = set()
    for pairs, isbare in ((ix["ids"], False), (ix["bare"], True)):
        rows, total = _prefix_range(pairs, Q, cap=300)
        for k, i in rows:
            # an exact id match scores the same with or without the provider prefix ("sofr" must find
            # nyfed:sofr as strongly as the SOFR ETF; popularity then orders them); prefix matches on the
            # bare part are discounted a little against full-id prefix matches
            base = 200 if k == Q else max(0, 90 - (len(k) - len(Q)) * 2) * (0.92 if isbare else 1.0)
            if base > hits.get(i, (0, 0))[0]:
                hits[i] = (base, 0)
            if k == Q:
                id_hit.add(i)
            detail["id_exact" if k == Q else "id_prefix"] += 1
    # 2. token search: word-prefix expansion on the query tokens; synonyms match EXACT index tokens only
    #    (prefix-expanding a synonym like "de" for germany turned "degree" into a match) and count less
    toks = tokens(q)
    syn_only = defaultdict(int)
    if toks:
        groups = []
        for t in toks:
            direct = set()
            for c in _expand(t, toklist, index):
                direct.update(index[c])
            via_syn = set()
            for syn in SYN.get(t, []):
                if syn in index:
                    via_syn.update(index[syn])
            via_syn -= direct
            for i in via_syn:
                syn_only[i] += 1
            groups.append(direct | via_syn)
        inter = set.intersection(*groups) if groups and all(groups) else set()
        if inter:
            detail["token_and"] = len(inter)
            for i in inter:
                b = 60 + (10 if len(toks) > 1 else 0) - 10 * syn_only.get(i, 0)
                if b > hits.get(i, (0, 0))[0]:
                    hits[i] = (b, len(toks))
        if len(inter) < limit and len(toks) > 1:
            cnt = defaultdict(int)
            for g in groups:
                for i in g:
                    cnt[i] += 1
            for i, c in cnt.items():
                if i in inter or c < max(1, len(toks) - 1):
                    continue
                b = 25 + 12 * c - 10 * syn_only.get(i, 0)
                if b > hits.get(i, (0, 0))[0]:
                    hits[i] = (b, c)
            detail["token_or"] = sum(1 for c in cnt.values() if c >= max(1, len(toks) - 1))
    # 3. series-level drill for prov:FLOW:KEY-shaped queries against the 567M-series Tier-1 index --
    #    runs whether or not the token index matched anything (a full series id never matches a doc)
    drill = None
    m = re.match(r"^(eurostat|ecb):([A-Za-z0-9_@\-]+)(?::(.*))?$", q, re.I)
    if m:
        try:
            drill = tier1_prefix(m.group(1).lower(), m.group(2).upper(), (m.group(3) or "").upper(), 40)
        except Exception as e:  # noqa: BLE001
            drill = {"error": str(e)[:120], "rows": []}
        # the dataset itself is a hit too (query = its id + a key)
        rows_ds, _ = _prefix_range(ix["ids"], (m.group(1) + ":" + m.group(2)).upper(), cap=3)
        for k, i in rows_ds:
            if k == (m.group(1) + ":" + m.group(2)).upper() and i not in hits:
                hits[i] = (120, 0)
    if not hits and not (drill and drill.get("rows")):
        return {"q": q, "rows": [], "total": 0, "detail": detail, "suggest": _suggest(q, toklist, index), "series_hits": drill}
    ql = q.lower()
    scored = []
    facets_all = defaultdict(int)
    for i, (b, nt) in hits.items():
        d = docs[i]
        facets_all[d[D_PROV]] += 1
        if prov and d[D_PROV] != prov:
            continue
        if kind and d[D_KIND] != kind:
            continue
        pw = PROV_WEIGHT.get(d[D_PROV], 3)
        if d[D_KIND] == "instrument" and i not in id_hit:
            pw = 4          # a company NAME (or a symbol merely starting with the query) is rarely what a macro query wants
        s = b + pop[i] * 28 + pw
        tl = d[D_TITLE].lower()
        if tl.startswith(ql):
            s += 14
        elif ql in tl:
            s += 10 if d[D_KIND] == "dataset" else 6      # a dataset whose title carries the whole phrase opens N series on the topic
        if d[D_KIND] == "dataset":
            s += 3 if (d[D_N] or 0) > 100 else 0
            if toks and len(toks) > 1 and d[D_PROV] in ("statcan", "eurostat", "ecb", "worldbank", "ofr", "boj", "census", "treasury"):
                s += 8          # a browsable dataset is a strong answer to a topical multi-word query
        if d[D_PROV] == "fred" and d[D_ID].split(":", 1)[1] in FAMOUS:
            s += 6
        scored.append((s, i))
    scored.sort(key=lambda x: (-x[0], docs[x[1]][D_ID]))
    rows = [doc_row(docs[i], s) for s, i in scored[:limit]]
    facets = facets_all          # counts over every provider, even when one is filtered in
    for row in rows:
        if row["provider"] == "tv" and ":" in row["id"]:
            srcs = sources_for(row["id"])
            if srcs:
                row["sources"] = srcs
            elif row["id"].split(":")[0].upper() in ("ECONOMICS", "TVC") and not (row.get("src") in ("fred", "ecb", "yahoo") and row.get("sid")):
                row["sources"] = []                      # TradingView-only: no warehouse source yet -> shown as such
    out = {"q": q, "rows": rows, "total": len(scored), "detail": detail,
           "facets": sorted(({"provider": p, "provider_name": PROV_NAME.get(p, p.upper()), "n": n} for p, n in facets.items()), key=lambda x: -x["n"])}
    if drill is not None:
        out["series_hits"] = drill
    return out


def _suggest(q, toklist, index, n=8):
    toks = tokens(q)
    if not toks:
        return []
    t = toks[-1]
    for pre in (t, t[:4], t[:3], t[:2]):
        if len(pre) < 2:
            break
        c = _expand(pre, toklist, index, cap=400)
        c = sorted(c, key=lambda x: -len(index[x]))[:n]
        if c:
            return c
    return []


# ---------------- Tier-0 / Tier-1 access for eurostat + ecb

def tier0(prov):
    if prov not in _T0CACHE:
        _T0CACHE[prov] = _get_json("data/index/%s/flows.json.gz" % prov) or {"flows": {}}
    return _T0CACHE[prov]


def blocks_map(prov, flow):
    k = prov + "/" + flow
    if k not in _T1CACHE:
        _T1CACHE[k] = _get_json("data/index/%s/t1/%s.blocks.json" % (prov, flow))
        if len(_T1CACHE) > 300:
            _T1CACHE.pop(next(iter(_T1CACHE)))
    return _T1CACHE[k]


def _t1_read_block(prov, flow, b):
    txt = _get("data/index/%s/t1/%s.jsonl" % (prov, flow), rng="bytes=%d-%d" % (b["o"], b["o"] + b["c"] - 1)).decode("utf-8", "replace")
    out = []
    for ln in txt.split("\n"):
        ln = ln.strip()
        if ln.startswith("{") and ln.endswith("}"):
            try:
                out.append(json.loads(ln))
            except Exception:  # noqa: BLE001
                pass
    return out


def _t1_row(prov, flow, e, fn=None, dims=None):
    key = e["id"].split(":", 2)[-1]
    lab = label_key(prov, flow, key, fn, dims) if fn else None
    return {"id": e["id"], "symbol": key, "name": lab or ("%s · %s" % (flow, key)), "labeled": bool(lab), "provider": prov,
            "provider_name": PROV_NAME.get(prov), "kind": "series", "chartable": True, "geo": e.get("g"), "first": e.get("f"), "last": e.get("l"),
            "n": e.get("n"), "last_value": e.get("v"), "flow": flow}


def _page_rows(prov, flow, pnum):
    d = _get_json("data/providers/%s/series/page-%04d.json" % (prov, pnum)) or {}
    return [r for r in (d.get("rows") or []) if r.get("flow") == flow]


def _page_row(prov, flow, r, fn=None, dims=None):
    key = r["id"].split(":", 2)[-1]
    lab = label_key(prov, flow, key, fn, dims) if fn else None
    return {"id": r["id"], "symbol": key, "name": lab or r.get("name") or r["id"], "labeled": bool(lab), "provider": prov, "provider_name": PROV_NAME.get(prov),
            "kind": "series", "chartable": True, "geo": r.get("geo"), "first": r.get("first_obs"), "last": r.get("last_obs"), "n": r.get("n_obs"),
            "last_value": r.get("last_value"), "unit": r.get("unit"), "freq": r.get("freq"), "flow": flow, "dims": r.get("dims")}


def tier1_prefix(prov, flow, keypfx, limit=40):
    """Exact/prefix series-id lookup inside one flow: binary-search the block map, one Range read."""
    t0d = tier0(prov)
    flows = t0d.get("flows") or {}
    if flow not in flows:
        cands = [f for f in flows if f.startswith(flow)][:12]
        return {"flow": flow, "known": False, "flows_matching": cands, "rows": []}
    pfx = "%s:%s:%s" % (prov, flow, keypfx)
    bm = blocks_map(prov, flow)
    rows = []
    ldims, lfn = labeler(prov, flow)
    if bm and bm.get("blocks"):
        bl = bm["blocks"]
        keys = [b["k"] for b in bl]
        i = max(0, bisect.bisect_right(keys, pfx) - 1)
        for j in range(i, min(i + 3, len(bl))):
            for e in _t1_read_block(prov, flow, bl[j]):
                if e["id"].startswith(pfx):
                    rows.append(_t1_row(prov, flow, e, lfn, ldims))
                    if len(rows) >= limit:
                        break
            if len(rows) >= limit or (bl[j]["k"] > pfx + "\uffff"):
                break
        return {"flow": flow, "known": True, "tier": 1, "total_in_flow": bm.get("n"), "rows": rows[:limit]}
    rec = flows[flow]
    lo, hi = rec.get("lo", 0), min(rec.get("hi", 0), rec.get("lo", 0) + 160)
    pages = list(range(lo, hi + 1))
    with ThreadPoolExecutor(16) as ex:
        for prs in ex.map(lambda p: _page_rows(prov, flow, p), pages):
            for r in prs:
                if r["id"].startswith(pfx):
                    rows.append(_page_row(prov, flow, r, lfn, ldims))
            if len(rows) >= limit:
                break
    return {"flow": flow, "known": True, "tier": 0, "total_in_flow": rec.get("series"), "pages_scanned": len(pages), "rows": rows[:limit]}


def browse(ds, q="", limit=200, offset=0):
    """List series inside a dataset, with a text filter and dimension facets from the scanned sample."""
    ds = (ds or "").strip()
    q = (q or "").strip()
    prov, _, rest = ds.partition(":")
    prov = prov.lower()
    ql = q.lower()
    if prov in ("eurostat", "ecb"):
        flow = rest.upper()
        t0d = tier0(prov)
        rec = (t0d.get("flows") or {}).get(flow)
        if not rec:
            return {"ds": ds, "error": "unknown flow", "rows": []}
        total = rec.get("series")
        bm = blocks_map(prov, flow)
        rows, facets, scanned = [], defaultdict(lambda: defaultdict(int)), 0
        ldims, lfn = labeler(prov, flow)
        dim_names = [d["id"] for d in (ldims or [])]
        qtoks = [t for t in ql.replace(",", " ").split() if t]
        MAX_SCAN = 120000

        def matches(sid, key, extra):
            # every query token must appear in the id, a dimension code, its label, or the geo
            if not qtoks:
                return True
            lab = (label_key(prov, flow, key, lfn, ldims) or "") if lfn else ""
            hay = (sid + " " + key.replace(".", " ") + " " + lab + " " + extra).lower()
            return all(t in hay for t in qtoks)
        if bm and bm.get("blocks"):
            bl = bm["blocks"]
            for b in bl:
                if scanned >= MAX_SCAN or len(rows) >= offset + limit:
                    break
                for e in _t1_read_block(prov, flow, b):
                    scanned += 1
                    key = e["id"].split(":", 2)[-1]
                    parts = key.split(".")
                    if prov == "ecb" and parts and parts[0] == flow:
                        parts = parts[1:]
                    for pos, v in enumerate(parts[:12]):
                        facets[dim_names[pos] if pos < len(dim_names) else "d%d" % pos][v] += 1
                    if not matches(e["id"].lower(), key, (e.get("g") or "")):
                        continue
                    rows.append(_t1_row(prov, flow, e, lfn, ldims))
            tier = 1
        else:
            lo, hi = rec.get("lo", 0), rec.get("hi", 0)
            for p in range(lo, min(hi, lo + 40) + 1):
                if len(rows) >= offset + limit or scanned >= MAX_SCAN:
                    break
                for r in _page_rows(prov, flow, p):
                    scanned += 1
                    for k, v in (r.get("dims") or {}).items():
                        facets[k][str(v)] += 1
                    key = r["id"].split(":", 2)[-1]
                    if not matches(r["id"].lower(), key, (r.get("name") or "") + " " + (r.get("geo") or "")):
                        continue
                    rows.append(_page_row(prov, flow, r, lfn, ldims))
            tier = 0
        # facets carry labels when the DSD is built: [code, count, label]
        fac = {}
        for k, v in facets.items():
            pos = dim_names.index(k) if k in dim_names else None
            fac[k] = [[c, n, (lfn(pos, c) if (lfn and pos is not None) else None)] for c, n in sorted(v.items(), key=lambda kv: -kv[1])[:40]]
        return {"ds": ds, "flow": flow, "tier": tier, "total": total, "scanned": scanned, "truncated": scanned < (total or 0), "labeled": lfn is not None,
                "rows": rows[offset:offset + limit], "matched": len(rows), "facets": fac,
                "hint": ("dimension labels on" if lfn else "labels not built yet for this dataset — codes shown") + "; type codes or words (e.g. DE, monthly, chain linked) to narrow; giant flows scan the first %d series" % MAX_SCAN}
    if prov == "statcan":
        return browse_statcan(rest, ql, limit, offset)
    if prov == "worldbank":
        cs = _get_json(SD + "wb-countries.json") or []
        rows = []
        for c in cs:
            if ql and ql not in (c.get("name") or "").lower() and ql != (c.get("id") or "").lower():
                continue
            rows.append({"id": "worldbank:%s:%s" % (rest, c["id"]), "symbol": "%s:%s" % (rest, c["id"]), "name": "%s — %s" % (rest, c.get("name")),
                         "provider": "worldbank", "provider_name": "World Bank", "kind": "series", "chartable": True, "geo": c["id"], "region": c.get("region")})
        return {"ds": ds, "total": len(cs), "rows": rows[offset:offset + limit], "matched": len(rows)}
    # docs-linked children (boj DB, census dataset, treasury dataset, ofr dataset)
    ix = load_index()
    rows = []
    for d in ix["docs"]:
        if d[D_KIND] != "series":
            continue
        ok = False
        if prov == "boj" and d[D_PROV] == "boj" and d[D_KEY] == rest:
            ok = True
        elif prov in ("census", "treasury", "ofr") and d[D_PROV] == prov and (d[D_EXTRA] or {}).get("ds") == rest:
            ok = True
        if not ok:
            continue
        if ql and ql not in d[D_TITLE].lower() and ql not in d[D_ID].lower():
            continue
        rows.append(doc_row(d))
        if len(rows) >= offset + limit + 2000:
            break
    return {"ds": ds, "total": len(rows), "rows": rows[offset:offset + limit], "matched": len(rows)}


def _stream_gz_lines(key, max_bytes=None):
    """Lines of a gzipped text object. StatCan cubes are gzip(zip(csv)): the walker banked the
    full-table zip and gzipped it, so a PK magic after gunzip means "open the first CSV member"."""
    raw = _get(key)
    if max_bytes and len(raw) > max_bytes:
        raise ValueError("object too large to scan interactively (%d bytes)" % len(raw))
    body = gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb")
    head = body.read(4)
    body = io.BytesIO(head + body.read())
    if head[:2] == b"PK":
        import zipfile
        z = zipfile.ZipFile(body)
        names = [n for n in z.namelist() if n.lower().endswith(".csv") and "metadata" not in n.lower()] or z.namelist()
        body = z.open(names[0])
    f = io.TextIOWrapper(body, encoding="utf-8-sig", errors="ignore", newline="")
    for ln in f:
        yield ln.rstrip("\r\n")


def browse_statcan(pid, ql, limit, offset, max_rows=400000):
    key = "data/warm/statcan/data/%s.dat.gz" % pid
    try:
        it = _stream_gz_lines(key, max_bytes=90_000_000)
        hdr = next(it)
    except StopIteration:
        return {"ds": "statcan:" + pid, "rows": [], "error": "empty cube"}
    except Exception as e:  # noqa: BLE001
        return {"ds": "statcan:" + pid, "rows": [], "error": str(e)[:160]}
    cols = next(csv.reader([hdr]))
    ix = {c.strip().lstrip("\ufeff"): i for i, c in enumerate(cols)}
    vi = ix.get("VECTOR")
    di = ix.get("REF_DATE")
    if vi is None:
        return {"ds": "statcan:" + pid, "rows": [], "error": "no VECTOR column"}
    label_cols = [i for c, i in ix.items() if c not in ("REF_DATE", "DGUID", "UOM_ID", "SCALAR_ID", "VECTOR", "COORDINATE", "VALUE", "STATUS", "SYMBOL", "TERMINATED", "DECIMALS")]
    seen, rows, n = {}, [], 0
    rd = csv.reader(it)
    for p in rd:
        n += 1
        if n > max_rows:
            break
        if len(p) <= vi:
            continue
        v = p[vi]
        if v in seen:
            e = seen[v]
            d0 = p[di] if di is not None and di < len(p) else None
            if d0:
                if e["first"] is None or d0 < e["first"]:
                    e["first"] = d0
                if e["last"] is None or d0 > e["last"]:
                    e["last"] = d0
            e["n"] += 1
            continue
        label = " · ".join(p[i] for i in label_cols if i < len(p) and p[i])
        e = {"id": "statcan:%s:%s" % (pid, v), "symbol": v, "name": label[:200], "provider": "statcan", "provider_name": "StatCan", "kind": "series",
             "chartable": True, "first": p[di] if di is not None and di < len(p) else None, "last": p[di] if di is not None and di < len(p) else None, "n": 1}
        seen[v] = e
    for e in seen.values():
        e["first"], e["last"] = norm_period(e["first"]), norm_period(e["last"])
        if ql and ql not in e["name"].lower() and ql != e["symbol"].lower():
            continue
        rows.append(e)
    return {"ds": "statcan:" + pid, "total": len(seen), "scanned_rows": n, "truncated": n > max_rows, "rows": rows[offset:offset + limit], "matched": len(rows)}


# ================================================================ SERIES RESOLVERS

def _cache_key(sid):
    h = hashlib.sha1(sid.encode()).hexdigest()
    return "%s%s/%s.json" % (CACHE_PREFIX, h[:2], h)


def _cache_get(sid, max_age_s):
    try:
        o = s3.get_object(Bucket=BUCKET, Key=_cache_key(sid))
        age = (_now() - o["LastModified"]).total_seconds()
        if age > max_age_s:
            return None, age
        return json.loads(o["Body"].read()), age
    except Exception:  # noqa: BLE001
        return None, None


def _cache_put(sid, docd):
    try:
        _put_json(_cache_key(sid), docd, cache="public, max-age=1800")
    except Exception:  # noqa: BLE001
        pass


def _result(sid, prov, obs, name=None, unit=None, freq=None, source=None, extra=None):
    clean = {}
    for d, v in obs:
        dd = norm_period(d)
        vv = _f(v)
        if dd and vv is not None:
            clean[dd] = vv
    pts = [[k, v] for k, v in sorted(clean.items())]      # lists, like the JSON cache returns them (quote() bisects on them)
    out = {"id": sid, "provider": prov, "provider_name": PROV_NAME.get(prov, prov), "name": name or sid, "unit": unit, "freq": freq,
           "source": source, "n": len(pts), "first": pts[0][0] if pts else None, "last": pts[-1][0] if pts else None,
           "obs": pts, "as_of": _iso(), "version": VERSION}
    if extra:
        out.update(extra)
    return out


def _doc_lookup(sid):
    try:
        ix = load_index()
        rows, _ = _prefix_range(ix["ids"], sid.upper(), cap=3)
        for k, i in rows:
            if k == sid.upper():
                return ix["docs"][i]
    except Exception:  # noqa: BLE001
        return None
    return None


FRED_STALE_DAYS = {"D": 4, "W": 10, "BW": 18, "M": 40, "Q": 100, "SA": 200, "A": 400}


def _fred_tail_refresh(root, sid, j, freq, force=False):
    """The fred-catalog engine drains once and never revisits a banked doc (DGS10 sat at 2026-08-06 on
    2026-09-02). When the bank is older than its cadence allows, pull the tail from FRED, merge append-only
    and rewrite the doc in place -- the warehouse heals on open, exactly like the tv-bars universe."""
    obs = (j or {}).get("observations") or []
    if not obs or not FRED_KEY:
        return j, 0
    last = max((o.get("date") or "") for o in obs)
    try:
        age = (_now().date() - date.fromisoformat(last[:10])).days
    except Exception:  # noqa: BLE001
        return j, 0
    if not force and age <= FRED_STALE_DAYS.get((freq or "D")[:2].upper(), 40):
        return j, 0
    t = None
    for attempt in range(2):
        try:
            t = fred_get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&observation_start=%s&limit=100000"
                         % (urllib.parse.quote(sid), FRED_KEY, last[:10]), timeout=30)
            break
        except Exception:  # noqa: BLE001
            return j, 0
    if t is None:
        return j, 0
    tail = [o for o in t.get("observations") or [] if o.get("date") and o.get("date") > last]
    if not tail:
        return j, 0
    merged = obs + [{"date": o["date"], "value": o.get("value")} for o in tail]
    j["observations"] = merged
    meta = j.setdefault("meta", {})
    meta["last_updated"], meta["tail_refreshed_at"], meta["tail_refreshed_by"] = tail[-1]["date"], _iso(), "justhodl-symdir %s" % VERSION
    try:
        s3.put_object(Bucket=BUCKET, Key="data/warm/fred-scoped/%s/%s.json" % (root, sid), Body=json.dumps(j, separators=(",", ":")).encode(),
                      ContentType="application/json", CacheControl="public, max-age=900")
    except Exception:  # noqa: BLE001
        pass
    return j, len(tail)


def r_fred(sid, rest, d):
    root = d[D_KEY] if d else None
    obs, src, name = [], None, d[D_TITLE] if d else rest
    if root:
        try:
            j = _get_json("data/warm/fred-scoped/%s/%s.json" % (root, rest))
            j, added = _fred_tail_refresh(root, rest, j, (d[D_FREQ] if d else None) or ((j or {}).get("meta") or {}).get("frequency"))
            obs = [(o.get("date"), o.get("value")) for o in (j or {}).get("observations") or []]
            src = "warehouse:fred-scoped/%s" % root + (" (+%d obs tail from FRED, bank healed)" % added if added else "")
        except Exception:  # noqa: BLE001
            obs = []
    if not obs and FRED_KEY:
        j = fred_get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&observation_start=1776-07-04&limit=100000"
                     % (urllib.parse.quote(rest), FRED_KEY), timeout=30)
        obs = [(o.get("date"), o.get("value")) for o in j.get("observations") or []]
        src = "fred-api"
        if obs:
            # the catalog crawl leaked this id (COMPLETE_WITH_LEAKS) -- bank it now so it is warehouse-served from here on
            try:
                meta = (fred_get("https://api.stlouisfed.org/fred/series?series_id=%s&api_key=%s&file_type=json" % (urllib.parse.quote(rest), FRED_KEY), timeout=20).get("seriess") or [{}])[0]
                bank = {"meta": {"id": rest, "title": meta.get("title"), "units": meta.get("units_short"), "frequency": meta.get("frequency_short"), "seasonal_adjustment": meta.get("seasonal_adjustment_short"),
                                 "popularity": meta.get("popularity"), "observation_start": meta.get("observation_start"), "observation_end": meta.get("observation_end"), "last_updated": meta.get("last_updated"),
                                 "root": "On_Demand", "banked_by": "justhodl-symdir %s" % VERSION, "banked_at": _iso()},
                        "observations": [{"date": a, "value": b} for a, b in obs]}
                s3.put_object(Bucket=BUCKET, Key="data/warm/fred-scoped/On_Demand/%s.json" % rest, Body=json.dumps(bank, separators=(",", ":")).encode(),
                              ContentType="application/json", CacheControl="public, max-age=900")
                led = _get_json(SD + "fred-titles.json") or {}
                led[rest] = {"title": meta.get("title"), "units": meta.get("units_short"), "freq": meta.get("frequency_short"), "sa": meta.get("seasonal_adjustment_short"),
                             "pop": meta.get("popularity"), "obs_start": meta.get("observation_start"), "obs_end": meta.get("observation_end"), "on_demand_root": "On_Demand"}
                _put_json(SD + "fred-titles.json", led, cache="no-cache")
                src = "fred-api (banked on demand → warehouse)"
                name = meta.get("title") or name
            except Exception:  # noqa: BLE001
                pass
    ex = (d[D_EXTRA] if d else None) or {}
    merged = {norm_period(a): _f(b) for a, b in obs if norm_period(a) and _f(b) is not None}
    added = 0
    for flag, key in (("te", "data/warm/te-mirror/%s.json" % rest), ("arch", "data/warm/archived-fred/%s.json" % rest)):
        if ex.get(flag):
            j = _get_json(key) or {}
            for o in j.get("observations") or []:
                dd, vv = norm_period(o.get("date")), _f(o.get("value"))
                if dd and vv is not None and dd not in merged:
                    merged[dd] = vv
                    added += 1
    return _result(sid, "fred", list(merged.items()), name=name, unit=d[D_UNIT] if d else None, freq=d[D_FREQ] if d else None,
                   source=src, extra={"mirror_points_added": added} if added else None)


def r_te(sid, rest, d):
    j = _get_json("data/warm/te-mirror/%s.json" % rest) or _get_json("data/warm/archived-fred/%s.json" % rest) or {}
    return _result(sid, "te", [(o.get("date"), o.get("value")) for o in j.get("observations") or []], name=(d[D_TITLE] if d else rest), source="warehouse:te-mirror")


def _parse_eurostat_tsv(txt, key=None):
    lines = txt.split("\n")
    if not lines:
        return None, None
    hdr = lines[0].rstrip("\r")
    dims_part, sep, periods_part = hdr.partition("\\TIME_PERIOD")
    if not sep:
        return None, None
    periods = [p.strip() for p in periods_part.split("\t") if p.strip()]
    for ln in lines[1:]:
        ln = ln.rstrip("\r")
        if not ln:
            continue
        kp, tab, vp = ln.partition("\t")
        if not tab:
            continue
        if key and kp.strip().upper() != key.upper():
            continue
        vals = [v.strip() for v in vp.split("\t")]
        obs = [(periods[i], vals[i]) for i in range(min(len(vals), len(periods))) if _f(vals[i]) is not None]
        return kp.strip(), obs
    return None, None


def r_eurostat(sid, rest, d):
    flow, _, key = rest.partition(":")
    flow = flow.upper()
    if not key:
        raise ValueError("eurostat needs FLOW:KEY")
    obs, src = None, None
    try:
        b, _, _ = _http("https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/%s/%s?format=TSV&compressed=false" % (flow, urllib.parse.quote(key)), timeout=40)
        txt = b.decode("utf-8", "replace")
        if txt.startswith("\x1f\x8b") or b[:2] == b"\x1f\x8b":
            txt = gzip.decompress(b).decode("utf-8", "replace")
        _, obs = _parse_eurostat_tsv(txt)
        src = "eurostat-api"
    except Exception as e:  # noqa: BLE001
        src = "api-failed:" + str(e)[:60]
    if not obs:
        # warehouse raw scan (bounded): one row in {FLOW}.dat.gz whose key matches
        rk = "data/warm/eurostat/data/%s.dat.gz" % flow
        try:
            head = s3.head_object(Bucket=BUCKET, Key=rk)
            if head["ContentLength"] > 200_000_000:
                raise ValueError("raw flow too large for an interactive scan (%d MB)" % (head["ContentLength"] // 1_000_000))
            it = _stream_gz_lines(rk)
            hdr = next(it)
            dims_part, sep, periods_part = hdr.partition("\\TIME_PERIOD")
            periods = [p.strip() for p in periods_part.split("\t") if p.strip()]
            ku = key.upper()
            for ln in it:
                kp, tab, vp = ln.partition("\t")
                # the TSV key part is comma-separated ("A,CLV10_MEUR,B1GQ,DE"); the series key uses dots
                if tab and kp.strip().replace(",", ".").upper() == ku:
                    vals = [v.strip() for v in vp.split("\t")]
                    obs = [(periods[i], vals[i]) for i in range(min(len(vals), len(periods)))]
                    break
            src = (src + " -> " if src else "") + "warehouse:eurostat raw"
        except StopIteration:
            pass
    fd = _doc_lookup("eurostat:" + flow)
    lab = label_key("eurostat", flow, key)
    name = ((fd[D_TITLE] + " — ") if fd else (flow + " — ")) + (lab or key)
    dims = key.split(".")
    return _result(sid, "eurostat", obs or [], name=name, freq=dims[0] if dims else None, source=src,
                   extra={"flow": flow, "key": key, "source_url": "https://ec.europa.eu/eurostat/databrowser/view/%s" % flow.lower()})


def _parse_ecb_csv(txt, want_key=None):
    rd = csv.reader(io.StringIO(txt))
    hdr = next(rd, None)
    if not hdr:
        return [], {}
    ix = {h: i for i, h in enumerate(hdr)}
    ki, ti, vi = ix.get("KEY"), ix.get("TIME_PERIOD"), ix.get("OBS_VALUE")
    if ti is None or vi is None:
        return [], {}
    obs, meta = [], {}
    for p in rd:
        if len(p) <= max(ti, vi):
            continue
        if want_key and ki is not None and p[ki] != want_key:
            continue
        obs.append((p[ti], p[vi]))
        if not meta:
            for f in ("TITLE", "TITLE_COMPL", "UNIT", "UNIT_MEASURE", "FREQ"):
                if f in ix and ix[f] < len(p):
                    meta[f] = p[ix[f]]
    return obs, meta


def r_ecb(sid, rest, d):
    flow, _, key = rest.partition(":")
    flow = flow.upper()
    if not key:
        raise ValueError("ecb needs FLOW:KEY")
    skey = key[len(flow) + 1:] if key.upper().startswith(flow + ".") else key
    obs, meta, src = [], {}, None
    try:
        b, _, _ = _http("https://data-api.ecb.europa.eu/service/data/%s/%s?format=csvdata" % (flow, urllib.parse.quote(skey)), timeout=40)
        obs, meta = _parse_ecb_csv(b.decode("utf-8", "replace"))
        src = "ecb-api"
    except Exception as e:  # noqa: BLE001
        src = "api-failed:" + str(e)[:60]
    if not obs:
        files = sorted(o["Key"] for o in _list("data/warm/ecb/data/") if o["Key"].rsplit("/", 1)[-1].split("__")[0].split(".")[0] == flow and o["Key"].endswith(".dat.gz"))
        want = key if key.upper().startswith(flow + ".") else flow + "." + key
        for fk in files:
            try:
                raw = _get(fk)
                if len(raw) > 250_000_000:
                    continue
                txt = gzip.decompress(raw).decode("utf-8", "replace")
                o2, m2 = _parse_ecb_csv(txt, want_key=want)
                obs.extend(o2)
                meta = meta or m2
            except Exception:  # noqa: BLE001
                continue
        src = (src + " -> " if src else "") + "warehouse:ecb raw (%d slices)" % len(files)
    name = meta.get("TITLE_COMPL") or meta.get("TITLE") or label_key("ecb", flow, skey) or (d[D_TITLE] if d else None) or (flow + " · " + skey)
    return _result(sid, "ecb", obs, name=name, unit=meta.get("UNIT") or meta.get("UNIT_MEASURE"), freq=meta.get("FREQ") or skey.split(".")[0], source=src,
                   extra={"flow": flow, "key": skey, "source_url": "https://data.ecb.europa.eu/data/datasets/%s/%s.%s" % (flow, flow, skey)})


def r_nyfed(sid, rest, d):
    rate, _, field = rest.partition(":")
    kind = "unsecured" if rate in ("effr", "obfr") else "secured"
    obs, src = [], None
    try:
        j = _http_json("https://markets.newyorkfed.org/api/rates/%s/%s/search.json?startDate=2014-01-01" % (kind, rate), timeout=30)
        for r in j.get("refRates") or []:
            obs.append((r.get("effectiveDate"), r.get("volumeInBillions") if field == "volume" else r.get("percentRate")))
        src = "nyfed-api"
    except Exception as e:  # noqa: BLE001
        src = "api-failed:" + str(e)[:60]
    if not obs:
        j = _get_json("data/warm/nyfed/%s.json.gz" % rate) or {}
        obs = [(o.get("date"), o.get("volume_bn") if field == "volume" else o.get("rate")) for o in j.get("observations") or []]
        src = (src or "") + " warehouse:nyfed"
    return _result(sid, "nyfed", obs, name=(d[D_TITLE] if d else rest.upper()), unit="$bn" if field == "volume" else "Percent", freq="D", source=src)


def r_ofr(sid, rest, d, prov="ofr"):
    if d and d[D_KEY]:
        j = _get_json(d[D_KEY]) or {}
        if prov == "ofr":
            ts = ((j.get("payload") or {}).get("timeseries") or {}).get(rest) or {}
            meta = ts.get("metadata") or {}
            agg = (ts.get("timeseries") or {}).get("aggregation") or []
            return _result(sid, prov, [(a, b) for a, b in agg], name=d[D_TITLE], unit=meta.get("unit") or d[D_UNIT], freq=d[D_FREQ], source="warehouse:ofr")
        blob = j.get(rest) or next(iter(j.values()), {}) if isinstance(j, dict) else {}
        agg = ((blob or {}).get("timeseries") or {}).get("aggregation") or []
        return _result(sid, prov, [(a, b) for a, b in agg], name=d[D_TITLE], source="warehouse:" + prov)
    j = _http_json("https://data.financialresearch.gov/v1/series/timeseries?mnemonic=%s" % urllib.parse.quote(rest), timeout=30)
    agg = j if isinstance(j, list) else (j.get("timeseries") or {}).get("aggregation") or []
    return _result(sid, prov, [(a, b) for a, b in agg], name=rest, source="ofr-api")


def r_boj(sid, rest, d):
    db, _, code = rest.partition(":")
    keys = [o["Key"] for o in _list("data/warm/boj-full/api/%s/" % db) if o["Key"].endswith(".json.gz")]
    obs, name, unit, freq = [], None, None, None
    parts_hit, vals_seen = 0, 0
    with ThreadPoolExecutor(16) as ex:
        for j in ex.map(lambda k: _get_json(k) or {}, keys):
            for r in j.get("RESULTSET") or []:
                if r.get("SERIES_CODE") != code:
                    continue
                parts_hit += 1
                name = name or r.get("NAME_OF_TIME_SERIES")
                unit = unit or r.get("UNIT")
                freq = freq or r.get("FREQUENCY")
                vals = r.get("VALUES") or {}
                dts = vals.get("SURVEY_DATES") or []
                vv = None
                for k2, v2 in vals.items():
                    if k2 != "SURVEY_DATES" and isinstance(v2, list):
                        vv = v2
                        break
                if vv is None:
                    continue
                vals_seen += len(vv)
                for dt, v in zip(dts, vv):
                    ds = str(dt)
                    if len(ds) == 6:
                        ds = ds[:4] + "-" + ds[4:]
                    elif len(ds) == 8:
                        ds = ds[:4] + "-" + ds[4:6] + "-" + ds[6:]
                    obs.append((ds, v))
    diag = {"parts": len(keys), "parts_with_code": parts_hit, "values_seen": vals_seen, "values_nonnull": sum(1 for _, v in obs if v is not None)}
    return _result(sid, "boj", obs, name=name or (d[D_TITLE] if d else code), unit=unit, freq=freq, source="warehouse:boj api parts (%d)" % len(keys), extra={"diag": diag})


def r_statcan(sid, rest, d):
    pid, _, vec = rest.partition(":")
    if not vec:
        raise ValueError("statcan needs PRODUCT:VECTOR")
    obs, src, name = [], None, None
    vid = vec.lower().lstrip("v")
    try:
        j = _http_json("https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorByReferencePeriodRange?vectorIds=%%22%s%%22&startRefPeriod=1900-01-01&endReferencePeriod=2100-01-01" % vid, timeout=40)
        for blk in j if isinstance(j, list) else []:
            for p in (blk.get("object") or {}).get("vectorDataPoint") or []:
                obs.append((p.get("refPer"), p.get("value")))
        src = "statcan-wds"
    except Exception as e:  # noqa: BLE001
        src = "api-failed:" + str(e)[:60]
    if not obs:
        it = _stream_gz_lines("data/warm/statcan/data/%s.dat.gz" % pid, max_bytes=90_000_000)
        cols = next(csv.reader([next(it)]))
        ix = {c.strip().lstrip("\ufeff"): i for i, c in enumerate(cols)}
        vi, di, vali = ix.get("VECTOR"), ix.get("REF_DATE"), ix.get("VALUE")
        for p in csv.reader(it):
            if len(p) > max(vi, di, vali) and p[vi].lower() == vec.lower():
                obs.append((p[di], p[vali]))
        src = (src or "") + " warehouse:statcan cube"
    return _result(sid, "statcan", obs, name=(d[D_TITLE] if d else "%s %s" % (pid, vec)), source=src)


def r_worldbank(sid, rest, d):
    ind, _, iso = rest.partition(":")
    if not iso:
        raise ValueError("worldbank needs INDICATOR:COUNTRY")
    j = _http_json("https://api.worldbank.org/v2/country/%s/indicator/%s?format=json&per_page=20000" % (iso, ind), timeout=40)
    rows = j[1] if isinstance(j, list) and len(j) > 1 and j[1] else []
    name = None
    obs = []
    for r in rows:
        name = name or "%s — %s" % ((r.get("indicator") or {}).get("value"), (r.get("country") or {}).get("value"))
        obs.append((r.get("date"), r.get("value")))
    return _result(sid, "worldbank", obs, name=name or sid, freq="A", source="worldbank-api")


def r_treasury(sid, rest, d):
    dsn, _, field = rest.partition(":")
    key = (d[D_KEY] if d else None) or ("data/warm/treasury/%s.json.gz" % dsn.lower())
    j = _get_json(key) or {}
    obs = j.get("observations")
    if isinstance(obs, list) and obs and isinstance(obs[0], dict) and "date" in obs[0]:
        vk = ((d[D_EXTRA] or {}).get("field") if d else None) or field or next((x for x in obs[0].keys() if x not in ("date", "record_date") and _f(obs[0].get(x)) is not None), "value")
        return _result(sid, "treasury", [(o.get("date"), o.get(vk)) for o in obs], name=(d[D_TITLE] if d else "FiscalData " + dsn), unit=j.get("unit"), source="warehouse:treasury")
    rows = j.get("payload") or j.get("data") or j.get("rows") or []
    if isinstance(rows, dict):
        rows = rows.get("data") or []
    fld = (d[D_EXTRA] or {}).get("field") or field
    return _result(sid, "treasury", [(r.get("record_date"), r.get(fld)) for r in rows], name=d[D_TITLE], unit=j.get("unit"), source="warehouse:treasury")


def r_boe(sid, rest, d):
    key = (d[D_KEY] if d else None) or ("data/warm/boe-full/iadb/%s.csv.gz" % rest)
    obs = []
    it = _stream_gz_lines(key)
    hdr = next(it, "")
    for ln in it:
        p = ln.split(",")
        if len(p) >= 2:
            obs.append((p[0].strip(), p[1].strip()))
    return _result(sid, "boe", obs, name=(d[D_TITLE] if d else rest), source="warehouse:boe iadb")


def r_census(sid, rest, d):
    p = rest.split(":")
    if len(p) < 5:
        raise ValueError("census needs DATASET:DATA_TYPE:CATEGORY:SA:GEO")
    slug, dt, cc, sa, geo = p[:5]
    rows = _get_json("data/warm/census-us/%s/full.json.gz" % slug) or []
    if len(rows) < 2:
        raise ValueError("no census rows")
    ix = {h: i for i, h in enumerate(rows[0])}
    obs = []
    for r in rows[1:]:
        try:
            if r[ix["data_type_code"]] == dt and r[ix["category_code"]] == cc and str(r[ix["seasonally_adj"]]) == sa and (ix.get("geo_level_code") is None or r[ix["geo_level_code"]] == geo):
                obs.append((r[ix["time"]], r[ix["cell_value"]]))
        except Exception:  # noqa: BLE001
            continue
    return _result(sid, "census", obs, name=(d[D_TITLE] if d else sid), freq="M", source="warehouse:census-us")


def r_bls(sid, rest, d):
    if not BLS_KEY:
        raise ValueError("BLS_API_KEY not configured")
    obs = []
    y1 = int((d[D_FIRST] or "1950")[:4]) if d else 1950
    y2 = _now().year
    y = y1
    while y <= y2:
        payload = json.dumps({"seriesid": [rest], "startyear": str(y), "endyear": str(min(y + 19, y2)), "registrationkey": BLS_KEY}).encode()
        b, _, _ = _http("https://api.bls.gov/publicAPI/v2/timeseries/data/", timeout=40, headers={"Content-Type": "application/json"}, data=payload)
        j = json.loads(b)
        for s in ((j.get("Results") or {}).get("series") or []):
            for o in s.get("data") or []:
                per = o.get("period") or ""
                if per.startswith("M") and per != "M13":
                    obs.append(("%s-%s-01" % (o["year"], per[1:]), o.get("value")))
                elif per.startswith("Q") and per != "Q05":
                    obs.append(("%s-%02d-01" % (o["year"], (int(per[1:]) - 1) * 3 + 1), o.get("value")))
                elif per == "A01" or per == "M13" or per == "Q05":
                    obs.append(("%s-01-01" % o["year"], o.get("value"))) if per == "A01" else None
                elif per.startswith("S"):
                    obs.append(("%s-%02d-01" % (o["year"], (int(per[1:]) - 1) * 6 + 1), o.get("value")))
        y += 20
    return _result(sid, "bls", obs, name=(d[D_TITLE] if d else rest), source="bls-api-v2")


# ---------------- TradingView universe (any EXCHANGE:SYMBOL) and US equities, from the warehouse
TV_UNIV = "data/warm/tv-bars/universe/"
_lambda = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=90, retries={"max_attempts": 1}))


def _tv_safe(sym):
    return re.sub(r"[^A-Za-z0-9_.\-!]", "__", sym)


def _tv_bank_doc(sym):
    return _get_json(TV_UNIV + _tv_safe(sym) + ".json.gz")


def _tv_pull(syms, budget=35, ysym=None, refresh=False):
    """Bank symbols on demand through justhodl-tv-bars (mode=pull); returns {sym: result}."""
    try:
        resp = _lambda.invoke(FunctionName="justhodl-tv-bars", InvocationType="RequestResponse",
                              Payload=json.dumps({"mode": "pull", "tv_symbols": list(syms), "budget": budget, "ysym": ysym or {}, "refresh": bool(refresh)}).encode())
        body = json.loads(resp["Payload"].read() or b"{}")
        return body.get("results") or {}
    except Exception as e:  # noqa: BLE001
        return {sym: {"ok": False, "error": str(e)[:120]} for sym in syms}


def _bars_result(sid, prov, doc, name, source, extra=None):
    bars = doc.get("bars") or []
    ohlc = []
    for b in bars:
        try:
            dt = datetime.fromtimestamp(int(b[0]), tz=timezone.utc).strftime("%Y-%m-%d")
            ohlc.append([dt, float(b[1]), float(b[2]), float(b[3]), float(b[4]), float(b[5]) if len(b) > 5 and b[5] is not None else 0.0])
        except Exception:  # noqa: BLE001
            continue
    out = _result(sid, prov, [(o[0], o[4]) for o in ohlc], name=name, freq="D", source=source, extra=extra)
    out["ohlc"] = ohlc
    return out


# TradingView symbols that exist only inside TradingView (no Yahoo/exchange feed) map to the warehouse series that
# carry the same concept. Each entry lists every source we hold, best first; the search shows them all and the
# resolver serves the first that answers.
_TENOR = {"01M": "1MO", "02M": "2MO", "03M": "3MO", "04M": "4MO", "06M": "6MO", "01": "1", "02": "2", "03": "3", "05": "5", "07": "7", "10": "10", "20": "20", "30": "30"}
_ISO3 = {"US": "USA", "DE": "DEU", "GB": "GBR", "UK": "GBR", "JP": "JPN", "FR": "FRA", "IT": "ITA", "ES": "ESP", "CA": "CAN", "AU": "AUS", "CN": "CHN", "KR": "KOR", "IN": "IND",
         "BR": "BRA", "MX": "MEX", "TR": "TUR", "ZA": "ZAF", "CH": "CHE", "NL": "NLD", "PT": "PRT", "GR": "GRC", "SE": "SWE", "NO": "NOR", "DK": "DNK", "FI": "FIN", "AT": "AUT",
         "BE": "BEL", "IE": "IRL", "PL": "POL", "CZ": "CZE", "HU": "HUN", "NZ": "NZL", "IL": "ISR", "ID": "IDN", "RU": "RUS", "CL": "CHL", "CO": "COL", "SG": "SGP", "HK": "HKG"}
ECON_EQUIV = {"USINTR": "fred:FEDFUNDS", "USCPI": "fred:CPIAUCSL", "USCCPI": "fred:CPILFESL", "USUR": "fred:UNRATE", "USGDP": "fred:GDP", "USGDPQQ": "fred:A191RL1Q225SBEA",
              "USNFP": "fred:PAYEMS", "USIJC": "fred:ICSA", "USCJC": "fred:CCSA", "USRSM": "fred:RSAFS", "USIP": "fred:INDPRO", "USM2": "fred:M2SL", "USBOT": "fred:BOPGSTB",
              "USPPI": "fred:PPIACO", "USHS": "fred:HOUST", "USBP": "fred:PERMIT", "USCS": "fred:UMCSENT", "USDGO": "fred:DGORDER", "USPCE": "fred:PCEPI", "USCPCE": "fred:PCEPILFE",
              "USTBL": "fred:BOPGSTB", "USGD": "fred:GFDEBTN", "USAHE": "fred:AHETPI", "USPART": "fred:CIVPART", "USJO": "fred:JTSJOL", "USBBS": "fred:WALCL", "USCBBS": "fred:WALCL",
              "EUINTR": "fred:ECBDFR", "DEUR": "fred:LRHUTTTTDEM156S", "DECPI": "fred:DEUCPIALLMINMEI", "DEGDPQQ": "fred:CLVMNACSCAB1GQDE", "GBINTR": "fred:IRSTCB01GBM156N",
              "GBCPI": "fred:GBRCPIALLMINMEI", "JPINTR": "fred:IRSTCB01JPM156N", "JPCPI": "fred:JPNCPIALLMINMEI", "CNGDP": "fred:MKTGDPCNA646NWDB", "CNCPI": "fred:CHNCPIALLMINMEI",
              "CAINTR": "fred:IRSTCB01CAM156N", "AUINTR": "fred:IRSTCB01AUM156N", "USDXY": "fred:DTWEXBGS"}


def tv_equivalents(sym):
    """[(provider_id, note)] for a TradingView symbol we cannot bank from a market feed."""
    ex, _, bare = sym.upper().partition(":")
    out = []
    if ex == "TVC":
        m = re.match(r"^([A-Z]{2})(\d{2})(M?)Y$", bare)          # US02MY, DE10Y, JP30Y
        if m:
            cc, num, mm = m.group(1), m.group(2), m.group(3)
            tenor = _TENOR.get(num + ("M" if mm else ""))
            if cc == "US" and tenor:
                ust = (num.lstrip("0") + ("M" if mm else "Y"))
                if ust in ("2M", "4M"):                 # FRED's H.15 set has no 2- or 4-month CMT; Treasury's par curve does
                    out.append(("ustpar:" + ust, "US Treasury par yield curve, daily (home.treasury.gov)"))
                if tenor not in ("2MO", "4MO"):        # FRED has no DGS2MO / DGS4MO (verified: "series does not exist")
                    out.append(("fred:DGS" + tenor, "US Treasury constant-maturity yield, daily (FRED H.15)"))
                if ust in UST_TENORS and ust not in ("2M", "4M"):
                    out.append(("ustpar:" + ust, "US Treasury par yield curve, daily (home.treasury.gov)"))
            if cc == "DE" and not mm and num == "10":
                out.append(("official-yields:de-10y-bbk", "Bund 10-year, daily (Bundesbank)"))
            if cc in _ISO3 and not mm and num == "10":
                out.append(("fred:IRLTLT01%sM156N" % cc, "10-year government bond yield, monthly (OECD via FRED)"))
            if cc in _ISO3 and not mm and num in ("03", "3") and cc != "US":
                out.append(("fred:IR3TIB01%sM156N" % cc, "3-month interbank rate, monthly (OECD via FRED)"))
            if cc == "US" and mm and num == "03":
                out.append(("fred:DTB3", "3-month T-bill secondary market, daily"))
    if ex == "ECONOMICS" and bare in ECON_EQUIV:
        out.append((ECON_EQUIV[bare], "same indicator, official source"))
    seen, uniq = set(), []
    for i, n in out:
        if i and i not in seen:
            seen.add(i)
            uniq.append((i, n))
    return uniq


def sources_for(sym):
    """Every source we hold for a symbol's concept, with what each carries (the search bar shows this)."""
    rows = []
    for pid, note in tv_equivalents(sym):
        d = _doc_lookup(pid)
        prov = pid.split(":", 1)[0]
        if d:
            rows.append({"id": pid, "provider": d[D_PROV], "provider_name": PROV_NAME.get(d[D_PROV], d[D_PROV]), "name": d[D_TITLE], "freq": d[D_FREQ], "first": d[D_FIRST],
                         "last": d[D_LAST], "ohlc": False, "note": note, "banked": bool(d[D_KEY])})
        elif prov in ("ustpar", "official-yields"):
            rows.append({"id": pid, "provider": prov, "provider_name": PROV_NAME.get(prov, prov), "name": note, "freq": "D", "first": "1990" if prov == "ustpar" else None, "last": None, "ohlc": False,
                         "note": note, "banked": True})
        else:
            rows.append({"id": pid, "provider": prov, "provider_name": PROV_NAME.get(prov, prov), "name": note, "freq": None, "first": None, "last": None, "ohlc": False,
                         "note": note + " — not in the catalog yet; banks on first open", "banked": False})
    return rows


def r_tvsym(sid, sym, d):
    """'TVC:VIX' -> data/warm/tv-bars/universe/TVC__VIX.json.gz, banked on demand the first time.
    Symbols Khalid's dictionary already resolved to a warehouse provider (ECONOMICS:DEUR -> FRED:LRHUTTTTDEM156S)
    are served by that resolver -- the same series, from S3, with its full history."""
    ex = (d[D_EXTRA] or {}) if d else {}
    src_prov, src_id = (ex.get("src") or "").lower(), ex.get("sid")
    name = (d[D_TITLE] if d else None) or sym
    # 1. OHLC bank (candles) -- banked on first open, healed when stale
    doc = _tv_bank_doc(sym)
    src, bank_err = "warehouse:tv-bars", "bank not attempted"
    if doc and _bank_stale(doc):
        res = _tv_pull([sym], refresh=True)
        if (res.get(sym) or {}).get("ok"):
            doc = _tv_bank_doc(sym)
            src = "warehouse:tv-bars (tail healed)"
    if not doc:
        res = _tv_pull([sym])
        r0 = res.get(sym) or {}
        if r0.get("ok"):
            doc = _tv_bank_doc(sym)
            src = "warehouse:tv-bars (banked just now)"
        else:
            bank_err = r0.get("error") or "unknown"
    if doc:
        return _bars_result(sid, "tv", doc, name, src + " · " + str(doc.get("source") or ""), extra={"tv_symbol": sym, "banked_at": doc.get("as_of")})
    # 2. curated warehouse equivalents first (TVC:DE10Y -> Bundesbank daily before the OECD monthly the dictionary knows)
    alt_err = []
    for pid, note in tv_equivalents(sym):
        try:
            out = fetch_series(pid)
            if out.get("n"):
                out["id"], out["tv_symbol"], out["via"], out["via_note"] = sid, sym, pid, note
                out["name"] = name if name != sym else out.get("name")
                out["source"] = "%s (%s → %s)" % (out.get("source"), sym, pid)
                return out
            alt_err.append((pid, "no observations"))
        except Exception as e:  # noqa: BLE001
            alt_err.append((pid, "%s: %s" % (type(e).__name__, str(e)[:140])))
            continue
    # 3. the dictionary's provider series (close-only) as the generic fallback
    dict_err = ""
    if src_id and src_prov in ("fred", "ecb", "ofr", "nyfed", "eurostat", "boj", "bls", "worldbank"):
        inner = src_id.split(":", 1)[1] if ":" in src_id and src_id.split(":", 1)[0].lower() == src_prov else src_id
        try:
            out = fetch_series("%s:%s" % (src_prov, inner))
            if out.get("n"):
                out["id"], out["tv_symbol"], out["via"] = sid, sym, "%s:%s" % (src_prov, inner)
                out["name"] = name or out.get("name")
                return out
        except Exception as e:  # noqa: BLE001
            dict_err = "dictionary %s:%s failed: %s" % (src_prov, inner, str(e)[:60])     # a wrong source_id must not end the search
    err = ValueError("no market feed for %s and no warehouse equivalent (%s%s)" % (sym, bank_err[:100], ("; " + dict_err) if dict_err else ""))
    err.alternatives = [{"id": pid, "note": note, "error": dict(alt_err).get(pid)} for pid, note in tv_equivalents(sym)]
    raise err


def _bank_stale(doc, max_days=2):
    """A universe bank doc is stale when its last bar is older than the last 2 weekdays (nightly refresh missed / new symbol)."""
    try:
        last = date.fromisoformat((doc.get("last_date") or "")[:10])
    except Exception:  # noqa: BLE001
        return False
    d, n = _now().date(), 0
    while n < max_days:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return last < d


def r_equity(sid, ticker, d):
    """Bare US ticker -> warehouse bars: us-equities-daily if present, else the TradingView universe
    (banked on demand under the ticker's primary exchange; NASDAQ/NYSE/AMEX/CBOE/OTC tried in turn)."""
    t = ticker.upper()
    j = _get_json("data/warm/us-equities-daily/%s.json.gz" % t) or _get_json("data/warm/us-equities-daily/%s.json" % t)
    if isinstance(j, dict) and (j.get("bars") or j.get("observations")):
        bars = j.get("bars") or []
        if bars and isinstance(bars[0], list) and len(bars[0]) >= 5 and isinstance(bars[0][0], (int, float)):
            return _bars_result(sid, "equity", j, (d[D_TITLE] if d else t), "warehouse:us-equities-daily")
        obs = j.get("observations") or [(b.get("date"), b.get("close")) for b in bars if isinstance(b, dict)]
        return _result(sid, "equity", [(o.get("date"), o.get("close") or o.get("value")) if isinstance(o, dict) else o for o in obs],
                       name=(d[D_TITLE] if d else t), freq="D", source="warehouse:us-equities-daily")
    mkt = ((d[D_EXTRA] or {}).get("mkt") if d else None) or ""
    core = t.split(":", 1)[1] if ":" in t else t
    if t.startswith("X:") or mkt == "crypto":
        m = re.match(r"^([A-Z0-9]{2,10})(USDT|USDC|USD)$", core)
        ysym, bank = ((m.group(1) + "-USD") if m else core), "X:" + core
    elif t.startswith("C:") or mkt == "fx":
        ysym, bank = core + "=X", "C:" + core
    elif t.startswith("I:") or mkt == "indices":
        ysym, bank = {"SPX": "^GSPC", "NDX": "^NDX", "DJI": "^DJI", "VIX": "^VIX", "RUT": "^RUT", "NYA": "^NYA", "XAU": "^XAU", "HUI": "^HUI", "SOX": "^SOX",
                      "OEX": "^OEX", "RUI": "^RUI", "TNX": "^TNX", "TYX": "^TYX", "IRX": "^IRX", "FVX": "^FVX", "COMP": "^IXIC", "IXIC": "^IXIC"}.get(core, "^" + core), "I:" + core
    else:
        ysym, bank = core.replace(".", "-").replace("/", "-"), "US:" + core
    doc = _tv_bank_doc(bank)
    if doc and _bank_stale(doc):
        if (_tv_pull([bank], budget=25, ysym={bank: ysym}, refresh=True).get(bank) or {}).get("ok"):
            doc = _tv_bank_doc(bank)
            return _bars_result(sid, "equity", doc or {}, (d[D_TITLE] if d else t), "warehouse:tv-bars (tail healed) · " + str((doc or {}).get("source") or ""), extra={"bank": bank, "ysym": ysym})
    if not doc:
        res = _tv_pull([bank], budget=25, ysym={bank: ysym}).get(bank) or {}
        if not res.get("ok"):
            raise ValueError("no warehouse bars for %s (%s -> %s): %s" % (t, bank, ysym, (res.get("error") or "")[:120]))
        doc = _tv_bank_doc(bank)
        return _bars_result(sid, "equity", doc or {}, (d[D_TITLE] if d else t), "warehouse:tv-bars (banked just now) · " + str((doc or {}).get("source") or ""), extra={"bank": bank, "ysym": ysym})
    return _bars_result(sid, "equity", doc, (d[D_TITLE] if d else t), "warehouse:tv-bars · " + str(doc.get("source") or ""), extra={"bank": bank, "ysym": ysym})


UST_TENORS = {"1M": "BC_1MONTH", "2M": "BC_2MONTH", "3M": "BC_3MONTH", "4M": "BC_4MONTH", "6M": "BC_6MONTH", "1Y": "BC_1YEAR", "2Y": "BC_2YEAR", "3Y": "BC_3YEAR",
              "5Y": "BC_5YEAR", "7Y": "BC_7YEAR", "10Y": "BC_10YEAR", "20Y": "BC_20YEAR", "30Y": "BC_30YEAR"}
UST_KEY = "data/warm/treasury-par/curve.json.gz"


def _ust_fetch_year(year):
    """Treasury's daily par yield curve XML (the source FRED's H.15 mirrors; it also carries the 2- and 4-month CMTs)."""
    import xml.etree.ElementTree as ET
    url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value=%s" % year
    b, _, _ = _http(url, timeout=(240 if year == "all" else 60), headers={"Accept": "application/xml"}, retries=1)
    root = ET.fromstring(b)
    out = {}
    for el in root.iter():
        if _lname(el) != "properties":
            continue
        row = {_lname(c): (c.text or "").strip() for c in el}
        d = (row.get("NEW_DATE") or "")[:10]
        if not d:
            continue
        out[d] = {t: _f(row.get(col)) for t, col in UST_TENORS.items()}
    return out


def ust_bank(event=None, context=None):
    """mode=ustbank: bank the whole Treasury par yield curve (1990→today, 13 tenors). First bank pulls the
    single `all` document (one request); later runs refresh the current year only. Scheduled daily 21:30 UTC
    (Treasury posts the curve ~15:30–17:00 ET) and triggered once by a request that finds no bank."""
    doc = _get_json(UST_KEY) or {"rows": {}, "as_of": None}
    rows = doc.get("rows") or {}
    t0 = time.time()
    if not rows or (event or {}).get("full"):
        try:
            rows.update(_ust_fetch_year("all"))
        except Exception as e:  # noqa: BLE001
            doc.setdefault("errors", {})["all"] = str(e)[:100]
            for y in range(1990, _now().year + 1):
                if context and context.get_remaining_time_in_millis() < 60000:
                    break
                try:
                    rows.update(_ust_fetch_year(str(y)))
                except Exception as e2:  # noqa: BLE001
                    doc.setdefault("errors", {})[str(y)] = str(e2)[:80]
    else:
        for y in [_now().year] + ([_now().year - 1] if _now().month == 1 else []):
            try:
                rows.update(_ust_fetch_year(str(y)))
            except Exception as e:  # noqa: BLE001
                doc.setdefault("errors", {})[str(y)] = str(e)[:80]
    doc.update({"rows": rows, "as_of": _iso(), "n_days": len(rows), "first": min(rows) if rows else None, "last": max(rows) if rows else None,
                "source": "home.treasury.gov daily_treasury_yield_curve", "tenors": list(UST_TENORS), "elapsed_s": round(time.time() - t0, 1)})
    s3.put_object(Bucket=BUCKET, Key=UST_KEY, Body=gzip.compress(json.dumps(doc, separators=(",", ":")).encode()), ContentType="application/json", ContentEncoding="gzip",
                  CacheControl="public, max-age=900")
    return {"ok": bool(rows), "n_days": len(rows), "first": doc.get("first"), "last": doc.get("last"), "elapsed_s": doc["elapsed_s"], "errors": doc.get("errors")}


def ust_curve():
    """Request path: never fetch 37 years inline. Missing bank -> kick mode=ustbank (Event) and tell the caller;
    stale bank (> 1 weekday) -> refresh the current year inline (one small request)."""
    doc = _get_json(UST_KEY)
    if not doc or not doc.get("rows"):
        try:
            _lambda.invoke(FunctionName=os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "justhodl-symdir"), InvocationType="Event", Payload=json.dumps({"mode": "ustbank"}).encode())
        except Exception:  # noqa: BLE001
            pass
        raise ValueError("banking the Treasury par yield curve now (1990→today, one-time) — open again in a minute")
    if _bank_stale({"last_date": doc.get("last") or max(doc["rows"])}):
        try:
            doc["rows"].update(_ust_fetch_year(str(_now().year)))
            doc.update({"as_of": _iso(), "n_days": len(doc["rows"]), "last": max(doc["rows"])})
            s3.put_object(Bucket=BUCKET, Key=UST_KEY, Body=gzip.compress(json.dumps(doc, separators=(",", ":")).encode()), ContentType="application/json", ContentEncoding="gzip",
                          CacheControl="public, max-age=900")
        except Exception:  # noqa: BLE001
            pass
    return doc


def r_ustpar(sid, tenor, d):
    tenor = tenor.upper()
    if tenor not in UST_TENORS:
        raise ValueError("tenor %s not in %s" % (tenor, list(UST_TENORS)))
    doc = ust_curve()
    obs = [(dt, v.get(tenor)) for dt, v in sorted((doc.get("rows") or {}).items()) if v.get(tenor) is not None]
    return _result(sid, "ustpar", obs, name="US Treasury par yield curve %s (daily, Treasury)" % tenor, unit="Percent", freq="D",
                   source="warehouse:treasury-par (%s)" % (doc.get("source") or "treasury.gov"), extra={"tenor": tenor, "as_of": doc.get("as_of")})


def r_official_yield(sid, rest, d):
    """official-yields lane (justhodl-repo): de-10y-bbk (Bundesbank daily), ea-aaa-10y-ecb."""
    key = "data/warm/official-yields/%s.json" % rest
    j = _get_json(key) or {}
    pts = j.get("points") or j.get("observations") or j.get("series") or []
    obs = []
    for o in pts:
        if isinstance(o, dict):
            obs.append((o.get("date") or o.get("d"), o.get("value") if o.get("value") is not None else o.get("v")))
        elif isinstance(o, (list, tuple)) and len(o) >= 2:
            obs.append((o[0], o[1]))
    return _result(sid, "official-yields", obs, name=j.get("name") or j.get("title") or rest, unit="Percent", freq="D", source="warehouse:official-yields (%s)" % (j.get("source") or rest))


RESOLVERS = {"ustpar": r_ustpar, "official-yields": r_official_yield, "fred": r_fred, "te": r_te, "eurostat": r_eurostat, "ecb": r_ecb, "nyfed": r_nyfed, "ofr": r_ofr,
             "ofr-hfm": lambda s, r, d: r_ofr(s, r, d, "ofr-hfm"), "ofr-bsrm": lambda s, r, d: r_ofr(s, r, d, "ofr-bsrm"),
             "boj": r_boj, "statcan": r_statcan, "worldbank": r_worldbank, "treasury": r_treasury, "boe": r_boe, "census": r_census, "bls": r_bls}
CACHE_TTL = {"ustpar": 3600, "official-yields": 3600, "tv": 6 * 3600, "equity": 6 * 3600, "fred": 6 * 3600, "te": 86400, "eurostat": 86400, "ecb": 6 * 3600, "nyfed": 3600, "ofr": 6 * 3600, "boj": 86400, "statcan": 86400,
             "worldbank": 86400, "treasury": 6 * 3600, "boe": 86400, "census": 86400, "bls": 86400}


def _bare_resolve(bare):
    """Bare id -> provider id when it is not a known instrument but IS a directory series (FRED wins ties)."""
    try:
        ix = load_index()
    except Exception:  # noqa: BLE001
        return None
    rows, _ = _prefix_range(ix["ids"], bare, cap=3)
    if any(k == bare for k, i in rows) and any(ix["docs"][i][D_KIND] == "instrument" for k, i in rows if k == bare):
        return None
    if bare.startswith(("X:", "C:", "I:")):
        return None
    cands = []
    lo = bisect.bisect_left(ix["bare"], (bare,))
    hi = bisect.bisect_right(ix["bare"], (bare, float("inf")))
    for k, i in ix["bare"][lo:hi]:
        d = ix["docs"][i]
        if d[D_KIND] == "series" and d[D_PROV] in RESOLVERS:
            cands.append(d)
    if not cands:
        return None
    cands.sort(key=lambda d: (0 if d[D_PROV] == "fred" else 1, -d[D_POP]))
    return cands[0][D_ID]


def closest_ids(sid, limit=5):
    """Directory ids that extend the typed id (TVC:US10 -> TVC:US10Y, TVC:US10Y…), most popular first."""
    try:
        ix = load_index()
    except Exception:  # noqa: BLE001
        return []
    Q = sid.upper().strip()
    rows, _ = _prefix_range(ix["ids"], Q, cap=200)
    out = [(ix["pop"][i], ix["docs"][i]) for k, i in rows if k != Q]
    if ":" in Q:
        bare = Q.split(":", 1)[1]
        rows2, _ = _prefix_range(ix["bare"], bare, cap=200)
        out += [(ix["pop"][i] - 0.05, ix["docs"][i]) for k, i in rows2 if ix["docs"][i][D_ID].upper() != Q]
    seen, res = set(), []
    for pop, d in sorted(out, key=lambda x: -x[0]):
        if d[D_ID] in seen or d[D_KIND] == "dataset":
            continue
        seen.add(d[D_ID])
        res.append({"id": d[D_ID], "note": "closest symbol: %s" % (d[D_TITLE] or "")[:60], "pop": round(float(pop), 3)})
        if len(res) >= limit:
            break
    return res


def fetch_series(sid, nocache=False, _depth=0):
    try:
        return _fetch_series(sid, nocache=nocache)
    except Exception as e:  # noqa: BLE001
        if _depth > 0 or not sid or ":" not in sid:
            raise
        cands = closest_ids(sid)
        # a typed id that is the prefix of a clearly better-known real id resolves to it (TVC:US10 -> TVC:US10Y)
        if cands and (len(cands) == 1 or cands[0]["pop"] >= cands[1]["pop"] + 0.1 or cands[0]["id"].upper().startswith(sid.upper().strip())):
            try:
                out = fetch_series(cands[0]["id"], nocache=nocache, _depth=1)
                out["resolved_from"], out["requested"] = sid, sid
                out["source"] = "%s (%s → %s)" % (out.get("source"), sid, cands[0]["id"])
                return out
            except Exception:  # noqa: BLE001
                pass
        alts = list(getattr(e, "alternatives", None) or []) + [c for c in cands if c["id"] not in {a.get("id") for a in (getattr(e, "alternatives", None) or [])}]
        err = ValueError(str(e)[:200])
        err.alternatives = alts[:6]
        raise err


def _fetch_series(sid, nocache=False):
    sid = (sid or "").strip()
    if sid.upper().startswith("FRED:"):
        sid = "fred:" + sid[5:]
    prov, _, rest = sid.partition(":")
    prov = prov.lower()
    if prov in RESOLVERS and rest:
        sid = prov + ":" + rest
    elif ":" in sid and sid.split(":", 1)[0].upper() not in ("X", "C", "I", "TV"):
        prov, rest, sid = "tv", sid.upper(), sid.upper()                                                    # EXCHANGE:SYMBOL (TradingView universe)
    else:
        if sid.upper().startswith("TV:"):
            sid = sid[3:]
            prov, rest, sid = "tv", sid.upper(), sid.upper()
        else:
            # a bare id: an instrument charts as an equity; otherwise a series id typed without its provider
            # (HQMCB10YRP -> fred:HQMCB10YRP) resolves through the directory's bare index, FRED first
            bare = sid.upper()
            hit = _bare_resolve(bare)
            if hit:
                prov, rest, sid = hit.split(":", 1)[0], hit.split(":", 1)[1], hit
            else:
                prov, rest, sid = "equity", bare, bare                                                       # bare ticker or Polygon X:/C:/I:
    ttl = CACHE_TTL.get(prov, 21600)
    if not nocache:
        c, age = _cache_get(sid, ttl)
        if c:
            c["cached"] = True
            c["cache_age_s"] = int(age)
            return c
    d = _doc_lookup(sid)
    if prov == "tv":
        out = r_tvsym(sid, rest, d)
    elif prov == "equity":
        out = r_equity(sid, rest, d)
    else:
        out = RESOLVERS[prov](sid, rest, d)
    if out.get("n"):
        _cache_put(sid, out)
    out["cached"] = False
    return out


def quote(ids):
    out = {}
    fetch_budget = 12
    for sid in ids[:60]:
        sid = sid.strip()
        if not sid:
            continue
        try:
            prov = sid.split(":", 1)[0].lower()
            if prov == "fred" and not sid.startswith("fred:"):
                sid = "fred:" + sid.split(":", 1)[1]
            if prov not in RESOLVERS:
                sid = sid.upper()
            c, age = _cache_get(sid, 10 * 86400)
            if c is None and fetch_budget > 0:
                fetch_budget -= 1
                c = fetch_series(sid)
            if not c or not c.get("obs"):
                out[sid] = {"ok": False}
                continue
            obs = c["obs"]
            last, prev = obs[-1], (obs[-2] if len(obs) > 1 else None)
            def _at(days):
                target = (datetime.fromisoformat(last[0]) - timedelta(days=days)).date().isoformat()
                i = bisect.bisect_right(obs, [target, float("inf")]) - 1
                return obs[i][1] if i >= 0 else None
            m1, q1, y1 = _at(30), _at(91), _at(365)
            pct = lambda a, b: (round((a - b) / abs(b) * 100, 3) if (a is not None and b not in (None, 0)) else None)  # noqa: E731
            out[sid] = {"ok": True, "name": c.get("name"), "unit": c.get("unit"), "freq": c.get("freq"), "last": last[1], "last_date": last[0],
                        "prev": prev[1] if prev else None, "prev_date": prev[0] if prev else None, "chg": (last[1] - prev[1]) if prev else None,
                        "chg_pct": pct(last[1], prev[1]) if prev else None, "mom_pct": pct(last[1], m1), "qoq_pct": pct(last[1], q1), "yoy_pct": pct(last[1], y1),
                        "n": c.get("n"), "first": c.get("first"), "spark": [v for _, v in obs[-40:]], "cached": c.get("cached", True)}
        except Exception as e:  # noqa: BLE001
            out[sid] = {"ok": False, "error": str(e)[:100]}
    return out


# ================================================================ EXPLORER (every provider -> every dataset/series, paged)

_HUB = {"doc": None, "at": 0}


def hub():
    if not _HUB["doc"] or time.time() - _HUB["at"] > 900:
        _HUB["doc"] = _get_json("data/provider-catalog.json") or {}
        _HUB["at"] = time.time()
    return _HUB["doc"]


def explorer(qs):
    """The Data Explorer behind chart-pro: no provider -> every provider with what it holds (hub counts + directory
    counts); provider=slug -> its datasets/series from the directory (paged, filtered, most popular first), falling
    back to the provider's hub catalog file for providers the directory does not index."""
    prov = (qs.get("provider") or "").strip().lower()
    q = (qs.get("q") or "").strip().lower()
    kind = (qs.get("kind") or "").strip()
    offset, limit = int(qs.get("offset") or 0), max(1, min(int(qs.get("limit") or 200), 500))
    ix = load_index()
    docs = ix["docs"]
    if not prov:
        counts = defaultdict(lambda: {"series": 0, "dataset": 0, "instrument": 0})
        for d in docs:
            counts[d[D_PROV]][d[D_KIND]] += 1
        h = hub()
        out = []
        for p in h.get("providers") or []:
            slug = p.get("slug")
            c = counts.get(slug, {})
            out.append({"slug": slug, "name": p.get("name"), "api": p.get("api"), "datasets": p.get("datasets"), "series_count": p.get("series_count"), "mb": p.get("total_mb"),
                        "freshest_h": p.get("freshest_h"), "coverage_pct": p.get("coverage_pct"), "in_directory": dict(c), "note": (p.get("catalog_note") or "")[:160],
                        "engines": (p.get("engines") or [])[:6]})
        for slug, c in counts.items():
            if slug not in {x["slug"] for x in out}:
                out.append({"slug": slug, "name": PROV_NAME.get(slug, slug.upper()), "in_directory": dict(c), "virtual": True})
        out.sort(key=lambda x: -(sum((x.get("in_directory") or {}).values()) + (x.get("datasets") or 0)))
        return {"providers": out, "as_of": h.get("as_of"), "totals": h.get("totals"), "directory_docs": len(docs),
                "series_level": {"eurostat": 564204235, "ecb": 3240832}}
    toks = tokens(q) if q else []
    rows, total = [], 0
    for i, d in enumerate(docs):
        if d[D_PROV] != prov:
            continue
        if kind and d[D_KIND] != kind:
            continue
        if toks:
            hay = (d[D_TITLE] + " " + d[D_ID]).lower()
            if not all(t in hay for t in toks):
                continue
        total += 1
        if total > offset and len(rows) < limit:
            rows.append(doc_row(d))
    if total == 0:
        # providers the directory does not index (gdelt: 402k daily event files) list from their hub catalog:
        # the first list-of-records field wins; a list of S3 keys becomes one row per key
        cat = _get_json("data/providers/%s.json" % prov) or {}
        items = []
        for k, v in cat.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                items = v
                break
        if not items:
            for k, v in cat.items():
                if isinstance(v, dict) and len(v) > 3 and k in ("datasets", "series", "flows", "files", "keys"):
                    items = [dict(x, id=kk) if isinstance(x, dict) else {"id": kk, "name": str(x)[:120]} for kk, x in v.items()]
                    break
        if not items:
            for k, v in cat.items():
                if isinstance(v, list) and v and isinstance(v[0], str):
                    items = [{"id": x.rsplit("/", 1)[-1], "name": x, "key": x} for x in v]
                    break
        if toks:
            items = [it for it in items if all(t in (json.dumps(it, default=str).lower()) for t in toks)]
        total = len(items)
        for it in items[offset:offset + limit]:
            iid = str(it.get("id") or it.get("key") or it.get("name") or it.get("slug") or "")
            rows.append({"id": "%s:%s" % (prov, iid), "symbol": iid[:80], "name": str(it.get("name") or it.get("title") or it.get("key") or iid)[:200],
                         "provider": prov, "provider_name": PROV_NAME.get(prov, prov.upper()), "kind": "dataset", "chartable": False, "browse": False,
                         "first": it.get("first") or it.get("first_obs"), "last": it.get("last") or it.get("last_obs") or it.get("updated") or it.get("modified"), "n": it.get("rows") or it.get("n") or it.get("series"),
                         "mb": it.get("mb") or it.get("size_mb") or (round(it["bytes"] / 1e6, 2) if isinstance(it.get("bytes"), (int, float)) else None)})
    facets = defaultdict(int)
    for d in docs:
        if d[D_PROV] == prov:
            facets[d[D_KIND]] += 1
    h = next((p for p in (hub().get("providers") or []) if p.get("slug") == prov), None) or {}
    return {"provider": prov, "provider_name": h.get("name") or PROV_NAME.get(prov, prov.upper()), "api": h.get("api"), "hub": {k: h.get(k) for k in ("datasets", "series_count", "total_mb", "freshest_h", "coverage_pct", "catalog_note")},
            "total": total, "offset": offset, "limit": limit, "rows": rows, "kinds": dict(facets),
            "series_level_via_browse": prov in ("eurostat", "ecb", "statcan", "worldbank", "boj", "census", "treasury", "ofr")}


# ================================================================ HANDLER

def _resp(obj, status=200, ttl=60):
    return {"statusCode": status,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=%d" % ttl, "X-Symdir": VERSION},
            "body": json.dumps(obj, separators=(",", ":"), default=str)}


def lambda_handler(event, context):
    event = event or {}
    mode = event.get("mode")
    path = (event.get("rawPath") or "").rstrip("/")
    qs = event.get("queryStringParameters") or {}
    if mode == "build":
        return build(event, context)
    if mode == "titles":
        return fetch_titles(event, context)
    if mode == "codelists":
        return codelists_lane(event, context)
    if mode == "fredfresh":
        return fred_fresh(event, context)
    if mode == "fredupdates":
        return fred_updates(event, context)
    if mode == "ustbank":
        return ust_bank(event, context)
    if mode == "warm" or path == "/warm":
        # keep-warm ping (every 5 min): also the moment a warm container notices a newer daily build
        force = qs.get("force") == "1"
        if not force and _IDX["docs"] is not None:
            man = _get_json(SD + "manifest.json") or {}
            if (man.get("built_at") or "") > (_IDX.get("built_at") or ""):
                force = True
        ix = load_index(force=force)
        out = {"ok": True, "docs": len(ix["docs"]), "load_s": ix.get("load_s"), "built_at": ix.get("built_at"), "reloaded": force}
        if path == "/warm":
            return _resp(out, ttl=0)
        return out
    if mode == "search" or path == "/search":
        q = qs.get("q") or event.get("q") or ""
        try:
            lim = max(1, min(int(qs.get("limit") or event.get("limit") or 40), 200))
            t = time.time()
            out = search(q, lim, prov=(qs.get("provider") or None), kind=(qs.get("kind") or None))
            out["ms"] = int((time.time() - t) * 1000)
            out["built_at"] = _IDX.get("built_at")
            return _resp(out, ttl=120)
        except Exception as e:  # noqa: BLE001
            return _resp({"q": q, "rows": [], "error": str(e)[:200], "trace": traceback.format_exc()[-600:]}, 500, ttl=0)
    if mode == "browse" or path == "/browse":
        try:
            t = time.time()
            out = browse(qs.get("ds") or event.get("ds"), qs.get("q") or "", max(1, min(int(qs.get("limit") or 200), 1000)), int(qs.get("offset") or 0))
            out["ms"] = int((time.time() - t) * 1000)
            return _resp(out, ttl=300)
        except Exception as e:  # noqa: BLE001
            return _resp({"rows": [], "error": str(e)[:200], "trace": traceback.format_exc()[-600:]}, 500, ttl=0)
    if mode == "series" or path == "/series":
        sid = qs.get("id") or event.get("id") or ""
        try:
            t = time.time()
            out = fetch_series(sid, nocache=(qs.get("nocache") == "1"))
            out["ms"] = int((time.time() - t) * 1000)
            return _resp(out, ttl=900)
        except Exception as e:  # noqa: BLE001
            alts = getattr(e, "alternatives", None) or []
            return _resp({"id": sid, "obs": [], "error": str(e)[:200], "alternatives": alts, "trace": traceback.format_exc()[-600:]}, 500, ttl=0)
    if mode == "explorer" or path == "/explorer":
        try:
            t = time.time()
            out = explorer(qs)
            out["ms"] = int((time.time() - t) * 1000)
            return _resp(out, ttl=300)
        except Exception as e:  # noqa: BLE001
            return _resp({"rows": [], "error": str(e)[:200], "trace": traceback.format_exc()[-600:]}, 500, ttl=0)
    if mode == "quote" or path == "/quote":
        ids = (qs.get("ids") or event.get("ids") or "")
        if isinstance(ids, str):
            ids = ids.split(",")
        t = time.time()
        out = quote(ids)
        return _resp({"quotes": out, "ms": int((time.time() - t) * 1000)}, ttl=600)
    if path in ("", "/", "/health"):
        man = _get_json(SD + "manifest.json") or {}
        return _resp({"ok": True, "version": VERSION, "index_loaded": _IDX["docs"] is not None, "docs": len(_IDX["docs"]) if _IDX["docs"] else None,
                      "directory_built_at": man.get("built_at"), "directory_docs": man.get("docs"),
                      "routes": ["/search?q=", "/browse?ds=&q=", "/series?id=", "/quote?ids=", "/warm"]}, ttl=30)
    return _resp({"error": "unknown route", "path": path}, 404, ttl=0)
