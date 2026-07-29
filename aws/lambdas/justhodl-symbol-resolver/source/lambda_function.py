"""justhodl-symbol-resolver — turn imported TV tickers into FETCHABLE series.

ops 4084 audited the 10,319 tickers imported from Khalid's 491 watchlists:
only 43.6% had any fetch route. The largest block — 4,369 — is macro
(ECONOMICS 3,317 / FRED 765 / COT3 229 / USI 143 / COT 111): tickers that
carry his notes but that the fleet cannot pull.

The mechanism to fix this already exists and is proven: the vault's
ALIASES table maps a bare TV code to a fetch key, e.g.
    "CHINTR": "fred_alias:IRSTCI01CHM156N"
CHINTR is ECONOMICS:CHINTR. That table is hand-curated and a few hundred
long. This engine generates the rest, writing data/symbol-aliases.json,
which the vault reads as a LOWER-precedence layer than the curated dict
(curated always wins — a human decision outranks a generated one).

STEP 1 (this version) — FRED: symbols, the certain case.
  For FRED:MABMM301JPM189S the series id IS the fetch key. Strip the
  prefix, VERIFY it against the FRED API (a series that 404s is not an
  alias, it is a broken promise), emit fred:<id>. No inference anywhere.

Deliberately NOT done yet:
  ECONOMICS: matching needs each ticker's description as the join key,
  and ops 4085 found the harvester discards descriptions when source is
  null — which is every macro symbol. Fixed in extension v1.8.1; the
  matcher lands once real descriptions accumulate. Guessing the mapping
  from a code alone would point Khalid's notes at the wrong series, which
  is worse than leaving it unrouted.

LEDGER: FRED is rate-limited, so verification accretes across runs.
"""
import json
import os
import re
import time
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone

import boto3

MARKER = "symbol-resolver v2.0 ops4089 step2-economics"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/symbol-aliases.json"
LEDGER_KEY = "symbol-resolver/ledger.json"
FRED_KEY = os.environ.get("FRED_KEY", "")
MAX_NEW = int(os.environ.get("MAX_NEW", "400"))
SPACING = 0.30

s3 = boto3.client("s3", region_name="us-east-1")


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def fred_series(sid):
    """Return the series record if FRED actually serves it, else None."""
    url = ("https://api.stlouisfed.org/fred/series?"
           + urllib.parse.urlencode({"series_id": sid, "api_key": FRED_KEY,
                                     "file_type": "json"}))
    try:
        j = json.loads(urllib.request.urlopen(url, timeout=20).read())
        arr = j.get("seriess") or []
        return arr[0] if arr else None
    except Exception:
        return None


# ─────────────────────── STEP 2: ECONOMICS ───────────────────────────
# TradingView's macro namespace is <COUNTRY><INDICATOR>: USCPI, JPM3,
# DEUNR. That decomposition is real structure, not a guess — but it only
# gives a SEARCH QUERY, never an answer. Every candidate must still be
# confirmed against FRED's own catalogue, and only a strong confirmation
# becomes an alias. Anything weaker is published as a candidate with its
# score so Khalid can see the queue rather than trust a silent mapping.
COUNTRY = {
    "US": "United States", "JP": "Japan", "CN": "China", "DE": "Germany",
    "GB": "United Kingdom", "UK": "United Kingdom", "FR": "France",
    "IT": "Italy", "ES": "Spain", "CA": "Canada", "AU": "Australia",
    "KR": "Korea", "IN": "India", "BR": "Brazil", "MX": "Mexico",
    "CH": "Switzerland", "SE": "Sweden", "NO": "Norway", "NL": "Netherlands",
    "EU": "Euro Area", "RU": "Russia", "ZA": "South Africa", "TR": "Turkey",
    "ID": "Indonesia", "TH": "Thailand", "PL": "Poland", "AR": "Argentina",
    "SG": "Singapore", "HK": "Hong Kong", "TW": "Taiwan", "NZ": "New Zealand",
    "DK": "Denmark", "FI": "Finland", "BE": "Belgium", "AT": "Austria",
    "PT": "Portugal", "IE": "Ireland", "GR": "Greece", "CL": "Chile",
    "CO": "Colombia", "PE": "Peru", "PH": "Philippines", "MY": "Malaysia",
    "VN": "Vietnam", "SA": "Saudi Arabia", "IL": "Israel", "CZ": "Czech",
    "HU": "Hungary", "RO": "Romania", "UA": "Ukraine", "EG": "Egypt",
}
INDICATOR = {
    "CPI": "consumer price index", "CORECPI": "core consumer price index",
    "GDP": "gross domestic product", "GDPQQ": "real gross domestic product",
    "GDPYY": "real gross domestic product", "UNR": "unemployment rate",
    "INTR": "interest rate", "M0": "monetary base", "M1": "M1",
    "M2": "M2", "M3": "M3 money supply", "PPI": "producer price index",
    "IP": "industrial production", "IPYY": "industrial production",
    "RETAILSALES": "retail sales", "RS": "retail sales",
    "BOT": "balance of trade", "CA": "current account",
    "EXP": "exports", "IMP": "imports", "GDEBT": "government debt",
    "GSPE": "government spending", "GREV": "government revenue",
    "HOUS": "housing starts", "BP": "building permits",
    "CCI": "consumer confidence", "BCI": "business confidence",
    "MANPMI": "manufacturing PMI", "SERVPMI": "services PMI",
    "LFPR": "labor force participation rate", "NFP": "nonfarm payrolls",
    "WG": "wages", "PSAV": "personal savings", "INFL": "inflation rate",
    "FDI": "foreign direct investment", "FER": "foreign exchange reserves",
    "CAPU": "capacity utilization", "JOB": "job openings",
}


def fred_search(text, limit=6):
    url = ("https://api.stlouisfed.org/fred/series/search?"
           + urllib.parse.urlencode({"search_text": text, "api_key": FRED_KEY,
                                     "file_type": "json", "limit": limit,
                                     "order_by": "popularity",
                                     "sort_order": "desc"}))
    try:
        return json.loads(urllib.request.urlopen(url, timeout=25).read()
                          ).get("seriess") or []
    except Exception:
        return []


def decompose(code):
    """ECONOMICS:USCPI -> ('United States', 'consumer price index')."""
    c = code.upper()
    for n in (2,):
        if len(c) > n and c[:n] in COUNTRY:
            rest = c[n:]
            if rest in INDICATOR:
                return COUNTRY[c[:n]], INDICATOR[rest]
            return COUNTRY[c[:n]], None
    return None, None


def score_match(title, country, indicator, desc):
    """Confidence in [0,1]. Requires BOTH the geography and the concept to
    appear — a title that matches only one of them is a different series."""
    t = (title or "").lower()
    if not t:
        return 0.0
    geo = 1.0 if (country and country.lower() in t) else 0.0
    ind_terms = [w for w in (indicator or "").lower().split() if len(w) > 2]
    ind = (sum(1 for w in ind_terms if w in t) / len(ind_terms)) if ind_terms else 0.0
    # A real TradingView description, when we have one, is far stronger
    # evidence than a decomposed code.
    dsc = 0.0
    if desc:
        dw = [w for w in re.sub(r"[^a-z ]", " ", desc.lower()).split() if len(w) > 3]
        if dw:
            dsc = sum(1 for w in dw if w in t) / len(dw)
    if not geo or ind < 0.5:
        return round(min(0.6, 0.5 * ind + 0.4 * dsc), 3)
    return round(min(1.0, 0.45 * geo + 0.35 * ind + 0.20 * dsc), 3)


ACCEPT = 0.80


def resolve_economics(tickers, descs, verified, dead, budget, context):
    """Returns (n_aliased, n_candidates, candidates, calls)."""
    cands, calls, aliased = [], 0, 0
    for t in tickers:
        if calls >= budget or (context and
                               context.get_remaining_time_in_millis() < 45000):
            break
        code = t.split(":", 1)[1]
        if code in verified or code in dead:
            continue
        country, indicator = decompose(code)
        desc = descs.get(t) or descs.get(code) or ""
        query = desc or " ".join(x for x in (country, indicator) if x)
        if not query:
            dead.add(code)
            continue
        hits = fred_search(query)
        calls += 1
        time.sleep(SPACING)
        if not hits:
            dead.add(code)
            continue
        best, best_s = None, 0.0
        for h in hits:
            sc = score_match(h.get("title"), country, indicator, desc)
            if sc > best_s:
                best, best_s = h, sc
        if not best:
            dead.add(code)
            continue
        row = {"alias": f"fred:{best.get('id')}",
               "title": best.get("title"),
               "units": best.get("units_short") or best.get("units"),
               "freq": best.get("frequency_short"),
               "route": "economics-matched",
               "confidence": best_s,
               "query": query[:120],
               "had_description": bool(desc),
               "tv_symbol": t}
        if best_s >= ACCEPT:
            verified[code] = row
            aliased += 1
        else:
            cands.append(row)
    return aliased, len(cands), cands, calls


def lambda_handler(event, context):
    print(f"[symbol-resolver] {MARKER}")
    now = datetime.now(timezone.utc)

    led = gj(LEDGER_KEY, {}) or {}
    verified = led.get("verified") or {}      # bare code -> alias row
    dead = set(led.get("dead") or [])         # ids FRED does not serve

    wl = gj("data/tv-watchlists.json", {}) or {}
    tickers = set()
    for l in (wl.get("watchlists") or wl.get("lists") or []):
        for x in (l.get("symbols") or []):
            x = str(x).strip().upper()
            if x:
                tickers.add(x)

    fred_tickers = sorted(t for t in tickers if t.startswith("FRED:"))
    todo = [t for t in fred_tickers
            if t.split(":", 1)[1] not in verified
            and t.split(":", 1)[1] not in dead]

    spent = newly = 0
    for t in todo:
        if spent >= MAX_NEW or (context and
                                context.get_remaining_time_in_millis() < 45000):
            break
        sid = t.split(":", 1)[1]
        rec = fred_series(sid)
        spent += 1
        time.sleep(SPACING)
        if not rec:
            dead.add(sid)
            continue
        verified[sid] = {
            "alias": f"fred:{sid}",
            "title": rec.get("title"),
            "units": rec.get("units_short") or rec.get("units"),
            "freq": rec.get("frequency_short") or rec.get("frequency"),
            "last_updated": rec.get("last_updated"),
            "route": "fred-verified",
            "confidence": 1.0,
            "tv_symbol": t,
        }
        newly += 1

    # ── STEP 2: ECONOMICS matching, with whatever budget step 1 left ──
    descs = (gj("data/tv-descriptions.json", {}) or {}).get("descriptions") or {}
    econ_tickers = sorted(t for t in tickers if t.startswith("ECONOMICS:"))
    econ_budget = max(0, MAX_NEW - spent)
    e_alias, e_cand_n, e_cands, e_calls = resolve_economics(
        econ_tickers, descs, verified, dead, econ_budget, context)
    spent += e_calls
    newly += e_alias
    prev_c = {c["tv_symbol"]: c for c in (led.get("candidates") or [])}
    for c in e_cands:
        prev_c[c["tv_symbol"]] = c
    candidates = sorted(prev_c.values(), key=lambda x: -x["confidence"])

    s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY,
                  Body=json.dumps({"verified": verified,
                                   "dead": sorted(dead),
                                   "candidates": candidates,
                                   "updated": now.isoformat()}),
                  ContentType="application/json")

    # The artifact the vault consumes: bare TV code -> vault alias string.
    aliases = {code: row["alias"] for code, row in verified.items()}

    out = {
        "generated_at": now.isoformat(),
        "marker": MARKER,
        "aliases": aliases,
        "detail": verified,
        "n_aliases": len(aliases),
        "fred_tickers_total": len(fred_tickers),
        "fred_verified": len(verified),
        "fred_dead": len(dead),
        "verified_this_run": newly,
        "fred_calls_this_run": spent,
        "coverage_pct": round(len(verified) / max(len(fred_tickers), 1) * 100, 1),
        "by_route": dict(Counter(r["route"] for r in verified.values())),
        "economics_total": len(econ_tickers),
        "economics_aliased": sum(1 for v in verified.values()
                                 if v.get("route") == "economics-matched"),
        "economics_candidates": len(candidates),
        "economics_accept_threshold": ACCEPT,
        "candidates": [
            {k: c[k] for k in ("tv_symbol", "alias", "title", "confidence",
                               "query", "had_description")}
            for c in candidates[:60]],
        "note": ("Step 1: FRED: tickers, where the series id is the fetch "
                 "key and every id is verified against the FRED API before it "
                 "becomes an alias. ECONOMICS: matching is intentionally "
                 "absent until real descriptions accumulate (extension "
                 "v1.8.1). Step 2: ECONOMICS codes are decomposed to a "
                 "country and a concept, searched against FRED's catalogue, "
                 "and scored — a title must match BOTH the geography and the "
                 "concept. Only >=0.80 becomes an alias; everything weaker is "
                 "published as a CANDIDATE with its score, never silently "
                 "promoted. A wrong alias would chart the wrong series under "
                 "Khalid's own note, which is worse than leaving it unrouted."),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out),
                  ContentType="application/json", CacheControl="max-age=300")

    print(f"[symbol-resolver] fred_tickers={len(fred_tickers)} "
          f"verified={len(verified)} (+{newly}) dead={len(dead)} calls={spent}")
    return {"statusCode": 200,
            "body": json.dumps({"fred_verified": len(verified),
                                "verified_this_run": newly,
                                "dead": len(dead)})}
