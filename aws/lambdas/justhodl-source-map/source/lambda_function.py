"""justhodl-source-map — the attribution rollup engine.

WHY THIS EXISTS (ops 4071 finding): data/source-map.json — the artifact
harvest-monitor.html renders — had NO producer.  A probe across all 756
fleet functions found zero Lambdas writing it and zero schedules touching
it.  It existed only because an ops script wrote it by hand, which means
the monitor froze the moment the session ended.  That is precisely the
"declared != live" failure this fleet has been burned by before, so the
logic is promoted here into a real scheduled engine.

WHAT IT DOES
  1. Reads data/tv-sources.json (the extension's landing artifact).
  2. Normalises attribution: strips the source/ provider/ country/
     prefixes the TV payloads carry, drops lowercase-slug junk (logoids
     like "django_model" that are rendering hints, not publishers).
  3. Writes the cleaned store back — idempotent, so junk that re-lands on
     a later sync is purged automatically instead of accumulating.
  4. Rolls attribution up into KNOWN agency families and NEW sources.
  5. NEW vs the hand-run version: an ECONOMICS→agency rollup, agency
     coverage against the gov-sources registry, and harvest progress
     telemetry — the numbers that say whether the walk is actually
     reaching the payoff rather than grinding through trading venues.

Every key written here is rendered by harvest-monitor.html; the field
coverage is asserted by the deploy op.
"""
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone

import boto3

MARKER = "source-map engine v2.1 ops4083"
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

# Junk = pure lowercase slugs. TV ships logoids ("django_model",
# "us_treasury_logo") in the same fields as real publisher names; a real
# publisher has capitals or spaces.
JUNK_RX = re.compile(r"^[a-z0-9_]{3,}$")
PFX_RX = re.compile(r"^(?:source|provider|country)/", re.I)

# Agency families. Keys mirror the justhodl-gov-sources registry so the
# two engines speak the same vocabulary and can be joined.
KNOWN = {
    "FRED": ("federal reserve", "fred", "st. louis"),
    "US-TREASURY": ("u.s. department of the treasury", "treasury"),
    "BLS": ("bureau of labor",),
    "BEA": ("bureau of economic analysis",),
    "CENSUS-US": ("u.s. census bureau", "united states census"),
    "ECB": ("european central bank",),
    "EUROSTAT": ("eurostat",),
    "BOJ": ("bank of japan",),
    "MOF-JAPAN": ("ministry of finance (japan", "japan ministry of finance",
                  "ministry of finance, japan"),
    "ESTAT-JAPAN": ("statistics bureau of japan", "e-stat"),
    "BOE": ("bank of england",),
    "SNB": ("swiss national bank",),
    "NORGES": ("norges bank",),
    "BCRP-PERU": ("banco central de reserva",),
    "BCB-BRAZIL": ("banco central do brasil", "central bank of brazil"),
    "PBOC": ("people's bank of china",),
    "MOEA-TAIWAN": ("ministry of economic affairs",),
    "CFTC": ("commodity futures",),
    "SEC-EDGAR": ("securities and exchange",),
    "OFR": ("office of financial research",),
    "IMF": ("international monetary fund",),
    "HKMA": ("hong kong monetary",),
    "OECD": ("oecd",),
    "WORLD-BANK": ("world bank",),
    "COINMETRICS": ("coin metrics", "coinmetrics"),
    "COINGECKO": ("coingecko",),
    "EIA": ("energy information administration",),
    "MARKET-VENUES": (
        "tvc", "sgx", "tpex", "xetr", "omx", "six", "bme", "tradegate",
        "nasdaq", "nyse", "cboe", "cme", "ice ", "eurex", "tradingview",
        "arca", "amex", "otc", "lse ", "tsx", "borsa", "euronext", "xetra",
        "b3 ", "bmv", "hkex", "krx", "twse", "sse", "szse", "asx", "moex",
        "bist", "forex", "fx ", "binance", "coinbase", "kraken", "bitstamp",
        "bybit", "okx", "bitfinex"),
}

# Venues are attribution, but they are not the payoff — separating them
# keeps "coverage" from being inflated by knowing NVDA is on NASDAQ.
NON_AGENCY = {"MARKET-VENUES"}


def fam_of(src):
    t = str(src or "").lower()
    for fam, keys in KNOWN.items():
        if any(k in t for k in keys):
            return fam
    return None


def gj(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def lambda_handler(event, context):
    print(f"[source-map] {MARKER}")
    now = datetime.now(timezone.utc)

    sr = gj("data/tv-sources.json", {}) or {}
    store = sr.get("sources") or {}
    diag = sr.get("last_harvest_diag") or {}

    # ── 1. normalise + purge junk (idempotent) ──
    real = {}
    for k, v in store.items():
        if not isinstance(v, dict):
            continue
        n0 = PFX_RX.sub("", str(v.get("source") or "")).strip()
        if not n0 or JUNK_RX.match(n0.replace("-", "_")):
            continue
        real[k] = {"source": n0,
                   "description": v.get("description"),
                   "updated": v.get("updated")}
    junk = len(store) - len(real)

    if real and len(real) != len(store):
        sr["sources"] = real
        sr["n_symbols"] = len(real)
        s3.put_object(Bucket=BUCKET, Key="data/tv-sources.json",
                      Body=json.dumps(sr), ContentType="application/json",
                      CacheControl="max-age=120")

    # ── 2. roll up ──
    by, ex = Counter(), defaultdict(list)
    for sym, v in real.items():
        src = str(v.get("source"))
        by[src] += 1
        if len(ex[src]) < 3:
            ex[src].append(sym)

    known_ct, new_rows = Counter(), []
    for src, n in by.most_common():
        fam = fam_of(src)
        if fam:
            known_ct[fam] += n
        else:
            new_rows.append({"source": src, "n_symbols": n,
                             "examples": ex[src]})

    # ── 3. THE PAYOFF: ECONOMICS symbols → publishing agency ──
    econ = {k: v["source"] for k, v in real.items()
            if k.upper().startswith(("ECONOMICS", "FRED"))}
    econ_by = Counter(econ.values())
    economics_agencies = [{"source": s, "n_symbols": n,
                           "family": fam_of(s) or "UNMAPPED"}
                          for s, n in econ_by.most_common(40)]

    agency_families = {f: n for f, n in known_ct.items()
                       if f not in NON_AGENCY}

    # v2.1 — MACRO JOIN. TradingView returns source=null for its entire
    # macro namespace (ops 4081), so agency attribution for ECONOMICS/FRED
    # symbols cannot come from the harvester at all. justhodl-macro-
    # attribution resolves it from FRED's own series/release/sources
    # metadata and the vault's government adapters. Merged here so the
    # page shows one honest agency picture instead of a browser-only view
    # that is structurally stuck at zero.
    ma = gj("data/macro-attribution.json", {}) or {}
    macro_attr = ma.get("attribution") or {}
    macro_fams = Counter()
    for sym, v in macro_attr.items():
        macro_fams[v.get("family") or "OTHER-OFFICIAL"] += 1
    for f, n in macro_fams.items():
        agency_families[f] = agency_families.get(f, 0) + n
    for row in (ma.get("by_publisher") or [])[:40]:
        economics_agencies.append({"source": row.get("publisher"),
                                   "n_symbols": row.get("n_symbols"),
                                   "family": row.get("family")})
    econ_count_extra = len(macro_attr)
    macro_unattributed = ma.get("unattributed") or 0

    agency_rows = sum(agency_families.values())

    # ── 4. is the walk actually reaching the payoff? ──
    done = int(diag.get("done") or 0)
    total = int(diag.get("total") or 0)
    progress = {
        "walked": done,
        "total": total,
        "pct": round(done / total * 100, 1) if total else 0.0,
        "tier1_done": diag.get("tier1_done"),
        "rate_per_min": diag.get("rate_per_min"),
        "elapsed_s": diag.get("elapsed_s"),
        "matched": diag.get("matched"),
        "eta_hours": (round((total - done) / diag["rate_per_min"] / 60, 1)
                      if diag.get("rate_per_min") else None),
    }

    out = {
        "generated_at": now.isoformat(),
        "marker": MARKER,
        "symbols_with_source": len(real),
        "distinct_sources": len(by),
        "junk_purged": junk,
        "known_families": dict(known_ct),
        "agency_families": agency_families,
        "agency_rows": agency_rows,
        "venue_rows": known_ct.get("MARKET-VENUES", 0),
        "economics_agencies": economics_agencies,
        "economics_symbols": len(econ) + econ_count_extra,
        "macro_attributed": econ_count_extra,
        "macro_unattributed": macro_unattributed,
        "macro_coverage_pct": ma.get("coverage_pct"),
        "harvest_progress": progress,
        "new_sources": new_rows,
    }
    s3.put_object(Bucket=BUCKET, Key="data/source-map.json",
                  Body=json.dumps(out), ContentType="application/json",
                  CacheControl="max-age=120")

    print(f"[source-map] DONE real={len(real)} junk={junk} "
          f"agency={agency_rows} venue={out['venue_rows']} "
          f"econ={len(econ)} walked={done}/{total}")
    return {"statusCode": 200,
            "body": json.dumps({"symbols_with_source": len(real),
                                "agency_rows": agency_rows,
                                "economics_symbols": len(econ)})}
