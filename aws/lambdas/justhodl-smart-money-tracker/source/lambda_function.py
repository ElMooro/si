"""
justhodl-smart-money-tracker v2 — Top Hedge Funds Activity (per-CIK fetch)

ENDPOINT FIX (v2, 2026-05-11):
Probe 444 found holder-performance-summary requires ?cik=X (per-filer), not
year+quarter. Per-CIK call returns full historical series with QoQ data
(marketValue, previousMarketValue, securitiesAdded/Removed, portfolioSize,
performance, performancePercentage, holdingPeriod, turnover).

PIPELINE:
  1. Curated list of ~25 well-known hedge fund CIKs (Berkshire, BlackRock,
     Vanguard, Bridgewater, Renaissance, Citadel, Tiger, Pershing Square...)
  2. Plus discovery: institutional-ownership/latest pages 0-4 → 50 more CIKs
  3. For each unique CIK (~75 total):
       - GET holder-performance-summary?cik=X
       - Take most recent record (sorted by date desc)
  4. Process: sort by marketValue desc, build summary rankings
  5. Write to S3 at screener/smart-money.json

OUTPUT SCHEMA: same as v1 but with proper QoQ data:
  filers: [{cik, investor_name, market_value, qoq_change_pct,
            securities_added, securities_removed, performance_pct, ...}]
  summary: {biggest_gainers, biggest_decliners, most_active, ...}

v2.1.0 (2026-10-26, bank fusion wave 2): the FMP walker bank is the first rung of both reads - discovery from the
newest banked institutional-ownership/latest page-set (data/warm/fmp/institutional-ownership-latest/<day>.json.gz, up
to 60 pages instead of 5 live pages) and each filer's holder-performance-summary from its banked per-CIK file
(data/warm/fmp/institutional-ownership-holder-performance-summary/<cik>.json.gz); the live calls stand as the fallback
and are capped per run (`source` per filer, `source_used`, `feed_basis`).  Ranking and the summary are unchanged.
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import json
import os
import threading
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "screener/smart-money.json"

s3 = boto3.client("s3", region_name="us-east-1")

try:  # v2.1.0 bank fusion: the walker's 13F feeds (no key)
    import fmp_bank as _bank
except Exception:  # noqa: BLE001
    _bank = None
VERSION = "2.1.0"
BANK_DISCOVER_TARGET = int(os.environ.get("SM_BANK_DISCOVER_TARGET", "200"))     # filers discovered from the banked feed
LIVE_DISCOVER_TARGET = 80                                                          # the v2 live discovery, unchanged
LIVE_FILER_CAP = int(os.environ.get("SM_LIVE_FILER_CAP", "40"))                    # live holder-performance-summary calls per run, at most

# Curated list of well-known hedge funds / asset managers (CIK numbers).
# Some CIKs may be wrong/missing — invalid ones just return no data + are skipped.
WELL_KNOWN_FUNDS = [
    ("0001067983", "BERKSHIRE HATHAWAY INC"),
    ("0001364742", "BLACKROCK INC"),
    ("0000102909", "VANGUARD GROUP INC"),
    ("0001350694", "BRIDGEWATER ASSOCIATES"),
    ("0001037389", "RENAISSANCE TECHNOLOGIES LLC"),
    ("0001423053", "CITADEL ADVISORS LLC"),
    ("0001179392", "TWO SIGMA INVESTMENTS LP"),
    ("0001009207", "D.E. SHAW & CO INC"),
    ("0001167483", "TIGER GLOBAL MANAGEMENT LLC"),
    ("0001029160", "SOROS FUND MANAGEMENT LLC"),
    ("0001336528", "PERSHING SQUARE CAPITAL"),
    ("0001079114", "GREENLIGHT CAPITAL INC"),
    ("0001040273", "THIRD POINT LLC"),
    ("0001048445", "ELLIOTT INVESTMENT MGMT"),
    ("0001418814", "VALUEACT HOLDINGS LP"),
    ("0001061768", "LONE PINE CAPITAL LLC"),
    ("0001135730", "COATUE MANAGEMENT LLC"),
    ("0001031972", "BAUPOST GROUP LLC"),
    ("0001296958", "POINT72 ASSET MANAGEMENT"),
    ("0001103804", "VIKING GLOBAL INVESTORS LP"),
    ("0001541617", "MILLENNIUM MANAGEMENT LLC"),
    ("0000820027", "TUDOR INVESTMENT CORP ET AL"),
    ("0001167274", "GLENVIEW CAPITAL MGMT LLC"),
    ("0001512699", "SANDS CAPITAL MANAGEMENT"),
    ("0001275148", "WHALE ROCK CAPITAL MGMT"),
    ("0001000275", "ROYAL BANK OF CANADA"),
    ("0000093751", "STATE STREET CORP"),
    ("0000315066", "FIDELITY MANAGEMENT & RESEARCH"),
    ("0001137518", "BRIDGEWATER PURE ALPHA"),
    ("0001112520", "MAVERICK CAPITAL LTD"),
]


def fmp(path, params="", retries=2):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-SM/2.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            if e.code == 400 or e.code == 404:
                return None
            print(f"[fmp] {path}: HTTP {e.code}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            return None
    return None


def discover_bank_ciks(target=None):
    """First rung: distinct filer CIKs from the newest banked institutional-ownership/latest page-set.
    Returns ({cik: name}, day, n_rows); ({}, None, 0) when the bank has none."""
    if _bank is None or not hasattr(_bank, "walker_latest"):
        return {}, None, 0
    try:
        day, rows = _bank.walker_latest("institutional-ownership-latest", lookback_days=10)
    except Exception:  # noqa: BLE001
        day, rows = None, None
    if not rows:
        return {}, None, 0
    discovered = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        cik = r.get("cik")
        name = r.get("name") or r.get("investorName")
        if cik and str(cik) not in discovered:
            discovered[str(cik)] = name
            if len(discovered) >= (target or BANK_DISCOVER_TARGET):
                break
    return discovered, day, len(rows)


def bank_performance(cik):
    """The filer's banked holder-performance-summary rows (the live endpoint's shape) or None when not banked."""
    if _bank is None:
        return None
    try:
        rows = _bank.walker("institutional-ownership-holder-performance-summary", cik)
    except Exception:  # noqa: BLE001
        rows = None
    return rows if isinstance(rows, list) and rows else None


def discover_more_ciks(target=50):
    """Pull recent 13F filings to find more CIKs beyond the curated list."""
    discovered = {}
    for page in range(5):  # 5 × 100 = 500 records
        recs = fmp("institutional-ownership/latest", f"&page={page}&limit=100")
        if not isinstance(recs, list) or not recs:
            break
        for r in recs:
            cik = r.get("cik")
            name = r.get("name") or r.get("investorName")
            if cik and cik not in discovered:
                discovered[cik] = name
                if len(discovered) >= target:
                    break
        if len(discovered) >= target:
            break
    return discovered


_live_budget = {"n": 0}
_live_lock = threading.Lock()


def fetch_filer(cik_name):
    cik, fallback_name = cik_name
    data = bank_performance(cik)
    source = "fmp-walker bank"
    if data is None:
        with _live_lock:
            allowed = _live_budget["n"] < LIVE_FILER_CAP
            if allowed:
                _live_budget["n"] += 1
        if not allowed:
            return None                                   # live reads capped for the run: this filer waits for the bank
        data = fmp("institutional-ownership/holder-performance-summary",
                     f"&cik={cik}")
        source = "fmp live"
    if not isinstance(data, list) or not data:
        return None
    # Sort by date desc, take most recent
    data.sort(key=lambda r: r.get("date", ""), reverse=True)
    latest = data[0]

    mv = latest.get("marketValue") or 0
    prev_mv = latest.get("previousMarketValue") or 0
    if mv <= 0:
        return None

    qoq_pct = latest.get("changeInMarketValuePercentage")
    if qoq_pct is None and prev_mv > 0:
        qoq_pct = round((mv - prev_mv) / prev_mv * 100, 2)

    added = latest.get("securitiesAdded") or 0
    removed = latest.get("securitiesRemoved") or 0

    return {
        "cik": cik,
        "investor_name": latest.get("investorName") or fallback_name,
        "date": (latest.get("date", "") or "")[:10],
        "portfolio_size": latest.get("portfolioSize"),
        "securities_added": added,
        "securities_removed": removed,
        "market_value": mv,
        "previous_market_value": prev_mv,
        "change_in_market_value": latest.get("changeInMarketValue"),
        "qoq_change_pct": qoq_pct,
        "net_activity": added - removed,
        "activity_score": added + removed,
        "performance": latest.get("performance"),
        "performance_pct": latest.get("performancePercentage"),
        "avg_holding_period": latest.get("averageHoldingPeriod"),
        "avg_holding_period_top10": latest.get("averageHoldingPeriodTop10"),
        "turnover": latest.get("turnover"),
        "source": source,
    }


def lambda_handler(event, context):
    started = time.time()

    # 1. Discover additional CIKs: the banked latest feed first (v2.1.0), the live pages when the bank has none
    _live_budget["n"] = 0
    print("[sm] discovering recent 13F filers...")
    discovered, bank_day, bank_rows = discover_bank_ciks()
    if discovered:
        source_used = "fmp-walker bank institutional-ownership-latest (%s, %d rows)" % (bank_day, bank_rows)
    else:
        discovered = discover_more_ciks(target=LIVE_DISCOVER_TARGET)
        source_used = "fmp live institutional-ownership/latest" if discovered else "curated list only"
    print(f"[sm] discovered {len(discovered)} CIKs ({source_used})")

    # 2. Combine curated + discovered (dedupe)
    seen = set()
    all_targets = []
    for cik, name in WELL_KNOWN_FUNDS:
        if cik not in seen:
            seen.add(cik)
            all_targets.append((cik, name))
    for cik, name in discovered.items():
        if cik not in seen:
            seen.add(cik)
            all_targets.append((cik, name))
    print(f"[sm] {len(all_targets)} unique CIKs to fetch")

    # 3. Fetch performance summary for each in parallel
    filers = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for result in ex.map(fetch_filer, all_targets):
            if result:
                filers.append(result)
    source_counts = {}
    for f in filers:
        source_counts[f.get("source")] = source_counts.get(f.get("source"), 0) + 1
    print(f"[sm] fetched performance for {len(filers)} filers {source_counts}; live calls used {_live_budget['n']}/{LIVE_FILER_CAP}")

    # 4. Sort by AUM desc
    filers.sort(key=lambda f: -(f.get("market_value") or 0))

    # 5. Build summary
    def slim(f):
        return {
            "cik": f["cik"],
            "name": f["investor_name"],
            "mv": f["market_value"],
            "qoq_pct": f["qoq_change_pct"],
            "added": f["securities_added"],
            "removed": f["securities_removed"],
            "size": f["portfolio_size"],
            "perf_pct": f.get("performance_pct"),
        }

    summary = {
        "n_filers": len(filers),
        "most_active": [slim(f) for f in sorted(
            filers, key=lambda f: -(f.get("activity_score") or 0))[:15]],
        "biggest_gainers": [slim(f) for f in sorted(
            [f for f in filers if f.get("qoq_change_pct") is not None],
            key=lambda f: -(f.get("qoq_change_pct") or -999))[:15]],
        "biggest_decliners": [slim(f) for f in sorted(
            [f for f in filers if f.get("qoq_change_pct") is not None],
            key=lambda f: (f.get("qoq_change_pct") or 999))[:15]],
        "biggest_increasers": [slim(f) for f in sorted(
            filers, key=lambda f: -(f.get("securities_added") or 0))[:15]],
        "biggest_reducers": [slim(f) for f in sorted(
            filers, key=lambda f: -(f.get("securities_removed") or 0))[:15]],
        "best_performers": [slim(f) for f in sorted(
            [f for f in filers if f.get("performance_pct") is not None],
            key=lambda f: -(f.get("performance_pct") or -999))[:15]],
    }

    # Derive quarter label from as_of_date
    as_of = max((f.get("date") or "" for f in filers), default="")
    quarter_label = "—"
    if as_of and len(as_of) >= 10:
        try:
            m = int(as_of[5:7])
            y = int(as_of[:4])
            q = (m - 1) // 3 + 1
            quarter_label = f"Q{q} {y}"
        except Exception:
            pass

    payload = {
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "source_used": source_used,
        "feed_basis": {"discovery": source_used, "bank_day": bank_day, "bank_feed_rows": bank_rows, "n_targets": len(all_targets),
                       "filer_sources": source_counts, "live_filer_calls": _live_budget["n"], "live_filer_cap": LIVE_FILER_CAP},
        "as_of_date": as_of,
        "as_of_quarter": quarter_label,
        "n_filers": len(filers),
        "filers": filers,
        "summary": summary,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                       Body=json.dumps(payload, default=str),
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
        print(f"[s3] wrote {len(filers)} filers")
    except Exception as e:
        print(f"[s3] err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "n_filers": len(filers),
        "as_of": payload["as_of_date"],
        "elapsed_seconds": payload["elapsed_seconds"],
        "biggest_gainer": (summary["biggest_gainers"] or [None])[0],
        "biggest_decliner": (summary["biggest_decliners"] or [None])[0],
    })}
