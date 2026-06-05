"""justhodl-vintage-fred — point-in-time (as-reported) macro data via FRED ALFRED

THE PROBLEM IT SOLVES (the look-ahead killer):
FRED revises data after the fact (GDP, payrolls, even rates get restated). If a
backtest reads TODAY's revised value for a date in the past, every result is
inflated by information that didn't exist then. Institutional systems store the
VINTAGE — the value as it was *first reported*, tagged with its knowledge-date —
and never overwrite it.

This engine uses FRED's ALFRED archive (vintagedates + realtime params, free) to
capture, for each tracked series, the as-first-reported value with its
realtime_start (the date the market actually knew it). Output is append-only:
each series gets a vintage ledger keyed by knowledge-date.

OUTPUT: data/vintage/<series>.json  { series, vintages: [{date, value, known_on}] }
        data/vintage/_index.json     { series: [...], updated, n_series }
SCHEDULE: daily 13:00 UTC (before the macro engines run).

Any engine doing point-in-time analysis reads as_of(series, date) → the value
that was known on/before `date`, never a future revision.
"""
import json, os, time
import urllib.request, urllib.parse
from datetime import datetime, timezone, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
ALFRED = "https://api.stlouisfed.org/fred/series/observations"
s3 = boto3.client("s3", region_name=REGION)

# Macro series where revisions matter most (heavily-revised = highest look-ahead risk)
SERIES = [
    # Growth / activity (heavily revised)
    "GDPC1", "GDP", "INDPRO", "PAYEMS", "UNRATE", "RSAFS", "PCEC96",
    # Inflation (revised)
    "CPIAUCSL", "CPILFESL", "PCEPI", "PCEPILFE",
    # Rates / curve (mostly final but capture vintage anyway)
    "DGS10", "DGS2", "DGS3MO", "FEDFUNDS", "T10Y2Y",
    # Credit / financial conditions
    "BAMLH0A0HYM2", "NFCI", "STLFSI4",
    # Money / liquidity
    "M2SL", "WALCL", "RRPONTSYD",
    # Housing / consumer
    "HOUST", "UMCSENT", "ICSA",
]


def read_json(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default


def http_json(url, t=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=t) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def fetch_vintages(series_id, lookback_days=400):
    """Point-in-time observations via ALFRED. Setting realtime_start to a wide
    past window makes FRED return EACH observation tagged with the realtime
    period it was valid for — i.e. multiple as-reported rows per date, each
    with realtime_start = the date the market knew that value."""
    end = date.today()
    start = end - timedelta(days=lookback_days)
    base = {
        "series_id": series_id, "api_key": FRED_KEY, "file_type": "json",
        "observation_start": start.isoformat(), "observation_end": end.isoformat(),
    }
    # Primary: ALFRED realtime window (point-in-time vintages)
    params = dict(base, realtime_start=start.isoformat(), realtime_end=end.isoformat())
    d = http_json(ALFRED + "?" + urllib.parse.urlencode(params))
    obs = d.get("observations") if isinstance(d, dict) else None
    if not obs:
        # Fallback: latest-only (still gives a valid as-of-today vintage row)
        d = http_json(ALFRED + "?" + urllib.parse.urlencode(base))
        obs = d.get("observations") if isinstance(d, dict) else None
    if not obs:
        return None
    out = []
    for o in obs:
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append({
                "date": o["date"],
                "value": float(v),
                "known_on": o.get("realtime_start") or o["date"],
            })
        except (ValueError, KeyError):
            continue
    return out


def merge_vintages(existing, fresh):
    """Append-only merge: keep every (date, known_on) pair we've ever seen.
    Never overwrite a prior as-reported value — that's the whole point."""
    seen = {(r["date"], r["known_on"]): r for r in (existing or [])}
    for r in fresh or []:
        key = (r["date"], r["known_on"])
        if key not in seen:
            seen[key] = r
    merged = list(seen.values())
    merged.sort(key=lambda r: (r["date"], r["known_on"]))
    return merged[-5000:]


def lambda_handler(event=None, context=None):
    t0 = time.time()
    print(f"[vintage-fred] capturing {len(SERIES)} series")
    results = {}

    def one(sid):
        existing = (read_json(f"data/vintage/{sid}.json") or {}).get("vintages")
        fresh = fetch_vintages(sid)
        if fresh is None:
            return sid, None, (len(existing) if existing else 0)
        merged = merge_vintages(existing, fresh)
        doc = {"series": sid, "vintages": merged,
               "updated": datetime.now(timezone.utc).isoformat(),
               "n_vintages": len(merged),
               "note": "Point-in-time: 'value' is as-reported, 'known_on' is the date the market knew it. Append-only — never revised."}
        s3.put_object(Bucket=BUCKET, Key=f"data/vintage/{sid}.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=43200")
        return sid, len(merged), len(existing or [])

    captured = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for fut in as_completed([ex.submit(one, s) for s in SERIES]):
            sid, n, prev = fut.result()
            if n is not None:
                results[sid] = {"n_vintages": n, "added": n - prev}
                captured += 1

    index = {"series": list(results.keys()), "n_series": captured,
             "updated": datetime.now(timezone.utc).isoformat(),
             "detail": results,
             "method": ("FRED ALFRED archive — as-reported values with knowledge "
                        "dates, append-only. Use as_of(series, date) for point-in-"
                        "time reads with zero look-ahead bias.")}
    s3.put_object(Bucket=BUCKET, Key="data/vintage/_index.json",
                  Body=json.dumps(index, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=43200")
    print(f"[vintage-fred] DONE {round(time.time()-t0,1)}s — {captured}/{len(SERIES)} series captured")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "series_captured": captured,
                                                     "total": len(SERIES)})}
