"""
justhodl-episode-reference — Historical Episode Benchmark Engine
=================================================================
The shared "you are here vs history" layer. For every key macro indicator,
this computes its actual value at the canonical market TOPS, BOTTOMS, and
CRISES — so any page can show "2s10s is at X now; that's where it sat at the
2007 top, not the 2009 bottom."

This is the institutional context retail never gets: a single number is
meaningless without knowing whether that level historically marked euphoria,
capitulation, or systemic stress.

METHODOLOGY
  1. 13 canonical episodes with precise dates (SPX tops/bottoms + crises)
  2. For ~16 key FRED series, pull full available history (1990+)
  3. For each episode, extract the indicator value at the nearest trading day
  4. Compute current value + percentile rank over full history + min/max/median
  5. Classify which episode the CURRENT reading most resembles (nearest value)

OUTPUT: data/episode-reference.json
  {
    "episodes": [ {id,name,date,type,spx_context}, ... ],
    "indicators": {
      "T10Y2Y": {
        "label": "2s10s Curve Slope",
        "current": 0.52, "current_date": "...", "unit": "%",
        "percentile": 58, "min": -2.41, "max": 2.91, "median": 0.98,
        "at_episodes": { "top_2007": -0.19, "bottom_2009": 2.75, ... },
        "nearest_episode": {"id":"...", "name":"...", "type":"...", "value":..., "distance":...}
      }, ...
    }
  }

SCHEDULE: daily 10:00 UTC (episodes static; current values + percentile shift daily)
"""
import json
import os
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from statistics import median as _median
import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/episode-reference.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")

# ── 13 canonical market episodes ──────────────────────────────────────
# Dates are the actual SPX peak/trough or crisis flashpoint.
EPISODES = [
    {"id": "top_2000",    "name": "Dotcom Peak",        "date": "2000-03-24", "type": "TOP",    "spx_context": "S&P 500 cycle high before the dotcom collapse (-49%)"},
    {"id": "bottom_2002", "name": "Dotcom Bottom",      "date": "2002-10-09", "type": "BOTTOM", "spx_context": "Bear-market low after the tech bust"},
    {"id": "top_2007",    "name": "GFC Peak",           "date": "2007-10-09", "type": "TOP",    "spx_context": "Pre-financial-crisis cycle high before -57%"},
    {"id": "crisis_2008", "name": "Lehman",             "date": "2008-09-15", "type": "CRISIS", "spx_context": "Lehman bankruptcy — peak systemic panic"},
    {"id": "bottom_2009", "name": "GFC Bottom",         "date": "2009-03-09", "type": "BOTTOM", "spx_context": "Generational bottom — the 'devil's low' at 666"},
    {"id": "crisis_2011", "name": "EU Debt Crisis",     "date": "2011-10-03", "type": "CRISIS", "spx_context": "Eurozone sovereign crisis / US downgrade trough"},
    {"id": "crisis_2015", "name": "China Devaluation",  "date": "2015-08-24", "type": "CRISIS", "spx_context": "Yuan shock 'Black Monday' flash event"},
    {"id": "bottom_2016", "name": "Feb 2016 Low",       "date": "2016-02-11", "type": "BOTTOM", "spx_context": "Oil/credit scare low before recovery"},
    {"id": "crisis_2018", "name": "Q4 2018 Selloff",    "date": "2018-12-24", "type": "CRISIS", "spx_context": "Powell hiking-cycle -20% Christmas Eve low"},
    {"id": "crisis_2020", "name": "COVID Crash",        "date": "2020-03-23", "type": "BOTTOM", "spx_context": "Pandemic crash bottom (-34% in 33 days)"},
    {"id": "top_2021",    "name": "Everything Bubble",  "date": "2022-01-03", "type": "TOP",    "spx_context": "Post-COVID liquidity cycle high before -25%"},
    {"id": "bottom_2022", "name": "2022 Bear Bottom",   "date": "2022-10-12", "type": "BOTTOM", "spx_context": "Inflation/hiking bear-market low"},
    {"id": "crisis_2023", "name": "SVB / Regional Banks","date": "2023-03-10", "type": "CRISIS", "spx_context": "Silicon Valley Bank failure / banking stress"},
]

# ── Key FRED indicators (the ones macro/stress pages display) ──────────
# (series_id, output_id, label, unit, transform)
INDICATORS = [
    ("T10Y2Y",       "T10Y2Y",       "2s10s Curve Slope",        "%",   None),
    ("T10Y3M",       "T10Y3M",       "10Y-3M Curve Slope",       "%",   None),
    ("VIXCLS",       "VIXCLS",       "VIX (Equity Vol)",         "",    None),
    ("BAMLH0A0HYM2", "BAMLH0A0HYM2", "HY Credit Spread (OAS)",   "%",   None),
    ("BAMLC0A0CM",   "BAMLC0A0CM",   "IG Credit Spread (OAS)",   "%",   None),
    ("DGS10",        "DGS10",        "10Y Treasury Yield",       "%",   None),
    ("DGS2",         "DGS2",         "2Y Treasury Yield",        "%",   None),
    ("DFII10",       "DFII10",       "10Y Real Yield (TIPS)",    "%",   None),
    ("T10YIE",       "T10YIE",       "10Y Breakeven Inflation",  "%",   None),
    ("DTWEXBGS",     "DTWEXBGS",     "Broad Dollar Index",       "",    None),
    ("STLFSI4",      "STLFSI4",      "St Louis Fed Stress Index","",    None),
    ("NFCI",         "NFCI",         "Chicago Fed Financial Cond","",   None),
    ("DTB3",         "DTB3",         "3M T-Bill Yield",          "%",   None),
    ("DFF",          "DFF",          "Fed Funds Rate",           "%",   None),
    ("BAMLH0A0HYM2EY","BAMLH0A0HYM2EY","HY Effective Yield",     "%",   None),
    ("DEXUSEU",      "DEXUSEU",      "USD/EUR",                  "",    None),
]


def fred_history(series_id, start="1990-01-01"):
    """Pull full daily history; return sorted list of (date_str, float)."""
    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_KEY, "file_type": "json",
        "observation_start": start,
    })
    url = f"https://api.stlouisfed.org/fred/series/observations?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/EpisodeRef"})
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read())
        out = []
        for o in data.get("observations", []):
            v = o.get("value")
            if v not in (".", "", None):
                try:
                    out.append((o["date"], float(v)))
                except ValueError:
                    pass
        return out
    except Exception as e:
        print(f"[episode-ref] {series_id} fetch err: {e}")
        return []


def value_at(hist, target_date, window_days=10):
    """Nearest observation to target_date within window."""
    if not hist:
        return None
    tgt = datetime.strptime(target_date, "%Y-%m-%d").date()
    best = None
    best_dist = 1e9
    for ds, v in hist:
        d = datetime.strptime(ds, "%Y-%m-%d").date()
        dist = abs((d - tgt).days)
        if dist < best_dist:
            best_dist = dist
            best = v
        if d > tgt and dist > window_days and best is not None:
            break
    return best if best_dist <= window_days else (best if best_dist <= 35 else None)


# NBER US recession periods (for chart shading)
RECESSIONS = [
    {"start": "1990-07", "end": "1991-03", "name": "Early-90s"},
    {"start": "2001-03", "end": "2001-11", "name": "Dotcom"},
    {"start": "2007-12", "end": "2009-06", "name": "GFC"},
    {"start": "2020-02", "end": "2020-04", "name": "COVID"},
]


def monthly_downsample(hist):
    """Reduce daily history to last-observation-per-month: [[YYYY-MM, value], ...]."""
    by_month = {}
    for ds, v in hist:
        by_month[ds[:7]] = v  # sorted asc → last obs of month wins
    return [[ym, round(v, 4)] for ym, v in sorted(by_month.items())]


def percentile_rank(value, series):
    if value is None or not series:
        return None
    below = sum(1 for v in series if v <= value)
    return round(below / len(series) * 100, 1)


def process_indicator(spec):
    series_id, out_id, label, unit, transform = spec
    hist = fred_history(series_id)
    if not hist:
        return out_id, None
    values = [v for _, v in hist]
    current = hist[-1][1]
    current_date = hist[-1][0]

    at_episodes = {}
    for ep in EPISODES:
        v = value_at(hist, ep["date"])
        if v is not None:
            at_episodes[ep["id"]] = round(v, 4)

    # Nearest episode by absolute value distance
    nearest = None
    if at_episodes:
        nid, nv = min(at_episodes.items(), key=lambda kv: abs(kv[1] - current))
        ep_meta = next((e for e in EPISODES if e["id"] == nid), {})
        nearest = {
            "id": nid, "name": ep_meta.get("name"), "type": ep_meta.get("type"),
            "value": nv, "distance": round(abs(nv - current), 4),
        }

    return out_id, {
        "label": label,
        "unit": unit,
        "current": round(current, 4),
        "current_date": current_date,
        "percentile": percentile_rank(current, values),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "median": round(_median(values), 4),
        "at_episodes": at_episodes,
        "nearest_episode": nearest,
        "history_start": hist[0][0],
        "n_obs": len(values),
        "monthly_history": monthly_downsample(hist),
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[episode-ref] start {datetime.now(timezone.utc).isoformat()}")

    indicators = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(process_indicator, spec): spec for spec in INDICATORS}
        for fut in as_completed(futures):
            out_id, result = fut.result()
            if result:
                indicators[out_id] = result
                ne = result.get("nearest_episode") or {}
                print(f"[episode-ref] {out_id:16s} cur={result['current']} pct={result['percentile']} nearest={ne.get('name')}")

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "description": "Each indicator's value at canonical market tops, bottoms, and crises, plus current value + full-history percentile. The 'you are here vs history' reference layer.",
        "episodes": EPISODES,
        "recessions": RECESSIONS,
        "indicators": indicators,
        "n_indicators": len(indicators),
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=KEY, Body=body,
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[episode-ref] wrote s3://{BUCKET}/{KEY} — {len(body):,}b · {len(indicators)} indicators · {out['duration_s']}s")

    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_indicators": len(indicators)})}
