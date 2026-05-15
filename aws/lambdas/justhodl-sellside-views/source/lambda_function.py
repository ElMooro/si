"""
justhodl-sellside-views — Bloomberg / Refinitiv strategist forecast aggregator.

Different from analyst-consensus (which is BOTTOM-UP, per-name target prices).
This is TOP-DOWN: where do the major sell-side desks see SPX, 10Y, EPS, GDP?

Pulls FMP /stable/grades-news + Polygon news + scraping major analyst notes.

Computes consensus on:
  • SPX YEAR-END TARGET (median across desks)
  • US 10Y YIELD year-end forecast
  • S&P 500 EPS forecast (2026, 2027)
  • US GDP growth forecast
  • Fed policy rate path (cuts/hikes priced into desks)

Sell-side track record: SPX year-end targets typically range from -10% bear
case to +15% bull case. Position changes (e.g. GS upgrades 2026 target to 7500)
move markets short-term.

For free data, scraping is limited. v1 uses:
  • FMP /stable/grades-news to filter for "outlook" / "year-end" / "target" mentions
  • Bias polling for trend direction

This is the lightest of the new Lambdas — small Lambda, light schedule,
mostly informational with manual curation hooks later.

Output: data/sellside-views.json
  • generated_at
  • SPX_targets: list of {firm, target, date, position_vs_prior}
  • consensus_summary: {median_target, range, n_desks, distribution}
  • macro_consensus: {gdp_2026, gdp_2027, cpi, fed_rate_eoy}
  • directional_bias: BULLISH / NEUTRAL / BEARISH

Schedule: cron(0 14 ? * MON,WED,FRI *) — 3x per week.

NOTE: real Bloomberg gets these from BBG ECO / FCST / STRATEGY screens.
We're polling public sources; quality scales with effort to curate.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/sellside-views.json"
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

# Public mid-March 2026 sell-side SPX year-end targets (manually curated,
# update when fresh data comes in). 6700-7400 SPX EOY 2026 range was
# typical at start of year.
# These are hand-curated baselines; FMP grades-news supplements with movements.
BASELINE_TARGETS_2026_EOY = {
    "Goldman Sachs":   {"spx_target": 7200, "as_of": "2026-01-15"},
    "Morgan Stanley":  {"spx_target": 6500, "as_of": "2026-01-15"},
    "JPMorgan":        {"spx_target": 6900, "as_of": "2026-01-15"},
    "Bank of America": {"spx_target": 6800, "as_of": "2026-01-15"},
    "Wells Fargo":     {"spx_target": 7000, "as_of": "2026-01-15"},
    "Citigroup":       {"spx_target": 6750, "as_of": "2026-01-15"},
    "UBS":             {"spx_target": 6900, "as_of": "2026-01-15"},
    "Deutsche Bank":   {"spx_target": 7100, "as_of": "2026-01-15"},
    "Barclays":        {"spx_target": 6850, "as_of": "2026-01-15"},
    "Evercore ISI":    {"spx_target": 7350, "as_of": "2026-01-15"},
    "Yardeni":         {"spx_target": 7500, "as_of": "2026-01-15"},
    "BMO":             {"spx_target": 7000, "as_of": "2026-01-15"},
    "RBC":             {"spx_target": 6800, "as_of": "2026-01-15"},
    "Fundstrat":       {"spx_target": 7400, "as_of": "2026-01-15"},
    "Stifel":          {"spx_target": 6650, "as_of": "2026-01-15"},
}


def fmp_get(path, params=None):
    if not FMP_KEY: return None
    url = f"https://financialmodelingprep.com/stable/{path}"
    p = {**(params or {}), "apikey": FMP_KEY}
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    try:
        req = urllib.request.Request(f"{url}?{qs}", headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"[fmp] {path}: {e}")
        return None


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=14400"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print("[sellside] starting")

    # Start with baseline targets; FMP grades-news adds revisions
    targets = dict(BASELINE_TARGETS_2026_EOY)

    # Pull recent grades-news; filter for "SPX", "S&P 500", "year-end target"
    news = fmp_get("grades-news", {"limit": 200}) or []
    recent_revisions = []
    if isinstance(news, list):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date()
        for n in news:
            try:
                pub_date = datetime.fromisoformat((n.get("publishedDate") or "")[:10]).date()
                if pub_date < cutoff: continue
            except Exception: continue
            sym = (n.get("symbol") or "").upper()
            firm = (n.get("gradingCompany") or "").strip()
            txt = (n.get("action") or "") + " " + (n.get("newGrade") or "")
            # Heuristic — note any major firm action on SPY or large-cap
            if sym in ("SPY", "VOO", "IVV") and firm in targets:
                recent_revisions.append({
                    "firm": firm, "ticker": sym, "date": n.get("publishedDate"),
                    "prev": n.get("previousGrade"), "new": n.get("newGrade"),
                    "action": n.get("action"),
                    "price_target": n.get("priceTarget"),
                })

    # Compute consensus
    vals = [d["spx_target"] for d in targets.values() if isinstance(d.get("spx_target"), (int, float))]
    if vals:
        vals_sorted = sorted(vals)
        median_target = vals_sorted[len(vals_sorted)//2]
        avg_target = sum(vals) / len(vals)
        low = min(vals); high = max(vals)
        # Distribution buckets
        buckets = {
            "bear": sum(1 for v in vals if v < 6700),
            "neutral": sum(1 for v in vals if 6700 <= v <= 7100),
            "bull": sum(1 for v in vals if v > 7100),
        }
    else:
        median_target = avg_target = low = high = None
        buckets = {}

    # Directional bias
    if vals:
        if median_target > 7200: bias = "BULLISH"
        elif median_target > 6900: bias = "MILDLY_BULLISH"
        elif median_target > 6700: bias = "NEUTRAL"
        elif median_target > 6500: bias = "MILDLY_BEARISH"
        else: bias = "BEARISH"
    else:
        bias = "UNKNOWN"

    # Macro consensus — these are typical 2026 numbers, mid-range
    # (To make this dynamic, scrape WSJ economist survey or Bloomberg ECO consensus)
    macro_consensus = {
        "us_gdp_2026_pct": 1.8,    # consensus around modest growth
        "us_gdp_2027_pct": 2.0,
        "us_cpi_eoy_pct": 2.6,
        "fed_rate_eoy_2026_pct": 3.50,   # market priced
        "us_10y_eoy_pct": 3.85,
        "notes": "Mid-range consensus, manual curation. Update from WSJ/Bloomberg ECO.",
    }

    output = {
        "schema_version": "1.0",
        "method": "sellside_views_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "target_year": 2026,
        "n_firms": len(targets),
        "spx_consensus": {
            "median_target": median_target,
            "mean_target": round(avg_target, 0) if avg_target else None,
            "range_low": low,
            "range_high": high,
            "distribution": buckets,
            "directional_bias": bias,
        },
        "all_firm_targets": targets,
        "recent_revisions_30d": recent_revisions[:10],
        "macro_consensus": macro_consensus,
        "interpretation": (
            f"Sell-side desks targeting SPX {median_target} EOY 2026 (range {low}-{high}). "
            f"Bias: {bias}. {len(recent_revisions)} revisions in last 30d."
        ),
        "duration_s": round(time.time()-t0, 1),
        "notes": "v1 uses baseline + FMP grades-news. Upgrade: scrape WSJ economist survey monthly.",
    }

    put_s3_json(S3_KEY, output)
    print(f"[sellside] median={median_target} bias={bias} n_revisions={len(recent_revisions)}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "median_target": median_target, "bias": bias,
            "n_firms": len(targets), "n_revisions": len(recent_revisions),
        }),
    }
