"""
justhodl-macro-surprise — Macro Surprise Index (Citigroup CESI proxy)

When economic data prints BETTER than recent trend → growth scare relief
+ earnings revisions follow + equities rally.
When data prints WORSE → growth scare + risk-off.

Methodology:
  For each economic indicator:
    1. Pull last 24 monthly prints from FRED
    2. Compute "expected" = trailing 6-month moving average (excluding latest)
    3. Compute "actual" = latest print
    4. surprise_pct = (actual - expected) / abs(expected) * 100
    5. Z-score: surprise normalized by past surprises stdev

Aggregate index:
    Mean of z-scores across all indicators
    Positive = data beating trend (growth surprise)
    Negative = data missing trend (growth scare)

Indicators tracked (broad coverage):
  GROWTH:        Industrial Production, Retail Sales, NFP Payrolls, GDP
  INFLATION:     CPI, Core CPI, PCE, Core PCE, PPI
  EMPLOYMENT:    Unemployment Rate, Initial Claims, JOLTS
  HOUSING:       Housing Starts, New Home Sales, Existing Home Sales
  CONSUMER:      Consumer Confidence (UMich + CB), Personal Income
  LEADING:       Leading Index, ISM Manufacturing, ISM Services
  EXTERNAL:      Trade Balance, Exports

Output: data/macro-surprise.json
"""
import json
import os
import time
import boto3
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/macro-surprise.json"

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")

# FRED indicators to track + category + units interpretation
INDICATORS = {
    # GROWTH
    "INDPRO":          {"name": "Industrial Production",         "category": "GROWTH",     "higher_is_better": True},
    "RSAFS":           {"name": "Retail Sales",                  "category": "GROWTH",     "higher_is_better": True},
    "PAYEMS":          {"name": "Nonfarm Payrolls",              "category": "GROWTH",     "higher_is_better": True},
    "GDPC1":           {"name": "Real GDP",                      "category": "GROWTH",     "higher_is_better": True},
    # INFLATION (lower is better for "growth surprise" framing — but we track both)
    "CPIAUCSL":        {"name": "CPI (All Items)",               "category": "INFLATION",  "higher_is_better": False},
    "CPILFESL":        {"name": "Core CPI",                      "category": "INFLATION",  "higher_is_better": False},
    "PCEPI":           {"name": "PCE Price Index",               "category": "INFLATION",  "higher_is_better": False},
    "PCEPILFE":        {"name": "Core PCE",                      "category": "INFLATION",  "higher_is_better": False},
    "PPIACO":          {"name": "Producer Price Index",          "category": "INFLATION",  "higher_is_better": False},
    # EMPLOYMENT
    "UNRATE":          {"name": "Unemployment Rate",             "category": "EMPLOYMENT", "higher_is_better": False},
    "ICSA":            {"name": "Initial Jobless Claims",        "category": "EMPLOYMENT", "higher_is_better": False},
    "JTSJOL":          {"name": "JOLTS Job Openings",            "category": "EMPLOYMENT", "higher_is_better": True},
    # HOUSING
    "HOUST":           {"name": "Housing Starts",                "category": "HOUSING",    "higher_is_better": True},
    "HSN1F":           {"name": "New Home Sales",                "category": "HOUSING",    "higher_is_better": True},
    "EXHOSLUSM495S":   {"name": "Existing Home Sales",           "category": "HOUSING",    "higher_is_better": True},
    # CONSUMER
    "UMCSENT":         {"name": "U Michigan Consumer Sentiment", "category": "CONSUMER",   "higher_is_better": True},
    "CSUSHPISA":       {"name": "Case-Shiller Home Price",       "category": "HOUSING",    "higher_is_better": True},
    "PI":              {"name": "Personal Income",               "category": "CONSUMER",   "higher_is_better": True},
    "PCE":             {"name": "Personal Consumption Expenditures", "category": "CONSUMER", "higher_is_better": True},
    # LEADING
    "USSLIND":         {"name": "Leading Index for the US",      "category": "LEADING",    "higher_is_better": True},
    "MICH":            {"name": "Michigan Inflation Expectations", "category": "INFLATION", "higher_is_better": False},
    # EXTERNAL
    "BOPGSTB":         {"name": "Trade Balance: Goods & Services", "category": "EXTERNAL", "higher_is_better": True},
    "EXPGS":           {"name": "Exports of Goods & Services",   "category": "EXTERNAL",   "higher_is_better": True},
}


def http_get(url, timeout=15):
    req = urllib.request.Request(url, headers={
        "User-Agent": "justhodl-macro-surprise/1.0",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fetch_fred_series(series_id, n=24):
    """Pull last N monthly observations from FRED."""
    url = (
        f"https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
        f"&sort_order=desc&limit={n}"
    )
    try:
        d = http_get(url, timeout=12)
        obs = d.get("observations") or []
        # Reverse to ascending
        obs.reverse()
        # Filter out missing values
        out = []
        for o in obs:
            try:
                v = float(o["value"]) if o["value"] != "." else None
            except Exception:
                v = None
            if v is not None:
                out.append({"date": o["date"], "value": v})
        return out
    except Exception as e:
        print(f"[macro-surprise] FRED {series_id} fail: {e}")
        return []


def compute_surprise(observations, higher_is_better):
    """
    Compute surprise vs trailing 6m moving average (excluding latest print).
    Returns:
      - actual (latest)
      - expected (trailing 6m mean)
      - surprise_pct
      - z_score (surprise normalized by historical surprise stdev)
      - direction: "BEAT" / "MISS" / "INLINE"
      - score: 0-100 where 50 is neutral, 100 is extreme positive surprise
    """
    if len(observations) < 8:
        return None
    values = [o["value"] for o in observations]
    latest = values[-1]
    latest_date = observations[-1]["date"]
    # Expected = trailing 6m before latest
    expected = mean(values[-7:-1])  # 6 prints excluding latest
    if expected == 0:
        return None
    surprise_pct = (latest - expected) / abs(expected) * 100

    # Compute past surprises for z-score normalization
    past_surprises = []
    for i in range(7, len(values)):
        ex = mean(values[i-6:i])
        if ex == 0:
            continue
        sp = (values[i] - ex) / abs(ex) * 100
        past_surprises.append(sp)
    if len(past_surprises) < 4:
        return None
    s_mean = mean(past_surprises[:-1])  # exclude latest
    s_stdev = stdev(past_surprises[:-1]) if len(past_surprises) > 2 else 1
    z = round((surprise_pct - s_mean) / s_stdev, 2) if s_stdev > 0 else 0

    # Direction adjusted for inverted indicators (e.g. unemployment)
    if not higher_is_better:
        z = -z
        surprise_pct = -surprise_pct

    if z > 0.5:
        direction = "BEAT"
    elif z < -0.5:
        direction = "MISS"
    else:
        direction = "INLINE"

    # Score: map z-score to 0-100. z=0 → 50, z=2 → 90, z=-2 → 10
    score = max(0, min(100, 50 + z * 20))

    return {
        "latest_date": latest_date,
        "actual": round(latest, 4),
        "expected_6m_avg": round(expected, 4),
        "surprise_pct": round(surprise_pct, 2),
        "z_score": z,
        "direction": direction,
        "score": round(score, 1),
        "n_observations": len(values),
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[macro-surprise] start — {len(INDICATORS)} indicators")

    by_indicator = {}

    def task(series_id, meta):
        obs = fetch_fred_series(series_id, n=24)
        if not obs:
            return None
        s = compute_surprise(obs, meta["higher_is_better"])
        if s is None:
            return None
        return {
            "series_id": series_id,
            "name": meta["name"],
            "category": meta["category"],
            "higher_is_better": meta["higher_is_better"],
            **s,
        }

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(task, sid, m): sid for sid, m in INDICATORS.items()}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                by_indicator[res["series_id"]] = res

    print(f"[macro-surprise] computed: {len(by_indicator)}/{len(INDICATORS)}")

    # Aggregate by category
    by_category = {}
    for cat in set(m["category"] for m in INDICATORS.values()):
        cat_indicators = [v for v in by_indicator.values() if v["category"] == cat]
        if not cat_indicators:
            continue
        z_scores = [v["z_score"] for v in cat_indicators]
        avg_z = mean(z_scores)
        n_beat = sum(1 for v in cat_indicators if v["direction"] == "BEAT")
        n_miss = sum(1 for v in cat_indicators if v["direction"] == "MISS")
        if avg_z > 0.5:
            cat_dir = "BEATING"
        elif avg_z < -0.5:
            cat_dir = "MISSING"
        else:
            cat_dir = "INLINE"
        by_category[cat] = {
            "category": cat,
            "n_indicators": len(cat_indicators),
            "avg_z": round(avg_z, 2),
            "n_beat": n_beat,
            "n_miss": n_miss,
            "n_inline": len(cat_indicators) - n_beat - n_miss,
            "direction": cat_dir,
        }

    # Composite index: weighted average of growth + employment + housing - inflation
    growth_categories = ["GROWTH", "EMPLOYMENT", "HOUSING", "CONSUMER", "LEADING", "EXTERNAL"]
    inflation_categories = ["INFLATION"]
    growth_z = mean([by_category[c]["avg_z"] for c in growth_categories if c in by_category] or [0])
    infl_z = mean([by_category[c]["avg_z"] for c in inflation_categories if c in by_category] or [0])
    # Composite: growth surprise minus inflation surprise (good = growth up + inflation down)
    composite = growth_z - 0.3 * infl_z

    if composite > 0.5:
        regime = "GROWTH_SURPRISE_POSITIVE"
        regime_desc = "Data beating expectations — bullish for risk assets"
    elif composite < -0.5:
        regime = "GROWTH_SURPRISE_NEGATIVE"
        regime_desc = "Data missing expectations — growth scare risk"
    else:
        regime = "GROWTH_SURPRISE_INLINE"
        regime_desc = "Data inline with trend — no major surprise"

    # Top movers
    top_beats = sorted(by_indicator.values(), key=lambda x: -x["z_score"])[:5]
    top_misses = sorted(by_indicator.values(), key=lambda x: x["z_score"])[:5]

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_indicators_tracked": len(INDICATORS),
        "n_indicators_computed": len(by_indicator),
        "composite_z": round(composite, 2),
        "growth_z": round(growth_z, 2),
        "inflation_z": round(infl_z, 2),
        "regime": regime,
        "regime_description": regime_desc,
        "by_indicator": by_indicator,
        "by_category": by_category,
        "top_beats": top_beats,
        "top_misses": top_misses,
        "duration_s": round(time.time() - started, 2),
        "data_sources": {
            "indicators": "FRED API (free)",
        },
        "methodology": "Z-score of (actual − trailing 6m avg) normalized by historical surprise stdev",
        "regime_definitions": {
            "GROWTH_SURPRISE_POSITIVE": "Composite z > +0.5 — data beating, bullish risk",
            "GROWTH_SURPRISE_NEGATIVE": "Composite z < -0.5 — data missing, growth scare risk",
            "GROWTH_SURPRISE_INLINE": "Composite |z| < 0.5 — data near trend",
        },
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=3600",
    )
    print(f"[macro-surprise] composite={composite:+.2f} regime={regime}")
    print(f"[macro-surprise] wrote s3://{BUCKET}/{KEY} — {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "composite_z": round(composite, 2),
            "regime": regime,
            "n_indicators": len(by_indicator),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
