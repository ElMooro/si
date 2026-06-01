"""justhodl-eurodollar-stress

Composite USD/Eurodollar funding stress monitor. Replaces the legacy TED-spread approach
(retired with LIBOR) with 8 modern post-LIBOR signals that capture:
  - aggregate market stress (OFR FSI)
  - credit stress (HY/IG OAS)
  - equity vol (VIX)
  - dollar strength (broad DXY)
  - flight-to-safety (3M T-bill)
  - rate vol (10Y yield realized vol over 60d)
  - repo stress (SOFR vs effective Fed funds spread)
  - duration risk premium (term spread compression)

Each signal:
  - pulled from FRED (5y of daily history)
  - z-scored on 5y rolling, then percentile-ranked
  - mapped to 0-100 stress score (higher = more stress)

Composite = equal-weighted mean. Output severity bands:
  0-30   ABUNDANT      (excess liquidity, risk-on)
  30-50  CALM          (normal)
  50-70  MODERATE      (early stress)
  70-85  ELEVATED      (active stress, watch closely)
  85-100 CRITICAL      (crisis-level — exit risk)

Output: s3://justhodl-dashboard-live/data/eurodollar-stress.json
Schedule: rate(1 hour) — captures intra-day moves in fast indicators (VIX, repo).

Schema is consumed by:
  - wave-signal-logger v3 (translator log_eurodollar_stress) → DDB justhodl-signals
  - justhodl-ai-brief (snapshot.eurodollar_stress) → Claude synthesis prompt
  - brief.html snapshot tile
"""
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/eurodollar-stress.json"

S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# 8 signals — (id, fred_series, label, polarity, description)
# polarity: +1 = higher value means more stress; -1 = lower value means more stress
SIGNALS = [
    ("ofr_fsi",      "STLFSI4",         "St Louis Fed FSI",            +1, "St. Louis Fed Financial Stress Index v4 — composite of 18 weekly stress indicators"),
    ("hy_oas",       "BAMLH0A0HYM2",    "HY Credit OAS",               +1, "ICE BofA US High Yield option-adjusted spread (bps over Treasuries)"),
    ("ig_oas",       "BAMLC0A0CM",      "IG Credit OAS",               +1, "ICE BofA US Corporate (IG) option-adjusted spread (bps)"),
    ("vix",          "VIXCLS",          "VIX (Equity Vol)",            +1, "CBOE 30-day implied volatility — risk-off proxy"),
    ("broad_dollar", "DTWEXBGS",        "Broad Dollar Index",          +1, "Trade-weighted broad USD index — strong dollar = offshore funding strain"),
    ("t_bill_3m",    "DTB3",            "3M T-Bill Yield (Inv)",       -1, "Falling T-bill yield = flight to safety (inverted: low yield = high stress)"),
    ("rate_vol_10y", "DGS10",           "10Y Yield Realized Vol",      +1, "60d realized vol of 10Y yield — bond market stress"),
    ("repo_spread",  "SOFR_minus_DFF",  "SOFR – Fed Funds Spread",     +1, "Overnight repo (SOFR) vs effective Fed funds — repo dysfunction"),
]


# ---- FRED helpers ----

def get_fred_key():
    """Pull FRED key from any Lambda's env (it's broadcast across the system)."""
    for k in ["FRED_API_KEY", "FRED_KEY", "FRED_TOKEN"]:
        v = os.environ.get(k)
        if v:
            return v
    # SSM fallback
    try:
        return SSM.get_parameter(Name="/justhodl/fred/api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        # Last resort hard fallback (doc'd in user memory)
        return "2f057499936072679d8843d7fce99989"


def fred_series(series_id, start_date, key):
    """Fetch FRED series observations as list of (date_str, float) sorted asc by date."""
    qs = urllib.parse.urlencode({
        "series_id": series_id,
        "api_key": key,
        "file_type": "json",
        "observation_start": start_date,
    })
    req = urllib.request.Request(f"{FRED_BASE}?{qs}", headers={"User-Agent": "justhodl-stress/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        d = json.loads(resp.read().decode())
    out = []
    for obs in d.get("observations", []):
        v = obs.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((obs["date"], float(v)))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda x: x[0])
    return out


def realized_vol(values, window=60):
    """Annualized realized vol of daily yield-changes over window days (in vol units, %)."""
    if len(values) < window + 1:
        return None
    diffs = [values[i] - values[i-1] for i in range(1, len(values))]
    recent = diffs[-window:]
    if len(recent) < 2:
        return None
    sd = statistics.stdev(recent)
    return sd * (252 ** 0.5)  # annualize


def percentile_rank(value, history):
    """Percentile rank of value in history list (0-100)."""
    if not history:
        return None
    n_below = sum(1 for h in history if h < value)
    n_eq = sum(1 for h in history if h == value)
    return 100.0 * (n_below + 0.5 * n_eq) / len(history)


def score_signal(sig_id, fred_id, polarity, key, start_date):
    """Pull signal, compute 5y history of relevant metric, return today's score (0-100)."""
    if sig_id == "rate_vol_10y":
        # 10Y yield realized vol — pull DGS10, compute 60d rolling vol, take pctile of recent vs history
        hist = fred_series("DGS10", start_date, key)
        if len(hist) < 80:
            return None, None, None
        vols = []
        for i in range(60, len(hist)):
            window = [v for _, v in hist[i-60:i+1]]
            d = realized_vol(window, 60)
            if d is not None:
                vols.append(d)
        if len(vols) < 20:
            return None, None, None
        today_vol = vols[-1]
        history_vols = vols[:-1]
        score = percentile_rank(today_vol, history_vols)
        if polarity == -1:
            score = 100 - score
        return today_vol, score, hist[-1][0]

    if sig_id == "repo_spread":
        # SOFR - DFF (effective Fed funds rate)
        sofr = fred_series("SOFR", start_date, key)
        dff = fred_series("DFF", start_date, key)
        # Align by date — build dicts
        sofr_d = dict(sofr)
        dff_d = dict(dff)
        common_dates = sorted(set(sofr_d.keys()) & set(dff_d.keys()))
        if len(common_dates) < 30:
            return None, None, None
        spreads = [(d, sofr_d[d] - dff_d[d]) for d in common_dates]
        today_val = spreads[-1][1]
        history = [s for _, s in spreads[:-1]]
        score = percentile_rank(today_val, history)
        if polarity == -1:
            score = 100 - score
        return today_val, score, spreads[-1][0]

    # Default path: simple percentile of value vs 5y history
    obs = fred_series(fred_id, start_date, key)
    if len(obs) < 30:
        return None, None, None
    today_val = obs[-1][1]
    history = [v for _, v in obs[:-1]]
    score = percentile_rank(today_val, history)
    if polarity == -1:
        score = 100 - score
    return today_val, score, obs[-1][0]


def severity_band(composite):
    if composite is None:
        return "UNKNOWN"
    if composite >= 85:
        return "CRITICAL"
    if composite >= 70:
        return "ELEVATED"
    if composite >= 50:
        return "MODERATE"
    if composite >= 30:
        return "CALM"
    return "ABUNDANT"


def regime_call(composite):
    """Map composite to a directional regime call for downstream consumption."""
    if composite is None:
        return "UNKNOWN"
    if composite >= 70:
        return "ELEVATED_STRESS"
    if composite >= 50:
        return "MODERATE_STRESS"
    if composite >= 30:
        return "CALM"
    return "ABUNDANT_LIQUIDITY"


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[eurodollar-stress] starting at {datetime.now(timezone.utc).isoformat()}")

    key = get_fred_key()
    if not key:
        return {"statusCode": 500, "body": json.dumps({"error": "no FRED key"})}

    # 5y window for percentile + 1y warmup buffer
    start_date = (datetime.now(timezone.utc) - timedelta(days=365 * 6)).strftime("%Y-%m-%d")

    signals_out = []
    failures = []
    for sig_id, fred_id, label, polarity, desc in SIGNALS:
        try:
            t0 = time.time()
            value, score, as_of = score_signal(sig_id, fred_id, polarity, key, start_date)
            dt = round(time.time() - t0, 2)
            if value is None or score is None:
                failures.append({"signal": sig_id, "reason": "insufficient_data"})
                print(f"[eurodollar-stress] {sig_id:14s}: ✗ insufficient data ({dt}s)")
                continue
            sig = {
                "id": sig_id,
                "label": label,
                "fred_series": fred_id,
                "value": round(value, 4),
                "score_0_100": round(score, 1),
                "polarity": polarity,
                "as_of": as_of,
                "description": desc,
            }
            signals_out.append(sig)
            print(f"[eurodollar-stress] {sig_id:14s}: value={value:.4f} score={score:.1f}/100 ({dt}s)")
        except Exception as e:
            failures.append({"signal": sig_id, "reason": str(e)})
            print(f"[eurodollar-stress] {sig_id:14s}: ✗ ERROR: {e}")

    # Composite = mean of all successful signal scores
    if signals_out:
        scores = [s["score_0_100"] for s in signals_out]
        composite = round(statistics.mean(scores), 2)
    else:
        composite = None

    severity = severity_band(composite)
    regime = regime_call(composite)

    # Identify hot signals (>=70) for brief context
    hot_signals = sorted([s for s in signals_out if s["score_0_100"] >= 70],
                        key=lambda x: -x["score_0_100"])
    # Identify cold signals (<=30) for context
    cold_signals = sorted([s for s in signals_out if s["score_0_100"] <= 30],
                         key=lambda x: x["score_0_100"])

    out = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "v": "1.0",
        "composite_score": composite,
        "severity": severity,
        "regime": regime,
        "n_signals_used": len(signals_out),
        "n_signals_total": len(SIGNALS),
        "n_failures": len(failures),
        "signals": signals_out,
        "hot_signals": [{"id": s["id"], "label": s["label"], "score": s["score_0_100"], "value": s["value"]} for s in hot_signals[:5]],
        "cold_signals": [{"id": s["id"], "label": s["label"], "score": s["score_0_100"], "value": s["value"]} for s in cold_signals[:5]],
        "failures": failures,
        "thresholds": {
            "calm_max": 30,
            "moderate_min": 50,
            "elevated_min": 70,
            "critical_min": 85,
        },
        "duration_s": round(time.time() - started, 2),
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET,
        Key=OUTPUT_KEY,
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=1800",
    )
    print(f"[eurodollar-stress] composite={composite} severity={severity} regime={regime} duration={out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "composite_score": composite,
            "severity": severity,
            "regime": regime,
            "n_signals_used": len(signals_out),
            "duration_s": out["duration_s"],
        }),
    }
