"""
justhodl-treasury-noise — TREASURY CURVE DISLOCATION & FUNDING STRESS
═════════════════════════════════════════════════════════════════════════════════
In the spirit of Hu-Pan-Wang (2013) "noise": when arbitrage capital is scarce,
the Treasury curve develops dislocations a smooth model can't fit, and funding
markets tighten. A faithful HPW measure needs daily secondary yields for hundreds
of individual CUSIPs (no free source). This engine builds the free, honest proxy:

  1. CURVE NOISE — fit a Nelson-Siegel curve (grid-τ + OLS, no scipy) to the daily
     CMT par yields (DGS1MO…DGS30) and take the RMS residual (bps). The CMT curve
     is pre-smoothed, so this is COARSER than bond-level HPW — but it still spikes
     in genuine dislocations (e.g. the Mar-2023 SVB front-end hump: 1mo 4.62 <
     3mo 4.87 > 1y 4.30). Percentile-ranked vs ~2.5y history.
  2. FUNDING STRESS — bill-SOFR spread (3M T-bill secondary − SOFR). Bills bid far
     below repo = flight-to-quality / collateral scarcity. Percentile-ranked.

Composite treasury_stress 0-100 + regime CALM/WATCH/ELEVATED/STRESSED. This is a
RISK/REGIME signal — it feeds the crisis composites/canaries, not stock-picking.

HONEST SCOPE: a documented PROXY for HPW noise (CMT-based), not the full bond-level
measure. Combined with MOVE (separate engine) it covers Treasury-market systemic stress.
"""
import json
import os
import time
import math
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/treasury-noise.json"
HIST_KEY = "data/treasury-noise-history.json"
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
S3 = boto3.client("s3", region_name=REGION)

CMT = [("DGS1MO", 1 / 12), ("DGS3MO", 0.25), ("DGS6MO", 0.5), ("DGS1", 1.0),
       ("DGS2", 2.0), ("DGS3", 3.0), ("DGS5", 5.0), ("DGS7", 7.0),
       ("DGS10", 10.0), ("DGS20", 20.0), ("DGS30", 30.0)]


def fred_series(sid, start):
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
           f"&api_key={FRED_KEY}&file_type=json&observation_start={start}&sort_order=asc")
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            obs = json.loads(r.read()).get("observations", [])
        return {o["date"]: float(o["value"]) for o in obs if o.get("value") not in (None, ".", "")}
    except Exception as e:
        print(f"[treasury-noise] FRED {sid} fail: {str(e)[:80]}")
        return {}


def ns_loadings(m, tau):
    x = m / tau
    if x < 1e-6:
        return (1.0, 1.0, 0.0)
    e = math.exp(-x)
    l1 = (1 - e) / x
    return (1.0, l1, l1 - e)


def ols3(X, y):
    """Solve 3x3 normal equations (X'X)b = X'y via Gaussian elimination."""
    n = len(y)
    A = [[0.0] * 3 for _ in range(3)]
    b = [0.0] * 3
    for i in range(n):
        xi = X[i]
        for r in range(3):
            b[r] += xi[r] * y[i]
            for c in range(3):
                A[r][c] += xi[r] * xi[c]
    # Gaussian elimination with partial pivot
    M = [A[r][:] + [b[r]] for r in range(3)]
    for col in range(3):
        piv = max(range(col, 3), key=lambda r: abs(M[r][col]))
        if abs(M[piv][col]) < 1e-12:
            return None
        M[col], M[piv] = M[piv], M[col]
        pv = M[col][col]
        M[col] = [v / pv for v in M[col]]
        for r in range(3):
            if r != col:
                f = M[r][col]
                M[r] = [M[r][k] - f * M[col][k] for k in range(4)]
    return [M[r][3] for r in range(3)]


def fit_ns_rms(maturities, yields):
    """Nelson-Siegel fit: grid-search tau, OLS betas; return RMS residual in yield pts."""
    best = None
    tau = 0.3
    while tau <= 6.0:
        X = [ns_loadings(m, tau) for m in maturities]
        beta = ols3(X, yields)
        if beta is not None:
            resid = [yields[i] - sum(beta[k] * X[i][k] for k in range(3)) for i in range(len(yields))]
            rms = math.sqrt(sum(r * r for r in resid) / len(resid))
            if best is None or rms < best:
                best = rms
        tau += 0.1
    return best


def pctile(series, val):
    s = [x for x in series if x is not None]
    if not s or val is None:
        return None
    return round(sum(1 for x in s if x <= val) / len(s) * 100, 1)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    start = (date.today() - timedelta(days=950)).isoformat()
    cmt_data = {sid: fred_series(sid, start) for sid, _ in CMT}
    dtb3 = fred_series("DTB3", start)
    sofr = fred_series("SOFR", start)

    # common dates with a full CMT curve
    dates = sorted(set.intersection(*[set(cmt_data[sid].keys()) for sid, _ in CMT])) if all(cmt_data[sid] for sid, _ in CMT) else []
    noise_series = []   # (date, rms_bps)
    for d in dates:
        ys = [cmt_data[sid][d] for sid, _ in CMT]
        mats = [m for _, m in CMT]
        rms = fit_ns_rms(mats, ys)
        if rms is not None:
            noise_series.append((d, round(rms * 100, 3)))   # bps

    if not noise_series:
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps({"engine": "justhodl-treasury-noise", "ok": False,
                                       "error": "no CMT data", "generated_at": datetime.now(timezone.utc).isoformat()}).encode(),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": False})}

    latest_date, latest_noise = noise_series[-1]
    noise_vals = [v for _, v in noise_series]
    noise_pct = pctile(noise_vals, latest_noise)
    mean_n = sum(noise_vals) / len(noise_vals)
    sd_n = math.sqrt(sum((v - mean_n) ** 2 for v in noise_vals) / len(noise_vals)) if len(noise_vals) > 1 else None
    noise_z = round((latest_noise - mean_n) / sd_n, 2) if sd_n else None

    # funding stress: bill - SOFR (bps), percentile of the NEGATIVE (more negative = more stress)
    fund_series = []
    for d in sorted(set(dtb3) & set(sofr)):
        fund_series.append((d, round((dtb3[d] - sofr[d]) * 100, 1)))
    fund_spread = fund_series[-1][1] if fund_series else None
    fund_vals = [v for _, v in fund_series]
    # stress when bill richens below SOFR → low/negative spread → low percentile
    fund_low_pct = pctile(fund_vals, fund_spread) if fund_spread is not None else None
    fund_stress_pct = round(100 - fund_low_pct, 1) if fund_low_pct is not None else None

    # composite 0-100
    parts, wts = [], []
    if noise_pct is not None:
        parts.append(noise_pct); wts.append(0.65)
    if fund_stress_pct is not None:
        parts.append(fund_stress_pct); wts.append(0.35)
    treasury_stress = round(sum(p * w for p, w in zip(parts, wts)) / sum(wts), 1) if parts else None
    if treasury_stress is None:
        regime = "n/a"
    elif treasury_stress >= 80:
        regime = "STRESSED"
    elif treasury_stress >= 60:
        regime = "ELEVATED"
    elif treasury_stress >= 40:
        regime = "WATCH"
    else:
        regime = "CALM"

    # accrue history (light; FRED is the real history but keep a trail)
    try:
        hist = json.loads(S3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        hist = []
    if not hist or hist[-1].get("date") != latest_date:
        hist.append({"date": latest_date, "noise_bps": latest_noise,
                     "noise_pct": noise_pct, "fund_spread_bps": fund_spread,
                     "treasury_stress": treasury_stress})
    hist = hist[-520:]
    try:
        S3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
    except Exception as e:
        print(f"[treasury-noise] hist write fail: {e}")

    # recent noise trail for charting (last 60 obs)
    trail = [{"date": d, "noise_bps": v} for d, v in noise_series[-60:]]
    spike_days = sorted(noise_series, key=lambda x: x[1], reverse=True)[:5]

    payload = {
        "engine": "justhodl-treasury-noise", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(), "as_of_date": latest_date,
        "thesis": ("Treasury curve dislocation (Nelson-Siegel fit residual on CMT par yields) "
                   "+ funding stress (bill−SOFR). Rising noise/funding tightness = arbitrage-"
                   "capital scarcity / Treasury-market illiquidity — a systemic-stress canary."),
        "treasury_stress": treasury_stress, "regime": regime,
        "curve_noise_bps": latest_noise, "curve_noise_pctile": noise_pct, "curve_noise_z": noise_z,
        "bill_sofr_spread_bps": fund_spread, "funding_stress_pctile": fund_stress_pct,
        "history_points": len(noise_series),
        "noise_trail_60d": trail,
        "highest_noise_days": [{"date": d, "noise_bps": v} for d, v in spike_days],
        "interpretation": {
            "curve_noise_bps": "RMS deviation of CMT yields from a smooth Nelson-Siegel fit; "
                               "higher = more curve dislocation (front-end humps, benchmark rich/cheap)",
            "bill_sofr_spread_bps": "3M T-bill minus SOFR; deeply negative = bills bid in flight-to-quality / collateral scarcity",
            "treasury_stress": "0-100 composite (65% curve-noise percentile, 35% funding-stress percentile)",
        },
        "data_source": "FRED CMT par yields (DGS1MO–DGS30) + DTB3 + SOFR; Nelson-Siegel grid-τ + OLS",
        "caveats": [
            "PROXY for Hu-Pan-Wang noise: CMT is pre-smoothed by Treasury, so this understates "
            "the bond-level measure (which needs hundreds of individual CUSIP yields — no free source). "
            "Still spikes in real dislocations (SVB Mar-2023 front-end hump).",
            "Risk/regime signal — feeds crisis composites/canaries, not stock selection.",
            "Percentiles vs ~2.5y FRED history; recomputed statelessly each run.",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[treasury-noise] stress={treasury_stress} regime={regime} noise={latest_noise}bps "
          f"pct={noise_pct} fund={fund_spread}bps hist={len(noise_series)} in {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "treasury_stress": treasury_stress, "regime": regime,
        "curve_noise_bps": latest_noise, "curve_noise_pctile": noise_pct,
        "bill_sofr_spread_bps": fund_spread, "history_points": len(noise_series)})}
