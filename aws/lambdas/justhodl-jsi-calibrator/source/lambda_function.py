"""
JUSTHODL STRESS INDEX CALIBRATOR — empirical reweighting of the JSI components by how
well each has actually led equity drawdowns.

Two calibration tracks, each writing weights the JSI engine consumes:

  SPINE  — the FRED components have 36 years of history NOW. For each, we pull its full
           series since 1990, align to daily SP500, compute the forward 21-session SPY
           drawdown at each date, and fit the Spearman IC of that component's stress
           sub-score against forward drawdown across the ENTIRE 1990-2026 sample (thousands
           of paired obs spanning every crisis — a genuinely multi-regime fit, not 269
           risk-on points). Weights = max(0, IC - floor) normalised, shrunk 0.6 toward
           equal-weight priors, floored 5% / capped 40%.

  OVERLAY — the 12 live feeds only exist since 2025. We read jsi-overlay-history.json
           (written each JSI run), pair each day's feed scores with forward SPY drawdown,
           and fit IC per feed as the sample matures. Gated: <30 paired obs → equal weight.

Writes:
  SSM /justhodl/jsi/spine-weights    — calibrated spine component weights
  SSM /justhodl/jsi/overlay-weights  — calibrated overlay feed weights (once enough data)
  data/jsi-calibration.json          — full report (per-component IC, N, weights, mode)

Schedule: weekly, Sundays 09:30 UTC (after the GSI calibrator).
"""
import json
import math
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
REPORT_KEY = "data/jsi-calibration.json"
OVERLAY_HIST_KEY = "data/jsi-overlay-history.json"
SPINE_WEIGHTS_PARAM = "/justhodl/jsi/spine-weights"
OVERLAY_WEIGHTS_PARAM = "/justhodl/jsi/overlay-weights"

FRED_KEY = os.environ.get("FRED_KEY", "") or "2f057499936072679d8843d7fce99989"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
HISTORY_START = "1990-01-01"

FORWARD_DAYS = 21
IC_FLOOR = 0.05
WEIGHT_FLOOR = 0.05
WEIGHT_CAP = 0.40
SHRINKAGE = 0.6
MIN_N_BLEND = 30
MIN_N_FULL = 60

# same spine as the JSI engine (series_id, label, polarity)
SPINE = [
    ("VIXCLS", "Equity volatility (VIX)", +1),
    ("NFCI", "Chicago Fed NFCI", +1),
    ("KCFSI", "KC Fed Financial Stress", +1),
    ("STLFSI4", "St. Louis Fed Financial Stress", +1),
    ("BAMLH0A0HYM2", "High-yield credit OAS", +1),
    ("T10Y2Y", "Yield-curve (10Y-2Y)", -1),
    ("BAMLC0A0CM", "Investment-grade OAS", +1),
]

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ── math helpers (ported from GSI calibrator) ──
def rankdata(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(xs):
        j = i
        while j + 1 < len(xs) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def pearson(a, b):
    n = len(a)
    if n < 5:
        return None
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return None
    return cov / (va * vb) ** 0.5


def spearman(a, b):
    if len(a) != len(b) or len(a) < 5:
        return None
    return pearson(rankdata(a), rankdata(b))


def cap_and_floor(weights, floor=WEIGHT_FLOOR, cap=WEIGHT_CAP, max_iter=30):
    w = {k: float(v) for k, v in weights.items()}
    s0 = sum(w.values())
    if s0 <= 0:
        return w
    w = {k: v / s0 for k, v in w.items()}
    n = len(w)
    if floor * n > 1.0 or cap * n < 1.0:
        return w
    for _ in range(max_iter):
        below = {k for k, v in w.items() if v < floor - 1e-12}
        above = {k for k, v in w.items() if v > cap + 1e-12}
        if not below and not above:
            return w
        for k in below:
            w[k] = floor
        for k in above:
            w[k] = cap
        fixed_sum = floor * len(below) + cap * len(above)
        free = [k for k in w if k not in below and k not in above]
        free_target = 1.0 - fixed_sum
        free_current = sum(w[k] for k in free)
        if free_target < 0:
            return w
        if free_current > 0 and free:
            scale = free_target / free_current
            for k in free:
                w[k] *= scale
        elif free:
            for k in free:
                w[k] = free_target / len(free)
    return w


def _mean(xs): return sum(xs) / len(xs) if xs else 0.0
def _std(xs):
    if len(xs) < 2: return 0.0
    m = _mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def z_to_stress(z):
    return 100.0 / (1.0 + math.exp(-1.1 * z))


def http_json(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-jsi-cal/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fred_full(series_id):
    if not FRED_KEY:
        return []
    url = ("%s?series_id=%s&api_key=%s&file_type=json&observation_start=%s"
           % (FRED_BASE, series_id, FRED_KEY, HISTORY_START))
    try:
        d = http_json(url)
    except Exception:
        return []
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v not in (".", "", None):
            try:
                out.append((o["date"], float(v)))
            except (ValueError, KeyError):
                pass
    out.sort(key=lambda x: x[0])
    return out


def derive_empirical_weights(ic_by_key, priors, n):
    """max(0, IC-floor) normalised, shrunk toward priors by sample size, floor+cap."""
    smoothed = {k: max(0.0, (ic_by_key.get(k) or 0.0) - IC_FLOOR) for k in priors}
    total = sum(smoothed.values())
    empirical = {k: smoothed[k] / total for k in priors} if total > 0 else dict(priors)
    if n < MIN_N_BLEND:
        alpha, mode = 0.0, "insufficient"
    elif n < MIN_N_FULL:
        alpha = SHRINKAGE * (n - MIN_N_BLEND) / float(MIN_N_FULL - MIN_N_BLEND)
        mode = "blended"
    else:
        alpha, mode = SHRINKAGE, "empirical"
    shrunk = {k: alpha * empirical[k] + (1.0 - alpha) * priors[k] for k in priors}
    final = cap_and_floor(shrunk)
    fsum = sum(final.values())
    if fsum > 0:
        final = {k: final[k] / fsum for k in priors}
    return final, empirical, mode, alpha


def calibrate_spine():
    """Fit each spine component's IC vs forward 21d SPY drawdown across full 1990 history."""
    spy = fred_full("SP500")
    if len(spy) < 100:
        return None
    spy_dates = [d for d, _ in spy]
    spy_vals = [v for _, v in spy]
    spy_idx = {d: i for i, d in enumerate(spy_dates)}

    # forward 21-trading-day drawdown at each spy date
    fwd_dd = {}
    for i, d in enumerate(spy_dates):
        window = spy_vals[i + 1:i + 1 + FORWARD_DAYS]
        if len(window) < FORWARD_DAYS // 2:
            continue
        mn = min(window)
        fwd_dd[d] = max(0.0, (spy_vals[i] - mn) / spy_vals[i] * 100.0)

    ic_by_key, n_by_key = {}, {}
    for series_id, label, pol in SPINE:
        obs = fred_full(series_id)
        if len(obs) < 100:
            ic_by_key[series_id] = None
            n_by_key[series_id] = 0
            continue
        vals = [v for _, v in obs]
        mu, sd = _mean(vals), _std(vals) or 1.0
        xs, ys = [], []
        for d, v in obs:
            if d in fwd_dd:
                z = (v - mu) / sd * pol
                xs.append(z_to_stress(z))
                ys.append(fwd_dd[d])
        n_by_key[series_id] = len(xs)
        ic = spearman(xs, ys) if len(xs) >= 20 else None
        ic_by_key[series_id] = round(ic, 4) if ic is not None else None

    n = max(n_by_key.values()) if n_by_key else 0
    priors = {sid: 1.0 / len(SPINE) for sid, _, _ in SPINE}  # equal-weight priors
    final, empirical, mode, alpha = derive_empirical_weights(ic_by_key, priors, n)
    return {"ic": ic_by_key, "n_by_key": n_by_key, "weights": final,
            "empirical": empirical, "mode": mode, "alpha": round(alpha, 3),
            "labels": {sid: lbl for sid, lbl, _ in SPINE}, "sample_size": n}


def calibrate_overlay():
    """Fit each overlay feed's IC vs forward SPY drawdown from accrued snapshots."""
    try:
        hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OVERLAY_HIST_KEY)["Body"].read())
        snaps = sorted(hist.get("snapshots") or [], key=lambda r: r.get("date") or "")
    except Exception:
        return {"mode": "no_history", "sample_size": 0, "weights": {}, "ic": {}}

    paired = []
    for i, s in enumerate(snaps):
        spy = s.get("spy_close")
        if not spy:
            continue
        window = [w.get("spy_close") for w in snaps[i + 1:i + 1 + FORWARD_DAYS]
                  if isinstance(w.get("spy_close"), (int, float))]
        if len(window) < max(5, FORWARD_DAYS // 2):
            continue
        dd = max(0.0, (spy - min(window)) / spy * 100.0)
        paired.append({"feeds": s.get("feeds") or {}, "dd": dd})

    n = len(paired)
    all_feeds = set()
    for p in paired:
        all_feeds.update(p["feeds"].keys())
    ic_by_feed, n_by_feed = {}, {}
    for feed in all_feeds:
        xs, ys = [], []
        for p in paired:
            v = p["feeds"].get(feed)
            if isinstance(v, (int, float)):
                xs.append(v); ys.append(p["dd"])
        n_by_feed[feed] = len(xs)
        ic = spearman(xs, ys) if len(xs) >= 20 else None
        ic_by_feed[feed] = round(ic, 4) if ic is not None else None

    if n < MIN_N_BLEND or not all_feeds:
        return {"mode": "insufficient", "sample_size": n, "weights": {},
                "ic": ic_by_feed, "n_by_feed": n_by_feed,
                "note": f"{n} paired obs — need {MIN_N_BLEND}+ for overlay calibration"}

    priors = {f: 1.0 / len(all_feeds) for f in all_feeds}
    final, empirical, mode, alpha = derive_empirical_weights(ic_by_feed, priors, n)
    return {"mode": mode, "sample_size": n, "weights": final, "empirical": empirical,
            "ic": ic_by_feed, "n_by_feed": n_by_feed, "alpha": round(alpha, 3)}


def _put_ssm(name, obj):
    ssm.put_parameter(Name=name, Value=json.dumps(obj, default=str),
                      Type="String", Overwrite=True)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    spine = calibrate_spine()
    overlay = calibrate_overlay()

    report = {
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "spine": spine,
        "overlay": overlay,
        "methodology": {
            "spine": "Spearman IC of each FRED component's 0-100 stress sub-score vs forward 21-session SPY drawdown, fit across the full 1990-2026 sample (multi-regime). Weights = max(0, IC-0.05) normalised, shrunk 0.6 toward equal-weight, floored 5% / capped 40%.",
            "overlay": "Same fit on the accruing jsi-overlay-history snapshots; equal-weight until 30+ paired observations mature.",
        },
    }

    s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                  Body=json.dumps(report, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=600, public")

    if spine and spine.get("weights"):
        _put_ssm(SPINE_WEIGHTS_PARAM, {"weights": spine["weights"], "mode": spine["mode"],
                                       "sample_size": spine["sample_size"],
                                       "generated_at": report["generated_at"]})
    if overlay and overlay.get("weights"):
        _put_ssm(OVERLAY_WEIGHTS_PARAM, {"weights": overlay["weights"], "mode": overlay["mode"],
                                         "sample_size": overlay["sample_size"],
                                         "generated_at": report["generated_at"]})

    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "spine_mode": (spine or {}).get("mode"),
        "spine_n": (spine or {}).get("sample_size"),
        "spine_ic": (spine or {}).get("ic"),
        "spine_weights": (spine or {}).get("weights"),
        "overlay_mode": overlay.get("mode"),
        "overlay_n": overlay.get("sample_size"),
        "elapsed_s": report["elapsed_s"],
    })}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
