"""
justhodl-gsi-horizons -- the multi-horizon GSI calibrator.

The canonical gsi-calibrator fits a single 21-session forward window.
This engine extends that into a TERM STRUCTURE OF STRESS: the same
six-dimension Global Stress Index calibrated at multiple horizons --
5 days (tactical), 21 days (1 month, the canonical), 63 days
(quarterly cyclical), and 252 days (annual / cycle-level).

Each horizon gets its own empirically-fit weight vector because
the same dimensions can be informative at different speeds:

  - VIX and credit spreads tend to lead at 5-21d (panic / dealer-
    margin call dynamics)
  - rate-volatility and contagion tend to lead at 63d (regime
    transitions)
  - sovereign-stress and market-matrix tend to lead at 252d (cycle
    breaks)

Publications per horizon (where N is 5, 21, 63, 252):

  - SSM /justhodl/gsi/weights/Nd  (full payload identical in shape to
    /justhodl/gsi/weights so any consumer just substitutes the path)
  - data/gsi-horizons.json (the unified term-structure report)

The canonical 21d weight at /justhodl/gsi/weights is UNCHANGED --
this engine writes only to /justhodl/gsi/weights/Nd. Global-stress
reads both: canonical for back-compat, per-horizon for the new
gsi_by_horizon block in its output.

Runs weekly Sunday 09:30 UTC, after the canonical calibrator at
09:00. Same regularization as the canonical calibrator: 5% floor,
40% cap, 60/40 empirical/prior shrinkage.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
HIST_KEY = "data/gsi-dim-history.json"
REPORT_KEY = "data/gsi-horizons.json"
WEIGHTS_PREFIX = "/justhodl/gsi/weights/"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HORIZONS = [5, 21, 63, 252]
HORIZON_LABELS = {5: "5d  tactical", 21: "21d  monthly",
                  63: "63d  quarterly", 252: "252d  cycle"}

MIN_N_BLEND = 30
MIN_N_FULL = 60
IC_FLOOR = 0.05
WEIGHT_FLOOR = 0.05
WEIGHT_CAP = 0.40
SHRINKAGE = 0.6

DIMS = ("market", "credit", "vix", "rate_vol", "contagion", "sovereign")
PRIORS = {"market": 0.32, "credit": 0.18, "vix": 0.17,
          "rate_vol": 0.13, "contagion": 0.10, "sovereign": 0.10}

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


# ============== pure-python stats helpers ===============================
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
            even = free_target / len(free)
            for k in free:
                w[k] = even
    return w


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                           "parse_mode": "HTML"}).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}),
            timeout=8).read()
    except Exception as e:
        print("telegram fail: %s" % e)


def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return {}


def write_json(key, payload, cache_seconds=600):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(payload,
                                  default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=%d" % cache_seconds)


# ============== fitting at a single horizon =============================
def fit_at_horizon(snaps, horizon_days):
    """Pair each snapshot with its forward `horizon_days` SPY drawdown,
    fit Spearman IC per dimension, derive empirical weights, smooth
    against priors with SHRINKAGE, enforce floor and cap. Returns a
    payload dict ready for SSM + the report."""
    paired = []
    for i, s in enumerate(snaps):
        spy = s.get("spy_close")
        if not spy or i + 1 >= len(snaps):
            continue
        window = snaps[i + 1:i + 1 + horizon_days]
        future_spy = [w.get("spy_close") for w in window
                      if isinstance(w.get("spy_close"), (int, float))]
        if len(future_spy) < max(5, horizon_days // 2):
            continue
        min_fwd = min(future_spy)
        drawdown = max(0.0, (spy - min_fwd) / spy * 100.0)
        paired.append({"date": s.get("date"),
                       "dims": s.get("dims") or {},
                       "drawdown_pct": drawdown})

    n = len(paired)

    # per-dimension IC
    ic_by_dim, n_by_dim = {}, {}
    for dim in DIMS:
        xs, ys = [], []
        for r in paired:
            v = r["dims"].get(dim)
            if isinstance(v, (int, float)):
                xs.append(float(v))
                ys.append(r["drawdown_pct"])
        n_by_dim[dim] = len(xs)
        ic = spearman(xs, ys) if len(xs) >= 5 else None
        ic_by_dim[dim] = (round(ic, 4) if ic is not None else None)

    # empirical from positive IC
    smoothed = {d: max(0.0, (ic_by_dim[d] or 0.0) - IC_FLOOR) for d in DIMS}
    total = sum(smoothed.values())
    if total > 0:
        empirical = {d: smoothed[d] / total for d in DIMS}
    else:
        empirical = dict(PRIORS)

    # sample-size smoothing + always shrink toward priors
    if n < MIN_N_BLEND:
        mode = "insufficient"
        alpha = 0.0
    elif n < MIN_N_FULL:
        mode = "blended"
        alpha = SHRINKAGE * (n - MIN_N_BLEND) / float(
            MIN_N_FULL - MIN_N_BLEND)
    else:
        mode = "empirical"
        alpha = SHRINKAGE

    shrunk = {d: alpha * empirical[d] + (1.0 - alpha) * PRIORS[d]
              for d in DIMS}

    final = cap_and_floor(shrunk)
    fsum = sum(final.values())
    if fsum > 0:
        final = {d: final[d] / fsum for d in DIMS}

    return {
        "weights": {d: round(final[d], 4) for d in DIMS},
        "empirical_weights": {d: round(empirical[d], 4) for d in DIMS},
        "priors": PRIORS,
        "ic": ic_by_dim,
        "n_by_dim": n_by_dim,
        "sample_size": n,
        "mode": mode,
        "forward_days": horizon_days,
        "ic_floor": IC_FLOOR,
        "weight_floor": WEIGHT_FLOOR,
        "weight_cap": WEIGHT_CAP,
        "shrinkage": SHRINKAGE,
    }


# ============== handler =================================================
def lambda_handler(event, context):
    t0 = time.time()
    calibrated_at = datetime.now(timezone.utc).isoformat()

    hist = read_json(HIST_KEY)
    snaps = sorted(hist.get("snapshots") or [],
                   key=lambda s: s.get("date") or "")

    if not snaps:
        report = {
            "as_of": calibrated_at, "horizons": HORIZONS,
            "results": {}, "snapshots_total": 0,
            "note": "dim-history empty -- run the canonical "
                    "calibrator's backfill first.",
        }
        write_json(REPORT_KEY, report)
        return {"statusCode": 200, "body": json.dumps(report)}

    # fit at each horizon
    results = {}
    for hd in HORIZONS:
        payload = fit_at_horizon(snaps, hd)
        payload["calibrated_at"] = calibrated_at
        payload["horizon_label"] = HORIZON_LABELS[hd]

        # publish per-horizon weights to SSM (only if we have enough)
        if payload["mode"] != "insufficient":
            try:
                ssm.put_parameter(Name=WEIGHTS_PREFIX + "%dd" % hd,
                                  Type="String", Overwrite=True,
                                  Value=json.dumps(payload))
            except Exception as e:
                print("ssm put fail (%dd): %s" % (hd, e))
        results[str(hd)] = payload

    # cross-horizon analysis: which dim dominates at which horizon?
    dim_dominance = {}
    for d in DIMS:
        by_h = {}
        for hd in HORIZONS:
            ic = (results[str(hd)].get("ic") or {}).get(d)
            w = (results[str(hd)].get("weights") or {}).get(d)
            by_h[str(hd)] = {"ic": ic, "weight": w}
        dim_dominance[d] = by_h

    # the term-structure summary: per-horizon top-IC dim
    term_structure = []
    for hd in HORIZONS:
        ic = results[str(hd)].get("ic") or {}
        ranked = sorted([(d, ic.get(d) or 0.0) for d in DIMS],
                        key=lambda x: -x[1])
        n = results[str(hd)].get("sample_size") or 0
        mode = results[str(hd)].get("mode")
        term_structure.append({
            "horizon": hd,
            "label": HORIZON_LABELS[hd],
            "top_dim": ranked[0][0] if ranked[0][1] > 0 else None,
            "top_ic": round(ranked[0][1], 4) if ranked[0][1] > 0 else None,
            "n": n, "mode": mode,
            "ic_max": round(ranked[0][1], 4),
            "rank": [r[0] for r in ranked],
        })

    report = {
        "as_of": calibrated_at,
        "horizons": HORIZONS,
        "horizon_labels": HORIZON_LABELS,
        "snapshots_total": len(snaps),
        "earliest_snapshot": snaps[0].get("date") if snaps else None,
        "latest_snapshot": snaps[-1].get("date") if snaps else None,
        "results": results,
        "dim_dominance": dim_dominance,
        "term_structure": term_structure,
        "duration_s": round(time.time() - t0, 1),
        "methodology": (
            "For each forward horizon in %s, the engine pairs every "
            "snapshot's dim values with the corresponding forward SPY "
            "drawdown over that horizon and fits Spearman IC per "
            "dimension. Empirical weights = max(0, IC - %.2f) "
            "normalised; smoothed with priors via %.0f%% shrinkage; "
            "constrained to per-dim floor %.0f%% and cap %.0f%%. Same "
            "regularization as the canonical 21d calibrator -- the "
            "horizons are independently fit but bound by the same "
            "institutional priors so the term structure is comparable."
            % (HORIZONS, IC_FLOOR, (1 - SHRINKAGE) * 100,
               WEIGHT_FLOOR * 100, WEIGHT_CAP * 100)),
    }
    write_json(REPORT_KEY, report)

    # alert on big horizon disagreements (different top-dim at 21d vs 252d
    # tends to mean the cycle and the tactical layer are saying opposite
    # things -- worth noting)
    h21 = next((r for r in term_structure if r["horizon"] == 21), None)
    h252 = next((r for r in term_structure if r["horizon"] == 252), None)
    if (h21 and h252 and h21["top_dim"] and h252["top_dim"]
            and h21["top_dim"] != h252["top_dim"]):
        send_telegram(
            "\U0001F4CA <b>GSI horizons</b> -- tactical vs cycle "
            "divergence: 21d top dim <b>%s</b> (IC %s), 252d top dim "
            "<b>%s</b> (IC %s). Tactical and cyclical risk drivers "
            "are different." % (
                h21["top_dim"], h21["top_ic"],
                h252["top_dim"], h252["top_ic"]))

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "horizons_fit": [h["horizon"] for h in term_structure],
        "snapshots": len(snaps),
        "modes": {str(r["horizon"]): r["mode"] for r in term_structure},
        "elapsed_s": round(time.time() - t0, 1)})}
