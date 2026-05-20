"""
justhodl-gsi-calibrator -- empirical recalibration of the Global Stress
Index dimension weights.

Reads data/gsi-dim-history.json (per-dimension snapshots written by the
global-stress engine and the one-shot backfill), pairs each snapshot
with its forward 21-session SPY drawdown (the worst close-to-close
drawdown over the next 21 sessions), and fits the IC (Spearman rank
correlation) of each dimension's score against that forward outcome.

A dimension that genuinely leads equity drawdowns will have a positive
Spearman IC: higher current stress -> deeper forward drawdown. Negative
or near-zero IC is a dimension that did NOT lead. Weights are derived
as max(0, IC - floor) normalised, with a sample-size smoothing rule:

    N <  30:  priors only, SSM not updated, mode = "insufficient"
    30 <= N < 60: linear blend prior -> empirical, mode = "blended"
    N >= 60:  fully empirical, mode = "empirical"

Writes calibrated weights to SSM /justhodl/gsi/weights, the full
calibration report (per-dimension IC + sample paths) to
data/gsi-calibration.json, and prints a Telegram update on regime
flips (priors -> blended -> empirical, or significant weight shifts).

Schedule: weekly Sunday 09:00 UTC.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
DIM_HIST_KEY = "data/gsi-dim-history.json"
REPORT_KEY = "data/gsi-calibration.json"
WEIGHTS_PARAM = "/justhodl/gsi/weights"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

FORWARD_DAYS = 21          # forward window: ~1 trading month
MIN_N_BLEND = 30           # below this, fall back entirely to priors
MIN_N_FULL = 60            # above this, weight purely on empirical IC
IC_FLOOR = 0.05            # absolute IC below this is treated as noise
WEIGHT_FLOOR = 0.05        # every dimension gets at least 5% weight --
                           # preserves cross-dimension diversification
                           # even when one dim dominates the IC ranking
WEIGHT_CAP = 0.40          # no single dimension exceeds 40% -- prevents
                           # the index from collapsing into a single
                           # predictor when one IC outscores the rest
SHRINKAGE = 0.6            # at full-empirical mode, final weight is
                           # 0.6 * empirical + 0.4 * prior -- priors
                           # carry institutional knowledge that limited
                           # sample-period IC alone cannot replace
DIMS = ("market", "credit", "vix", "rate_vol", "contagion", "sovereign")
PRIORS = {"market": 0.32, "credit": 0.18, "vix": 0.17,
          "rate_vol": 0.13, "contagion": 0.10, "sovereign": 0.10}

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


#                        pure-python stats helpers                       
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
    """Project a weight vector onto the simplex constrained by per-
    dimension floor and cap. Iteratively normalises, clips any
    violations, and redistributes the displaced mass across the
    unconstrained dimensions; converges in 1-3 iterations in practice."""
    w = {k: float(v) for k, v in weights.items()}
    s0 = sum(w.values())
    if s0 <= 0:
        return w
    w = {k: v / s0 for k, v in w.items()}
    n = len(w)
    if floor * n > 1.0 or cap * n < 1.0:
        return w   # constraints infeasible -- pass through
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
            return w   # infeasible -- bail
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
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type":
                                              "application/json"})
        urllib.request.urlopen(req, timeout=8).read()
    except Exception as e:
        print("telegram fail: %s" % e)


def load_snapshots():
    try:
        d = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                     Key=DIM_HIST_KEY)["Body"].read())
        return sorted(d.get("snapshots") or [],
                      key=lambda s: s.get("date") or "")
    except Exception as e:
        print("load snapshots fail: %s" % e)
        return []


def load_prior_payload():
    """The previously-published calibration -- used to detect mode flips
    and significant weight shifts for the Telegram alert."""
    try:
        p = ssm.get_parameter(Name=WEIGHTS_PARAM)
        return json.loads(p["Parameter"]["Value"])
    except Exception:
        return None


#                              handler                             
def lambda_handler(event, context):
    t0 = time.time()
    snaps = load_snapshots()
    if len(snaps) < 5:
        # nothing useful yet
        report = {
            "as_of": datetime.now(timezone.utc).isoformat(),
            "mode": "no_history", "sample_size": 0,
            "snapshots_total": len(snaps),
            "weights": PRIORS, "priors": PRIORS,
            "note": "global-stress dim-history is empty -- run the "
                    "backfill, or wait for the engine to accumulate "
                    "forward-going snapshots.",
        }
        s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                      Body=json.dumps(report,
                                      default=str).encode("utf-8"),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps(report)}

    # ---- pair each snapshot with its forward 21-session SPY drawdown ----
    # Snapshots are sorted oldest -> newest by date. For each snapshot at
    # index i with spy_close c, walk forward through the next FORWARD_DAYS
    # entries, take min(spy_close), and compute drawdown.
    paired = []
    for i, s in enumerate(snaps):
        spy = s.get("spy_close")
        if not spy or i + 1 >= len(snaps):
            continue
        window = snaps[i + 1:i + 1 + FORWARD_DAYS]
        future_spy = [w.get("spy_close") for w in window
                      if isinstance(w.get("spy_close"), (int, float))]
        if len(future_spy) < max(5, FORWARD_DAYS // 2):
            # not enough forward window matured yet -- skip
            continue
        min_fwd = min(future_spy)
        drawdown = max(0.0, (spy - min_fwd) / spy * 100.0)
        paired.append({"date": s.get("date"), "dims": s.get("dims") or {},
                       "drawdown_21d_pct": drawdown,
                       "fwd_window_size": len(future_spy)})

    n = len(paired)
    # ---- per-dimension Spearman IC -------------------------------------
    ic_by_dim, n_by_dim = {}, {}
    for dim in DIMS:
        xs, ys = [], []
        for r in paired:
            v = r["dims"].get(dim)
            if isinstance(v, (int, float)):
                xs.append(float(v))
                ys.append(r["drawdown_21d_pct"])
        n_by_dim[dim] = len(xs)
        ic = spearman(xs, ys) if len(xs) >= 5 else None
        ic_by_dim[dim] = (round(ic, 4) if ic is not None else None)

    # ---- derive empirical weights from IC ------------------------------
    smoothed = {d: max(0.0, (ic_by_dim[d] or 0.0) - IC_FLOOR) for d in DIMS}
    total = sum(smoothed.values())
    if total > 0:
        empirical = {d: smoothed[d] / total for d in DIMS}
    else:
        empirical = dict(PRIORS)   # no positive IC -> stick with priors

    # ---- sample-size smoothing + always shrink toward priors -----------
    # Even at "empirical" mode (N >= MIN_N_FULL), the final weight is a
    # SHRINKAGE blend of empirical IC and the priors -- not a full
    # replacement. Priors encode dimension-level institutional knowledge
    # that limited-sample IC alone cannot replace, and shrinkage protects
    # against the index collapsing into one dominant predictor when a
    # single sample window happens to favour it.
    if n < MIN_N_BLEND:
        mode = "insufficient"
        alpha = 0.0
    elif n < MIN_N_FULL:
        mode = "blended"
        # ramp empirical content from 0 at N=MIN_N_BLEND up to SHRINKAGE
        # at N=MIN_N_FULL, then hold at SHRINKAGE for "empirical"
        alpha = SHRINKAGE * (n - MIN_N_BLEND) / float(
            MIN_N_FULL - MIN_N_BLEND)
    else:
        mode = "empirical"
        alpha = SHRINKAGE

    shrunk = {d: alpha * empirical[d] + (1.0 - alpha) * PRIORS[d]
              for d in DIMS}

    # ---- enforce per-dimension floor and cap ---------------------------
    # No dimension drops below WEIGHT_FLOOR (preserves diversification)
    # or exceeds WEIGHT_CAP (prevents any single dimension from
    # dominating the index); displaced mass redistributes across the
    # unconstrained dimensions.
    final = cap_and_floor(shrunk, floor=WEIGHT_FLOOR, cap=WEIGHT_CAP)

    # defensive renormalisation against floating-point drift
    fsum = sum(final.values())
    if fsum > 0:
        final = {d: final[d] / fsum for d in DIMS}

    # ---- payload + write -----------------------------------------------
    calibrated_at = datetime.now(timezone.utc).isoformat()
    payload = {
        "weights": {d: round(final[d], 4) for d in DIMS},
        "empirical_weights": {d: round(empirical[d], 4) for d in DIMS},
        "priors": PRIORS,
        "ic": ic_by_dim,
        "n_by_dim": n_by_dim,
        "sample_size": n,
        "mode": mode,
        "calibrated_at": calibrated_at,
        "forward_days": FORWARD_DAYS,
        "ic_floor": IC_FLOOR,
        "weight_floor": WEIGHT_FLOOR,
        "weight_cap": WEIGHT_CAP,
        "shrinkage": SHRINKAGE,
        "min_n_blend": MIN_N_BLEND,
        "min_n_full": MIN_N_FULL,
    }
    full_report = dict(payload)
    full_report.update({
        "as_of": calibrated_at,
        "snapshots_total": len(snaps),
        "earliest_snapshot": snaps[0].get("date") if snaps else None,
        "latest_snapshot": snaps[-1].get("date") if snaps else None,
        "paired_observations": [
            {"date": p["date"], "drawdown_21d_pct": round(
                p["drawdown_21d_pct"], 2),
             "dims": {k: v for k, v in p["dims"].items() if k in DIMS}}
            for p in paired[-100:]   # last 100 for the dashboard
        ],
        "methodology": (
            "Spearman rank IC between each dimension's stress score "
            "and the 21-session forward SPY drawdown. Empirical weights "
            "= max(0, IC - %.2f) normalised. Sample-size guard: priors "
            "for N<%d, linear blend prior->empirical for %d<=N<%d, full "
            "empirical for N>=%d." % (IC_FLOOR, MIN_N_BLEND, MIN_N_BLEND,
                                       MIN_N_FULL, MIN_N_FULL)),
        "duration_s": round(time.time() - t0, 1),
    })

    s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                  Body=json.dumps(full_report,
                                  default=str).encode("utf-8"),
                  ContentType="application/json")

    # ---- publish weights to SSM if we actually have enough data --------
    if mode != "insufficient":
        try:
            ssm.put_parameter(Name=WEIGHTS_PARAM, Type="String",
                              Overwrite=True,
                              Value=json.dumps(payload))
        except Exception as e:
            print("ssm put fail: %s" % e)

    # ---- alert on mode flips and big weight shifts ---------------------
    prior_payload = load_prior_payload() or {}
    prior_mode = prior_payload.get("mode")
    if mode != "insufficient" and prior_mode != mode:
        send_telegram(
            "\U0001F4CA <b>GSI calibrator</b> -- mode flip <b>%s \u2192 "
            "%s</b> on N=%d paired obs. Top IC: %s." % (
                prior_mode or "uncalibrated", mode, n,
                ", ".join(sorted([(d, ic_by_dim[d] or 0.0) for d in DIMS],
                                  key=lambda x: -(x[1] or 0))[0:1] and
                          ["%s %.2f" % (d, ic_by_dim[d])
                           for d in sorted(DIMS,
                                            key=lambda k:
                                            -(ic_by_dim[k] or 0))[:3]
                           if ic_by_dim[d] is not None])))

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "mode": mode, "sample_size": n,
        "ic": ic_by_dim, "weights": payload["weights"],
        "elapsed_s": round(time.time() - t0, 1)})}
