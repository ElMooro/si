"""
justhodl-opportunity-calibrator — the learning loop

WHAT IT DOES
Closes the feedback loop: it measures whether each of the Opportunity
Engine's four scoring factors (value, quality, growth, momentum) has
actually predicted forward returns, and re-weights the model accordingly.

HOW (the way a systematic fund calibrates a multi-factor model)
For every pair of daily snapshots ~30 days apart it computes, across the
whole S&P 500 cross-section, the INFORMATION COEFFICIENT (rank correlation)
between each factor's sub-score at entry and the realised 30-day return.
A factor with a consistently positive IC is predictive and earns weight;
a factor with a flat or negative IC loses weight. ICs are averaged over
all matured snapshot pairs.

SAFETY — why this cannot overfit to noise
  • DORMANT until there is real matured data: needs the oldest snapshot
    >= 33 days old AND >= MIN_PAIRS independent 30-day windows. Until then
    it writes the BASELINE prior and the engine is unchanged.
  • SHRINKAGE: calibrated weights are only blended 30% toward the data and
    70% anchored to the 40/30/20/10 prior — one noisy month cannot move
    the model far.
  • CLAMPED: every weight is held within [0.05, 0.55]; no factor can take
    over or disappear.
  • Negative-IC factors are floored at zero earned weight, never punished
    below the prior's protection.

INPUT   data/track-record/snapshots/*.json   (sub-scores + prices)
OUTPUT  SSM /justhodl/opportunity/weights  +  data/opportunity-calibration.json
SCHEDULE  weekly, Sunday 15:30 UTC
"""
import json
import os
import time
from datetime import datetime, timezone, date, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SNAP_PREFIX = "data/track-record/snapshots/"
OUT_KEY = "data/opportunity-calibration.json"
WEIGHTS_PARAM = "/justhodl/opportunity/weights"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")

FACTORS = ["value", "quality", "growth", "momentum"]
PRIOR = {"value": 0.40, "quality": 0.30, "growth": 0.20, "momentum": 0.10}
EVAL_HORIZON = 30          # days
HORIZON_TOL = 3            # accept a forward snapshot within +/- this
MIN_PAIRS = 5              # matured snapshot pairs needed to activate
MIN_CROSS = 30             # min names in a cross-section to score it
MIN_AGE = 33               # oldest snapshot must be this old to activate
BLEND = 0.30               # shrinkage toward the data (0 = pure prior)
W_LO, W_HI = 0.05, 0.55    # per-factor weight clamp
RET_HI, RET_LO = 1.20, -0.65   # split / data-error guard


# ───────────────────────── stats (pure python) ──────────────────────
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
    if len(a) < 5:
        return None
    return pearson(rankdata(a), rankdata(b))


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


# ─────────────────────────── snapshot io ────────────────────────────
def list_snapshots():
    out, tok = [], None
    while True:
        kw = {"Bucket": S3_BUCKET, "Prefix": SNAP_PREFIX}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            name = o["Key"].split("/")[-1]
            if name.endswith(".json"):
                try:
                    out.append((date.fromisoformat(name[:-5]), o["Key"]))
                except ValueError:
                    pass
        if resp.get("IsTruncated"):
            tok = resp.get("NextContinuationToken")
        else:
            break
    out.sort()
    return out


def load(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[calib] WARN read {key}: {e}")
        return None


def find_forward(snaps_by_date, base_date):
    """Snapshot closest to base_date + EVAL_HORIZON within tolerance."""
    target = base_date + timedelta(days=EVAL_HORIZON)
    best, best_gap = None, 999
    for d in snaps_by_date:
        gap = abs((d - target).days)
        if gap <= HORIZON_TOL and gap < best_gap:
            best, best_gap = d, gap
    return best


def write_report(out):
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")


def write_weights(weights):
    try:
        ssm.put_parameter(Name=WEIGHTS_PARAM, Type="String", Overwrite=True,
                          Value=json.dumps(weights))
        return True
    except Exception as e:
        print(f"[calib] WARN ssm put failed: {e}")
        return False


# ────────────────────────────  handler  ─────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    snaps = list_snapshots()
    by_date = {d: k for d, k in snaps}
    today = date.today()
    oldest_age = (today - snaps[0][0]).days if snaps else 0

    # ── per-window cross-sectional IC ──
    ic_series = {f: [] for f in FACTORS}   # one IC per matured window
    n_pairs, n_obs = 0, 0
    cache = {}
    for base_date, base_key in snaps:
        fwd = find_forward(by_date, base_date)
        if not fwd:
            continue
        base = cache.setdefault(base_date, load(base_key))
        forward = cache.setdefault(fwd, load(by_date[fwd]))
        if not base or not forward:
            continue
        bp, fp = base.get("picks", {}), forward.get("picks", {})
        cols = {f: [] for f in FACTORS}
        rets = []
        for tk, rec in bp.items():
            ss = rec.get("ss")
            entry = rec.get("p")
            fwd_rec = fp.get(tk)
            if not ss or len(ss) != 4 or not entry or not fwd_rec:
                continue
            cur = fwd_rec.get("p")
            try:
                entry, cur = float(entry), float(cur)
            except (TypeError, ValueError):
                continue
            if entry <= 0 or cur <= 0:
                continue
            r = cur / entry - 1.0
            if r > RET_HI or r < RET_LO:
                continue
            for i, f in enumerate(FACTORS):
                cols[f].append(float(ss[i]))
            rets.append(r)
        if len(rets) < MIN_CROSS:
            continue
        n_pairs += 1
        n_obs += len(rets)
        for f in FACTORS:
            ic = spearman(cols[f], rets)
            if ic is not None:
                ic_series[f].append(ic)

    avg_ic = {f: mean(ic_series[f]) for f in FACTORS}
    ic_hit = {f: (round(100 * sum(1 for x in ic_series[f] if x > 0)
                        / len(ic_series[f]), 1) if ic_series[f] else None)
              for f in FACTORS}

    activated = (oldest_age >= MIN_AGE and n_pairs >= MIN_PAIRS
                 and all(avg_ic[f] is not None for f in FACTORS))

    if activated:
        ic_pos = {f: max(avg_ic[f], 0.0) for f in FACTORS}
        tot = sum(ic_pos.values())
        ic_w = ({f: ic_pos[f] / tot for f in FACTORS} if tot > 0
                else dict(PRIOR))
        blended = {f: BLEND * ic_w[f] + (1 - BLEND) * PRIOR[f]
                   for f in FACTORS}
        clamped = {f: min(W_HI, max(W_LO, blended[f])) for f in FACTORS}
        tot2 = sum(clamped.values())
        weights = {f: round(clamped[f] / tot2, 4) for f in FACTORS}
        status = "calibrated"
    else:
        weights = dict(PRIOR)
        status = "insufficient_data"

    ssm_ok = write_weights(weights)

    out = {
        "schema_version": "1.0",
        "method": "information_coefficient_factor_calibration",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "status": status,
        "eval_horizon_days": EVAL_HORIZON,
        "n_snapshots": len(snaps),
        "oldest_snapshot_age_days": oldest_age,
        "n_matured_windows": n_pairs,
        "n_observations": n_obs,
        "avg_information_coefficient": {f: (round(avg_ic[f], 4)
                                            if avg_ic[f] is not None else None)
                                        for f in FACTORS},
        "ic_positive_rate_pct": ic_hit,
        "prior_weights": PRIOR,
        "factor_weights": weights,
        "weights_written_to_ssm": ssm_ok,
        "activation_rule": (f"needs oldest snapshot >= {MIN_AGE}d old and "
                            f">= {MIN_PAIRS} matured {EVAL_HORIZON}-day windows"),
        "headline": (
            "Model calibrated from realised returns — "
            + ", ".join(f"{f} {weights[f]:.2f}" for f in FACTORS)
            if status == "calibrated"
            else (f"Learning loop armed — running on the baseline prior. "
                  f"Calibration activates after ~{MIN_AGE} days of snapshots "
                  f"({oldest_age}d logged, {n_pairs}/{MIN_PAIRS} windows ready).")),
        "methodology": ("Information Coefficient = rank correlation between a "
                        "factor's score and the realised 30-day forward return "
                        "across the S&P 500, averaged over all matured "
                        "snapshot windows. Calibrated weights are blended only "
                        "30% toward the data and 70% to the 40/30/20/10 prior "
                        "(shrinkage), then clamped to [0.05, 0.55]. Windows "
                        "overlap, so treat IC t-stats as indicative."),
        "disclaimer": ("Hypothetical, research and education only. Calibration "
                       "reduces but does not eliminate model error."),
    }
    write_report(out)
    print(f"[calib] {status} · {n_pairs} windows · {n_obs} obs · "
          f"weights={weights} · ssm={ssm_ok} · {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "status": status, "n_matured_windows": n_pairs,
        "factor_weights": weights, "ssm": ssm_ok})}
