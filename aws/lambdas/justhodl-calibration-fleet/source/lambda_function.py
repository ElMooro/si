"""
justhodl-calibration-fleet -- the universal IC-calibration loop.

For every signal engine in the registry, this fleet calibrator pairs
the engine's score time-series with the 21-session forward SPY
drawdown and computes the Spearman rank Information Coefficient
(IC). It is the time-series quality check across the whole signal
fleet: which engines genuinely predict equity drawdowns, which are
noise, and which are contrarian.

Hedge-fund-grade outputs per engine:
  - ic_spearman      : the rank correlation against forward DD
  - n_paired         : sample size of paired observations
  - hit_rate         : % of times the engine top-quartile state
                       coincided with above-median forward drawdown
  - ic_split_half    : IC on the two halves of the sample -- a
                       stability check; large divergence flags
                       regime-fragile signals
  - quality_rating   : PREDICTIVE / NOISE / CONTRARIAN / INSUFFICIENT
  - weight_proposal  : max(0, IC - floor) normalised across engines,
                       what a downstream synthesizer should use

Runs weekly Sunday 09:00 UTC and on demand. Publishes:
  - data/calibration-fleet.json (the full report);
  - SSM /justhodl/calibration-fleet/weights (machine-readable map for
    synthesizers like signal-board, crisis-composite, alpha-confluence
    to consume).

Maintains its own daily-keyed history in
data/calibration-fleet-history.json. SPY closes come from
data/gsi-dim-history.json so every engine fits the same target.

The registry is config-driven: each entry declares the engine name,
the S3 key of its published JSON, and the path to its score within
that JSON. Adding an engine here makes it part of the next run.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = "justhodl-dashboard-live"
HIST_KEY = "data/calibration-fleet-history.json"
REPORT_KEY = "data/calibration-fleet.json"
GSI_HIST_KEY = "data/gsi-dim-history.json"
WEIGHTS_PARAM = "/justhodl/calibration-fleet/weights"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

FORWARD_DAYS = 21
MIN_N = 30          # absolute minimum to report IC at all
MIN_N_STABLE = 60   # below this, mark quality with a "thin sample" caveat
IC_FLOOR = 0.05
HIST_BARS = 400

# Registry: (name, source_key, score_path, direction).
# direction = "stress" -> high score predicts more drawdown (positive IC
# is good); "risk_on" -> high score predicts LESS drawdown (negative IC
# is good and gets flipped). Most engines are stress-direction.
REGISTRY = [
    {"name": "global_stress",      "source_key": "data/global-stress.json",
     "score_path": ["global_stress_index"], "direction": "stress",
     "label": "Global Stress Matrix"},
    {"name": "crisis_composite",   "source_key": "data/crisis-composite.json",
     "score_path": ["master_crisis_score"], "direction": "stress",
     "label": "Crisis Composite"},
    {"name": "dollar_radar",       "source_key": "data/dollar-radar.json",
     "score_path": ["dollar_pressure"], "direction": "stress",
     "label": "Dollar Radar"},
    {"name": "vol_radar",          "source_key": "data/vol-radar.json",
     "score_path": ["composite_score"], "direction": "stress",
     "label": "Vol Radar"},
    {"name": "eurodollar_stress",  "source_key": "data/eurodollar-stress.json",
     "score_path": ["composite_score"], "direction": "stress",
     "label": "Eurodollar Stress"},
    {"name": "market_extremes",    "source_key": "data/market-extremes.json",
     "score_path": ["score"], "direction": "stress",
     "label": "Market Extremes"},
    {"name": "signal_board",       "source_key": "data/signal-board.json",
     "score_path": ["composite"], "direction": "stress",
     "label": "Signal Board"},
    {"name": "systemic_stress",    "source_key": "data/systemic-stress.json",
     "score_path": ["score"], "direction": "stress",
     "label": "Systemic Stress (CISS)"},
    {"name": "vrp",                "source_key": "data/vrp.json",
     "score_path": ["score"], "direction": "stress",
     "label": "Variance Risk Premium"},
    {"name": "credit_stress",      "source_key": "data/credit-stress.json",
     "score_path": ["composite_score"], "direction": "stress",
     "label": "Credit Stress"},
]

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
ddb_client = boto3.client("dynamodb")

# DynamoDB time-series store maintained by justhodl-history-snapshotter.
# Schema: pk='feed#<output_key>', sk='<ISO timestamp>', payload=<JSON
# blob of the entire engine output at that time>. Gives us historical
# composite scores back to whenever the snapshotter first picked the
# feed up -- typically many months of dense (~5-minute) data.
HISTORY_TABLE = os.environ.get("HISTORY_TABLE", "justhodl-history")


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


def quantile(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    i = q * (len(xs) - 1)
    lo, hi = int(i), min(int(i) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (i - lo)


# ============== I/O helpers ==============================================
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


def deep_get(obj, path):
    """Navigate a nested JSON by a list of keys; returns None if any
    segment is missing or the leaf is not numeric."""
    cur = obj
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur if isinstance(cur, (int, float)) else None


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


# ============== handler ==================================================
def ddb_history_snapshots(output_key, score_path):
    """Pull every snapshot of one feed from the history-snapshotter
    DynamoDB store. Returns a list of (iso_date, score) pairs, oldest
    first, with one entry per session (the LATEST intraday snapshot per
    day, since intra-day variation is noise for daily IC calibration).
    Empty list on any error -- the calibrator continues with whatever
    history it does have."""
    pk = "feed#" + output_key
    by_date = {}
    try:
        last = None
        while True:
            kwargs = {"TableName": HISTORY_TABLE,
                      "KeyConditionExpression": "pk = :pk",
                      "ExpressionAttributeValues": {":pk": {"S": pk}}}
            if last:
                kwargs["ExclusiveStartKey"] = last
            resp = ddb_client.query(**kwargs)
            for it in resp.get("Items", []):
                sk = it.get("sk", {}).get("S", "")
                if not sk:
                    continue
                # payload may be stored as a Map or as a JSON string
                payload = None
                if "payload" in it:
                    p = it["payload"]
                    if "S" in p:
                        try:
                            payload = json.loads(p["S"])
                        except Exception:
                            continue
                    elif "M" in p:
                        # the AWS-SDK marshaled form -- unmarshal lazily;
                        # we only need a single field
                        payload = _unmarshal_dynamo(p)
                if not isinstance(payload, dict):
                    continue
                score = deep_get(payload, score_path)
                if not isinstance(score, (int, float)):
                    continue
                day = sk[:10]
                # keep the LATEST intraday snapshot per day
                by_date[day] = (sk, float(score))
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
    except Exception as e:
        print("[fleet] ddb query err %s: %s" % (output_key, e))
        return []
    return sorted(((d, v[1]) for d, v in by_date.items()),
                  key=lambda x: x[0])


def _unmarshal_dynamo(av):
    """Minimal DynamoDB AttributeValue -> Python recursion. Handles
    string, number, dict, list, boolean, null."""
    if not isinstance(av, dict):
        return av
    if "S" in av:
        return av["S"]
    if "N" in av:
        try:
            n = av["N"]
            return float(n) if "." in n or "e" in n.lower() else int(n)
        except Exception:
            return None
    if "BOOL" in av:
        return av["BOOL"]
    if "NULL" in av:
        return None
    if "M" in av:
        return {k: _unmarshal_dynamo(v) for k, v in av["M"].items()}
    if "L" in av:
        return [_unmarshal_dynamo(v) for v in av["L"]]
    return None


def lambda_handler(event, context):
    t0 = time.time()
    today_iso = datetime.now(timezone.utc).date().isoformat()

    # ---- 1. read GSI dim-history for the canonical SPY series ----------
    gsi_hist = read_json(GSI_HIST_KEY)
    spy_by_date = {}
    for s in gsi_hist.get("snapshots") or []:
        if isinstance(s.get("spy_close"), (int, float)):
            spy_by_date[s["date"]] = s["spy_close"]

    # ---- 2. read each registered engine's CURRENT score ----------------
    engine_status = []
    current_scores = {}
    for reg in REGISTRY:
        d = read_json(reg["source_key"])
        score = deep_get(d, reg["score_path"])
        engine_status.append({
            "name": reg["name"], "label": reg["label"],
            "source_key": reg["source_key"],
            "current_score": score,
            "live": score is not None,
        })
        if score is not None:
            current_scores[reg["name"]] = float(score)

    # ---- 3. append today's snapshot to the fleet history ---------------
    fleet_hist = read_json(HIST_KEY)
    snaps = fleet_hist.get("snapshots") or []
    # collapse per-date (intra-day re-runs overwrite the same day's row)
    snaps = [s for s in snaps if s.get("date") != today_iso]
    snaps.append({"date": today_iso,
                  "spy_close": spy_by_date.get(today_iso),
                  "scores": current_scores})
    snaps = sorted(snaps, key=lambda s: s.get("date") or "")[-HIST_BARS:]
    write_json(HIST_KEY, {"snapshots": snaps}, cache_seconds=300)

    # ---- 4. build a unified pair-up against forward SPY drawdown -------
    # SPY series spans BOTH the GSI dim-history (long, backfilled) and
    # fleet history rows (forward-going). We union them.
    spy_dated = dict(spy_by_date)
    for s in snaps:
        if s.get("spy_close"):
            spy_dated.setdefault(s["date"], s["spy_close"])
    spy_dates_sorted = sorted(spy_dated.keys())
    # quick lookup by index for the forward window
    idx_by_date = {d: i for i, d in enumerate(spy_dates_sorted)}

    def forward_dd(date_iso):
        """21-session forward SPY drawdown from this date. Requires at
        least FORWARD_DAYS // 2 future SPY closes to count."""
        idx = idx_by_date.get(date_iso)
        if idx is None:
            return None
        start = spy_dated[spy_dates_sorted[idx]]
        future = spy_dates_sorted[idx + 1:idx + 1 + FORWARD_DAYS]
        future_closes = [spy_dated[d] for d in future
                         if isinstance(spy_dated.get(d), (int, float))]
        if len(future_closes) < max(5, FORWARD_DAYS // 2):
            return None
        return max(0.0, (start - min(future_closes)) / start * 100.0)

    # ---- 5. per-engine IC + quality metrics ----------------------------
    engines_out = []
    weight_props = {}
    for reg in REGISTRY:
        name, direction = reg["name"], reg["direction"]
        # collect (score, forward_drawdown) pairs from BOTH histories.
        # GSI history only has the GSI score under reg `global_stress`;
        # others come from the fleet history we maintain ourselves.
        pairs = []
        if name == "global_stress":
            for s in gsi_hist.get("snapshots") or []:
                sc = s.get("gsi")
                dd = forward_dd(s.get("date"))
                if isinstance(sc, (int, float)) and dd is not None:
                    pairs.append((float(sc), dd))
        # union with fleet snapshots (so any engine accumulates forward)
        for s in snaps:
            sc = (s.get("scores") or {}).get(name)
            dd = forward_dd(s.get("date"))
            if isinstance(sc, (int, float)) and dd is not None:
                pairs.append((float(sc), dd))

        # DDB backfill: history-snapshotter has been archiving the
        # engine's full JSON every 5 minutes for as long as it has been
        # in FEEDS_TO_SNAPSHOT. This is what lets the fleet calibrate on
        # real history from day one rather than waiting ~3 months for
        # its own forward-going snapshots to accumulate.
        ddb_history = ddb_history_snapshots(reg["source_key"],
                                            reg["score_path"])
        seen_dates = set()
        for s in snaps:
            if (s.get("scores") or {}).get(name) is not None:
                seen_dates.add(s.get("date"))
        for d_iso, sc in ddb_history:
            if d_iso in seen_dates:
                continue
            dd = forward_dd(d_iso)
            if dd is not None:
                pairs.append((float(sc), dd))
                seen_dates.add(d_iso)
        # dedupe by date if necessary (gsi has more dates than fleet)
        # (a degree of double counting between gsi-dim-history snapshots
        # and current run is acceptable; both feed the same pair list)

        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        n = len(xs)

        if n < MIN_N:
            engines_out.append({
                "name": name, "label": reg["label"], "n_paired": n,
                "ic_spearman": None, "hit_rate": None,
                "ic_first_half": None, "ic_second_half": None,
                "quality_rating": "INSUFFICIENT",
                "weight_proposal": 0.0,
                "current_score": current_scores.get(name),
                "note": ("accumulating forward-going history (need %d "
                         "more sessions)" % (MIN_N - n)) if name not in
                        ("global_stress",) else "insufficient",
            })
            continue

        ic = spearman(xs, ys)
        if direction == "risk_on" and ic is not None:
            ic = -ic     # flip so positive IC = "good predictor"

        # hit rate: when the score is in its top quartile, is forward DD
        # above the sample median? Robust to non-linearity and outliers.
        q75 = quantile(xs, 0.75)
        med_dd = quantile(ys, 0.50)
        if q75 is not None and med_dd is not None:
            top = [(x, y) for x, y in zip(xs, ys) if x >= q75]
            hit = (sum(1 for _, y in top if y > med_dd) / len(top) * 100.0
                   if top else None)
        else:
            hit = None

        # split-half IC stability: each half's IC; large divergence
        # signals a non-stationary relationship.
        half = n // 2
        ic1 = spearman(xs[:half], ys[:half]) if half >= MIN_N // 2 else None
        ic2 = spearman(xs[half:], ys[half:]) if n - half >= MIN_N // 2 else None
        if direction == "risk_on":
            if ic1 is not None:
                ic1 = -ic1
            if ic2 is not None:
                ic2 = -ic2

        # quality
        if ic is None:
            rating = "INSUFFICIENT"
        elif ic >= 0.15:
            rating = "PREDICTIVE"
        elif ic >= 0.05:
            rating = "WEAK_PREDICTIVE"
        elif ic <= -0.15:
            rating = "CONTRARIAN"
        elif ic <= -0.05:
            rating = "WEAK_CONTRARIAN"
        else:
            rating = "NOISE"

        # weight proposal (positive IC only, IC_FLOOR clipped)
        wprop = max(0.0, (ic or 0.0) - IC_FLOOR)
        weight_props[name] = wprop

        engines_out.append({
            "name": name, "label": reg["label"], "n_paired": n,
            "ic_spearman": round(ic, 4) if ic is not None else None,
            "hit_rate": round(hit, 1) if hit is not None else None,
            "ic_first_half": round(ic1, 4) if ic1 is not None else None,
            "ic_second_half": round(ic2, 4) if ic2 is not None else None,
            "ic_stability_gap": (round(abs(ic1 - ic2), 4)
                                  if (ic1 is not None and ic2 is not None)
                                  else None),
            "quality_rating": rating,
            "weight_proposal": round(wprop, 4),
            "current_score": current_scores.get(name),
            "note": "thin sample" if n < MIN_N_STABLE else "",
        })

    # normalise weight proposals across engines
    total_w = sum(weight_props.values())
    if total_w > 0:
        for e in engines_out:
            if e["name"] in weight_props:
                e["weight_proposal_normalised"] = round(
                    weight_props[e["name"]] / total_w, 4)
            else:
                e["weight_proposal_normalised"] = 0.0
    else:
        for e in engines_out:
            e["weight_proposal_normalised"] = 0.0

    # ---- 6. publish weights to SSM (synthesizer-readable) --------------
    ssm_payload = {
        "weights": {e["name"]: e.get("weight_proposal_normalised", 0.0)
                    for e in engines_out},
        "ic": {e["name"]: e["ic_spearman"] for e in engines_out},
        "n_by_engine": {e["name"]: e["n_paired"] for e in engines_out},
        "calibrated_at": datetime.now(timezone.utc).isoformat(),
        "forward_days": FORWARD_DAYS,
        "ic_floor": IC_FLOOR,
    }
    try:
        ssm.put_parameter(Name=WEIGHTS_PARAM, Type="String",
                          Overwrite=True,
                          Value=json.dumps(ssm_payload))
    except Exception as e:
        print("ssm put fail: %s" % e)

    # ---- 7. publish full report ----------------------------------------
    n_predictive = sum(1 for e in engines_out
                       if e["quality_rating"] == "PREDICTIVE")
    n_weak = sum(1 for e in engines_out
                 if e["quality_rating"] in ("WEAK_PREDICTIVE",
                                             "WEAK_CONTRARIAN"))
    n_noise = sum(1 for e in engines_out
                  if e["quality_rating"] == "NOISE")
    n_contra = sum(1 for e in engines_out
                   if e["quality_rating"] == "CONTRARIAN")
    n_insuff = sum(1 for e in engines_out
                   if e["quality_rating"] == "INSUFFICIENT")

    report = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "forward_days": FORWARD_DAYS,
        "ic_floor": IC_FLOOR,
        "min_n": MIN_N,
        "min_n_stable": MIN_N_STABLE,
        "engines": sorted(engines_out,
                          key=lambda e: -(e.get("ic_spearman") or -99)),
        "summary": {
            "engines_total": len(engines_out),
            "predictive": n_predictive,
            "weak": n_weak,
            "noise": n_noise,
            "contrarian": n_contra,
            "insufficient": n_insuff,
            "top_ic": sorted([(e["label"], e["ic_spearman"])
                              for e in engines_out
                              if e["ic_spearman"] is not None],
                             key=lambda x: -x[1])[:3],
        },
        "history_snapshots": len(snaps),
        "spy_dates_available": len(spy_dated),
        "duration_s": round(time.time() - t0, 1),
        "methodology": (
            "For every signal engine in the registry, we pair each "
            "daily score with the 21-session forward SPY drawdown and "
            "compute the Spearman rank IC. PREDICTIVE engines have "
            "IC>=0.15, WEAK_PREDICTIVE 0.05<=IC<0.15, NOISE |IC|<0.05, "
            "WEAK_CONTRARIAN -0.15<IC<=-0.05, CONTRARIAN IC<=-0.15. The "
            "split-half IC measures stability: a large gap between "
            "first-half and second-half IC suggests a non-stationary "
            "relationship. Hit-rate is the % of top-quartile-score "
            "observations that coincided with above-median forward "
            "drawdown -- a robust nonparametric check. Weight proposals "
            "= max(0, IC - 0.05) normalised; SSM "
            "/justhodl/calibration-fleet/weights publishes them for "
            "synthesizer engines to consume."),
    }
    write_json(REPORT_KEY, report, cache_seconds=600)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "engines": len(engines_out),
        "predictive": n_predictive, "noise": n_noise,
        "contrarian": n_contra, "insufficient": n_insuff,
        "elapsed_s": round(time.time() - t0, 1)})}
