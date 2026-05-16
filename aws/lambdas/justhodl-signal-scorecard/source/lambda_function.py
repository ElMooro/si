"""justhodl-signal-scorecard — signal quality grading + decay enforcement.

The platform tracks 200+ signals and a calibrator already learns weights.
What was missing is the institutional discipline layer: a visible scoreboard
of every signal's REALISED edge, and ACTIVE deprecation of dead signals.
Funds kill the bottom half of their signals — this engine does it on evidence.

METHOD
══════
Scans the justhodl-outcomes table (every checked signal: signal_type,
window_key, correct bool, realised return). For each signal_type it computes:

  • sample size n, raw hit-rate
  • Wilson 95% lower bound on the hit-rate — this is the key institutional
    move: a 90%-hit signal with n=4 is NOT graded above a 56%-hit signal
    with n=300. Small samples get no credit until they earn it.
  • average realised return
  • a letter GRADE (A-F) and a STATUS:
       PROMOTED  — proven edge        (Wilson LB >= 0.57)
       ACTIVE    — performing         (LB 0.45-0.57, adequate sample)
       PROBATION — insufficient data  (n < MIN_SAMPLE)
       DEPRECATED— proven no edge     (LB < 0.45 with n >= DEPRECATE_N)
  • a performance_multiplier consumers apply: PROMOTED 1.25, ACTIVE 1.0,
    PROBATION 0.6, DEPRECATED 0.0 — deprecation is enforced, not cosmetic.

OUTPUTS
═══════
  s3://.../data/signal-scorecard.json   — the visible board
  SSM /justhodl/calibration/scorecard   — {signal_type: multiplier} for
                                          downstream consumers to enforce
Schedule: daily.
"""
import json, os, time, math
from datetime import datetime, timezone
from urllib import request
import boto3
from boto3.dynamodb.conditions import Attr

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/signal-scorecard.json"
OUTCOMES_TABLE = "justhodl-outcomes"
SSM_PARAM = "/justhodl/calibration/scorecard"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MIN_SAMPLE = 15        # below this -> PROBATION (not enough evidence)
DEPRECATE_N = 25       # need this much evidence before we kill a signal
PROMOTE_LB = 0.57      # Wilson LB to earn PROMOTED
DEPRECATE_LB = 0.45    # Wilson LB below this (with enough n) -> DEPRECATED

MULTIPLIER = {"PROMOTED": 1.25, "ACTIVE": 1.0, "PROBATION": 0.6, "DEPRECATED": 0.0}

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                               data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def wilson_lower(hits, n, z=1.96):
    """Wilson score interval lower bound — confidence-adjusted hit rate."""
    if n == 0:
        return 0.0
    p = hits / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def grade_for(lb, n):
    if n < MIN_SAMPLE:
        return "—"
    if lb >= 0.60:
        return "A"
    if lb >= 0.53:
        return "B"
    if lb >= 0.47:
        return "C"
    if lb >= 0.40:
        return "D"
    return "F"


def status_for(lb, n):
    if n < MIN_SAMPLE:
        return "PROBATION"
    if lb >= PROMOTE_LB:
        return "PROMOTED"
    if lb < DEPRECATE_LB and n >= DEPRECATE_N:
        return "DEPRECATED"
    return "ACTIVE"


def scan_outcomes():
    """Full scan of the outcomes table with pagination."""
    table = ddb.Table(OUTCOMES_TABLE)
    items, kwargs = [], {}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
        if len(items) > 100000:
            break
    return items


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[signal-scorecard] starting {datetime.now(timezone.utc).isoformat()}")

    outcomes = scan_outcomes()
    print(f"[signal-scorecard] scanned {len(outcomes)} outcome records")

    # group by signal_type
    by_sig = {}
    for o in outcomes:
        st = o.get("signal_type")
        if not st:
            continue
        g = by_sig.setdefault(st, {"n": 0, "hits": 0, "rets": [], "windows": {}})
        g["n"] += 1
        correct = bool(o.get("correct"))
        if correct:
            g["hits"] += 1
        # realised return
        oc = o.get("outcome") or {}
        r = oc.get("return_pct", oc.get("excess_return"))
        try:
            if r is not None:
                g["rets"].append(float(r))
        except (TypeError, ValueError):
            pass
        wk = o.get("window_key") or "all"
        w = g["windows"].setdefault(wk, {"n": 0, "hits": 0})
        w["n"] += 1
        if correct:
            w["hits"] += 1

    scorecard = []
    for st, g in by_sig.items():
        n, hits = g["n"], g["hits"]
        hit_rate = hits / n if n else 0.0
        lb = wilson_lower(hits, n)
        avg_ret = round(sum(g["rets"]) / len(g["rets"]), 2) if g["rets"] else None
        grade = grade_for(lb, n)
        status = status_for(lb, n)
        windows = {wk: {"n": w["n"],
                        "hit_rate": round(w["hits"] / w["n"], 3) if w["n"] else None,
                        "wilson_lb": round(wilson_lower(w["hits"], w["n"]), 3)}
                   for wk, w in g["windows"].items()}
        scorecard.append({
            "signal_type": st,
            "n": n,
            "hits": hits,
            "hit_rate": round(hit_rate, 3),
            "wilson_lb": round(lb, 3),
            "edge_vs_coinflip_pct": round((lb - 0.5) * 100, 1),
            "avg_return_pct": avg_ret,
            "grade": grade,
            "status": status,
            "performance_multiplier": MULTIPLIER[status],
            "by_window": windows,
        })

    # rank: promoted first, then by Wilson lower bound
    order = {"PROMOTED": 0, "ACTIVE": 1, "PROBATION": 2, "DEPRECATED": 3}
    scorecard.sort(key=lambda r: (order[r["status"]], -r["wilson_lb"]))

    promoted = [r["signal_type"] for r in scorecard if r["status"] == "PROMOTED"]
    deprecated = [r["signal_type"] for r in scorecard if r["status"] == "DEPRECATED"]
    probation = [r["signal_type"] for r in scorecard if r["status"] == "PROBATION"]
    graded = [r for r in scorecard if r["n"] >= MIN_SAMPLE]
    portfolio_lb = (round(sum(r["wilson_lb"] for r in graded) / len(graded), 3)
                    if graded else None)

    out = {
        "schema_version": "1.0",
        "method": "signal_scorecard_wilson_lb",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "n_outcomes_scanned": len(outcomes),
        "n_signals_tracked": len(scorecard),
        "n_promoted": len(promoted),
        "n_deprecated": len(deprecated),
        "n_probation": len(probation),
        "avg_graded_wilson_lb": portfolio_lb,
        "promoted_signals": promoted,
        "deprecated_signals": deprecated,
        "scorecard": scorecard,
        "thresholds": {"min_sample": MIN_SAMPLE, "deprecate_n": DEPRECATE_N,
                       "promote_lb": PROMOTE_LB, "deprecate_lb": DEPRECATE_LB},
        "interpretation": (
            "Every signal graded on its Wilson 95% lower-bound hit rate — small "
            "samples get no credit until they earn it. DEPRECATED signals have "
            "proven (n>=25) they cannot beat a coin flip; their "
            "performance_multiplier is 0 so consumers stop using them. "
            "PROMOTED signals get a 1.25x boost."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    # publish the enforcement map to SSM for downstream consumers
    multipliers = {r["signal_type"]: r["performance_multiplier"] for r in scorecard}
    try:
        ssm.put_parameter(Name=SSM_PARAM, Type="String", Overwrite=True,
                          Value=json.dumps({"updated_at": out["generated_at"],
                                            "multipliers": multipliers,
                                            "deprecated": deprecated}))
        print(f"[signal-scorecard] SSM {SSM_PARAM} updated ({len(multipliers)} signals)")
    except Exception as e:
        print(f"[signal-scorecard] SSM write failed: {e}")

    # alert on newly deprecated / promoted vs last run
    prior = {}
    try:
        prev = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY + ".prev")["Body"].read())
        prior = {r["signal_type"]: r["status"] for r in prev.get("scorecard", [])}
    except Exception:
        pass
    newly_dep = [s for s in deprecated if prior.get(s) and prior.get(s) != "DEPRECATED"]
    newly_pro = [s for s in promoted if prior.get(s) and prior.get(s) != "PROMOTED"]
    if newly_dep or newly_pro:
        lines = []
        if newly_dep:
            lines.append("DEPRECATED (no proven edge): " + ", ".join(newly_dep))
        if newly_pro:
            lines.append("PROMOTED (proven edge): " + ", ".join(newly_pro))
        maybe_telegram("[signal-scorecard] <b>Signal status changes</b>\n" + "\n".join(lines))
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY + ".prev",
                       Body=json.dumps(out, default=str).encode("utf-8"),
                       ContentType="application/json")
    except Exception:
        pass

    print(f"[signal-scorecard] done {out['elapsed_s']}s tracked={len(scorecard)} "
          f"promoted={len(promoted)} deprecated={len(deprecated)} probation={len(probation)}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_signals_tracked": len(scorecard),
        "n_promoted": len(promoted), "n_deprecated": len(deprecated),
        "n_probation": len(probation), "avg_graded_wilson_lb": portfolio_lb})}
