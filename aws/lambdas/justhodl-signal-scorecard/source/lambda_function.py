"""justhodl-signal-scorecard — signal quality grading + decay enforcement.

The platform tracks 200+ signals and a calibrator already learns weights.
What was missing is the institutional discipline layer: a visible scoreboard
of every signal's REALISED edge, and ACTIVE deprecation of dead signals.
Funds kill the bottom half of their signals — this engine does it on evidence.

WHAT COUNTS AS A SCORABLE OUTCOME
═════════════════════════════════
A signal is graded ONLY on outcomes that can actually confirm or deny it.
Three classes of outcome are excluded from grading, because scoring them
would punish a signal for a data problem rather than for being wrong:

  1. NEUTRAL prediction — the signal made no directional call. predicted_dir
     not in {UP,DOWN,OUTPERFORM,UNDERPERFORM,...}. Counted as n_neutral.
  2. LEGACY records — flagged is_legacy=1 (legacy_reason pre_baseline_fix_*).
     correct=None, actual_direction=UNKNOWN, price_at_signal=0. Never validly
     scored. Counted as n_legacy.
  3. UNRESOLVED outcomes — the outcome-checker could not establish a real
     realised move (actual_direction UNKNOWN/NEUTRAL, or a no-op price where
     price_at_signal == price_at_check). Counted as n_unresolved — this also
     SURFACES upstream data-pipeline gaps (e.g. an unpriceable instrument).

Only SCORED outcomes (directional prediction + resolved + non-legacy) drive
the hit-rate, Wilson LB, grade and status.

CORRECTNESS IS DERIVED, NOT TRUSTED
═══════════════════════════════════
For every scored outcome the engine recomputes correctness from ground
truth — predicted direction vs the sign of the realised (relative or
absolute) return — rather than trusting the stored `correct` flag. The
agreement rate with the stored flag is reported as an integrity metric.

METHOD
══════
For each signal_type, over its SCORED outcomes only:
  • n_scored, raw hit-rate
  • Wilson 95% lower bound on the hit-rate — a 90%-hit signal with n=4 is
    NOT graded above a 56%-hit signal with n=300. Small samples earn it.
  • average realised return
  • a letter GRADE (A-F) and a STATUS:
       PROMOTED     — proven edge          (Wilson LB >= 0.57)
       ACTIVE       — performing           (LB 0.45-0.57, adequate sample)
       INSUFFICIENT — too few scored outcomes to grade (n_scored < MIN)
       DEPRECATED   — proven no edge        (LB < 0.45 with n_scored >= DEPRECATE_N)
  • performance_multiplier: PROMOTED 1.25, ACTIVE 1.0, INSUFFICIENT 1.0
    (no evidence -> no penalty), DEPRECATED 0.0 — enforced, not cosmetic.

OUTPUTS
═══════
  s3://.../data/signal-scorecard.json   — the visible board
  SSM /justhodl/calibration/scorecard   — {signal_type: multiplier}
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

MIN_SAMPLE = 15        # below this many SCORED outcomes -> INSUFFICIENT
DEPRECATE_N = 25       # need this many scored outcomes before we kill a signal
PROMOTE_LB = 0.57      # Wilson LB to earn PROMOTED
DEPRECATE_LB = 0.45    # Wilson LB below this (with enough n) -> DEPRECATED

# predicted_dir buckets — a prediction only scores if it made a directional call
UP_DIRS = {"UP", "OUTPERFORM", "LONG", "BULLISH"}
DOWN_DIRS = {"DOWN", "UNDERPERFORM", "SHORT", "BEARISH"}
DIRECTIONAL = UP_DIRS | DOWN_DIRS

MULTIPLIER = {"PROMOTED": 1.25, "ACTIVE": 1.0, "INSUFFICIENT": 1.0, "DEPRECATED": 0.0}

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


def num(v):
    """Return float(v) or None if v is not a real number."""
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None   # reject NaN
    except (TypeError, ValueError):
        return None


def predicted_dir(o):
    """Normalised predicted direction; '' when no directional call was made."""
    pd = (o.get("predicted_dir") or o.get("predicted_direction") or "").strip().upper()
    return pd if pd in DIRECTIONAL else ""


def classify(o, pd):
    """Decide how an outcome should be treated. Returns (state, correct).

    state ∈ {legacy, unresolved, scored}; correct is bool only when scored.
    Correctness is DERIVED from ground truth, not read from the stored flag.
    """
    # 1 — legacy: explicitly flagged pre-baseline records, never validly scored
    if o.get("is_legacy") or o.get("legacy_reason"):
        return "legacy", None

    oc = o.get("outcome") or {}
    up_pred = pd in UP_DIRS

    # 2 — relative outcome (benchmark-relative signals e.g. screener_top_pick)
    er = num(oc.get("excess_return"))
    if er is not None:
        if er == 0.0:
            return "unresolved", None        # no measurable relative move
        correct = (er > 0) if up_pred else (er < 0)
        return "scored", correct

    # 3 — absolute outcome: needs a real realised direction
    ad = str(oc.get("actual_direction") or "").strip().upper()
    rp = num(oc.get("return_pct"))
    p_sig = num(oc.get("price_at_signal"))
    p_chk = num(oc.get("price_at_check"))
    # no-op price artifact: checker could not price the instrument
    if (p_sig is not None and p_chk is not None
            and p_sig == p_chk) or p_sig == 0:
        return "unresolved", None
    if ad in ("UP", "DOWN") and rp is not None:
        correct = (ad == "UP") if up_pred else (ad == "DOWN")
        return "scored", correct
    # fall back to return sign when actual_direction missing but return is real
    if rp is not None and rp != 0.0:
        correct = (rp > 0) if up_pred else (rp < 0)
        return "scored", correct

    return "unresolved", None


def grade_for(lb, n_scored):
    if n_scored < MIN_SAMPLE:
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


def status_for(lb, n_scored):
    """Status is decided on the SCORED sample only."""
    if n_scored < MIN_SAMPLE:
        return "INSUFFICIENT"          # cannot grade -> no penalty, no boost
    if lb >= PROMOTE_LB:
        return "PROMOTED"
    if lb < DEPRECATE_LB and n_scored >= DEPRECATE_N:
        return "DEPRECATED"            # proven it cannot beat a coin flip
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

    # group by signal_type, classifying every outcome
    by_sig = {}
    for o in outcomes:
        st = o.get("signal_type")
        if not st:
            continue
        g = by_sig.setdefault(st, {"n": 0, "n_neutral": 0, "n_legacy": 0,
                                   "n_unresolved": 0, "n_scored": 0, "hits": 0,
                                   "stored_agree": 0, "rets": [], "windows": {}})
        g["n"] += 1
        pd = predicted_dir(o)
        if not pd:
            g["n_neutral"] += 1
            continue

        state, correct = classify(o, pd)
        if state == "legacy":
            g["n_legacy"] += 1
            continue
        if state == "unresolved":
            g["n_unresolved"] += 1
            continue

        # state == "scored"
        g["n_scored"] += 1
        if correct:
            g["hits"] += 1
        # integrity check vs the stored flag
        stored = o.get("correct")
        if isinstance(stored, bool) or stored in (0, 1):
            if bool(stored) == bool(correct):
                g["stored_agree"] += 1
        oc = o.get("outcome") or {}
        r = num(oc.get("return_pct"))
        if r is None:
            r = num(oc.get("excess_return"))
        if r is not None:
            g["rets"].append(r)
        wk = o.get("window_key") or "all"
        w = g["windows"].setdefault(wk, {"n": 0, "hits": 0})
        w["n"] += 1
        if correct:
            w["hits"] += 1

    scorecard = []
    for st, g in by_sig.items():
        n_scored = g["n_scored"]
        hits = g["hits"]
        hit_rate = hits / n_scored if n_scored else 0.0
        lb = wilson_lower(hits, n_scored)
        avg_ret = round(sum(g["rets"]) / len(g["rets"]), 2) if g["rets"] else None
        grade = grade_for(lb, n_scored)
        status = status_for(lb, n_scored)
        agree = round(g["stored_agree"] / n_scored, 3) if n_scored else None
        windows = {wk: {"n": w["n"],
                        "hit_rate": round(w["hits"] / w["n"], 3) if w["n"] else None,
                        "wilson_lb": round(wilson_lower(w["hits"], w["n"]), 3)}
                   for wk, w in g["windows"].items()}
        scorecard.append({
            "signal_type": st,
            "n_total": g["n"],
            "n_scored": n_scored,
            "n_neutral": g["n_neutral"],
            "n_legacy": g["n_legacy"],
            "n_unresolved": g["n_unresolved"],
            "hits": hits,
            "hit_rate": round(hit_rate, 3),
            "wilson_lb": round(lb, 3),
            "edge_vs_coinflip_pct": round((lb - 0.5) * 100, 1),
            "avg_return_pct": avg_ret,
            "stored_flag_agreement": agree,
            "grade": grade,
            "status": status,
            "performance_multiplier": MULTIPLIER[status],
            "by_window": windows,
        })

    # rank: promoted first, then by Wilson lower bound
    order = {"PROMOTED": 0, "ACTIVE": 1, "INSUFFICIENT": 2, "DEPRECATED": 3}
    scorecard.sort(key=lambda r: (order[r["status"]], -r["wilson_lb"]))

    promoted = [r["signal_type"] for r in scorecard if r["status"] == "PROMOTED"]
    deprecated = [r["signal_type"] for r in scorecard if r["status"] == "DEPRECATED"]
    insufficient = [r["signal_type"] for r in scorecard if r["status"] == "INSUFFICIENT"]
    graded = [r for r in scorecard if r["n_scored"] >= MIN_SAMPLE]
    portfolio_lb = (round(sum(r["wilson_lb"] for r in graded) / len(graded), 3)
                    if graded else None)

    # data-quality flags: signals losing most of their outcomes to legacy/unresolved
    dq_flags = []
    for r in scorecard:
        lost = r["n_legacy"] + r["n_unresolved"]
        if r["n_total"] >= 20 and lost / r["n_total"] >= 0.5:
            dq_flags.append({"signal_type": r["signal_type"], "n_total": r["n_total"],
                             "n_legacy": r["n_legacy"], "n_unresolved": r["n_unresolved"],
                             "n_scored": r["n_scored"],
                             "note": ("mostly legacy — pre-baseline backfill"
                                      if r["n_legacy"] >= r["n_unresolved"]
                                      else "outcome-checker cannot resolve a realised move")})

    out = {
        "schema_version": "2.1",
        "method": "signal_scorecard_wilson_lb_scored_only",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "n_outcomes_scanned": len(outcomes),
        "n_outcomes_scored": sum(r["n_scored"] for r in scorecard),
        "n_outcomes_neutral": sum(r["n_neutral"] for r in scorecard),
        "n_outcomes_legacy": sum(r["n_legacy"] for r in scorecard),
        "n_outcomes_unresolved": sum(r["n_unresolved"] for r in scorecard),
        "n_signals_tracked": len(scorecard),
        "n_signals_graded": len(graded),
        "n_promoted": len(promoted),
        "n_deprecated": len(deprecated),
        "n_insufficient": len(insufficient),
        "avg_graded_wilson_lb": portfolio_lb,
        "promoted_signals": promoted,
        "deprecated_signals": deprecated,
        "data_quality_flags": dq_flags,
        "scorecard": scorecard,
        "thresholds": {"min_sample": MIN_SAMPLE, "deprecate_n": DEPRECATE_N,
                       "promote_lb": PROMOTE_LB, "deprecate_lb": DEPRECATE_LB},
        "interpretation": (
            "Signals are graded ONLY on SCORED outcomes — a directional "
            "prediction with a resolved realised move. NEUTRAL predictions, "
            "legacy pre-baseline records, and unresolved outcomes (the "
            "outcome-checker could not price the instrument) are counted and "
            "reported but never scored as misses. Correctness is recomputed "
            "from ground truth, not trusted from the stored flag; "
            "stored_flag_agreement reports how often the two agree. DEPRECATED "
            "signals proved (n_scored>=25) they cannot beat a coin flip — "
            "their multiplier is 0. PROMOTED signals get 1.25x. INSUFFICIENT "
            "signals lack the scored sample to grade and keep a neutral 1.0x. "
            "data_quality_flags lists signals losing most outcomes to legacy "
            "or unresolved records — an upstream pipeline gap to fix."
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
        
        # Emit one event per newly-changed signal so downstream engines
        # (engine-signal-map, miss-calibrator) can refresh immediately.
        # Fire-and-forget — never blocks scorecard run.
        try:
            from system_events import (
                publish_many, EVT_SIGNAL_DEPRECATED, EVT_SIGNAL_PROMOTED,
            )
            events_to_pub = []
            for s in newly_dep:
                # Find this signal's stats for richer event payload
                row = next((r for r in scorecard if r.get("signal_type") == s), {})
                events_to_pub.append((EVT_SIGNAL_DEPRECATED, {
                    "signal_type":   s,
                    "wilson_lb":     row.get("wilson_lb"),
                    "n_scored":      row.get("n_scored"),
                    "previous_status": prior.get(s),
                    "reason":        "scorecard moved below deprecate threshold",
                }))
            for s in newly_pro:
                row = next((r for r in scorecard if r.get("signal_type") == s), {})
                events_to_pub.append((EVT_SIGNAL_PROMOTED, {
                    "signal_type":   s,
                    "wilson_lb":     row.get("wilson_lb"),
                    "n_scored":      row.get("n_scored"),
                    "previous_status": prior.get(s),
                    "reason":        "scorecard reached promote threshold",
                }))
            # EventBridge limit: 10 per batch
            for i in range(0, len(events_to_pub), 10):
                publish_many(events_to_pub[i:i+10])
        except Exception as e:
            print(f"[signal-scorecard] event publish failed: {e}")
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY + ".prev",
                       Body=json.dumps(out, default=str).encode("utf-8"),
                       ContentType="application/json")
    except Exception:
        pass

    print(f"[signal-scorecard] done {out['elapsed_s']}s tracked={len(scorecard)} "
          f"graded={len(graded)} promoted={len(promoted)} deprecated={len(deprecated)} "
          f"insufficient={len(insufficient)} dq_flags={len(dq_flags)}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_signals_tracked": len(scorecard), "n_signals_graded": len(graded),
        "n_promoted": len(promoted), "n_deprecated": len(deprecated),
        "n_insufficient": len(insufficient), "avg_graded_wilson_lb": portfolio_lb,
        "n_outcomes_scored": out["n_outcomes_scored"],
        "n_outcomes_legacy": out["n_outcomes_legacy"],
        "n_outcomes_unresolved": out["n_outcomes_unresolved"]})}
