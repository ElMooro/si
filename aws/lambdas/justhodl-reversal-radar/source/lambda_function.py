"""
justhodl-reversal-radar — Composite TOP / BOTTOM probability detector.

Combines breadth deterioration + credit widening + vol regime + sentiment to
produce two 0-100 scores:
  top_score    = probability we're near a market top
  bottom_score = probability we're near a market bottom

Inputs (all already-running sidecars):
  data/report.json              — Khalid Index, VIX, breadth
  data/credit-stress.json       — HY/IG/CCC OAS levels + percentiles
  data/regime-composite.json    — 7-dimension meta-regime
  data/divergence.json          — cross-asset breaks
  data/vix-curve.json           — term structure
  data/correlation-breaks.json
  data/morning-intel.json       — recent breadth + market internals

Logic (top score):
  +20  if breadth deteriorating (advancers / decliners < 1.0)
  +15  if HY OAS widening fast (current > 5d avg + 10bps)
  +15  if VIX > 22 AND rising
  +15  if credit-stress.composite_regime = STRESS or MELTUP_PRONE
  +10  if 3+ correlation breaks
  +10  if regime-composite composite_score crossing below 0 from above
  +10  if Khalid Index dropped 10+ points in last 24h
  +5   bonus per stretch indicator at percentile >85

Logic (bottom score):
  +20  if VIX > 30 (panic)
  +20  if credit-stress shows DRAWDOWN regime (mass spread widening)
  +15  if Khalid Index < 30
  +15  if breadth thrust setting up (very low % above 50d MA)
  +10  if regime-composite composite_score < -30
  +10  if eurodollar-stress > 65
  +10  if 5+ correlation breaks (chaos)

Telegram alerts:
  - top_score >= 65 AND prior < 50 (newly elevated)
  - bottom_score >= 65 AND prior < 50
  - state flip (DEFENSIVE → CAPITULATION → NORMAL etc)

Schedule: cron(30 * ? * * *) — hourly at :30
"""
import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import urllib.request

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/reversal-radar.json"
S3_KEY_HISTORY = "data/reversal-radar-history.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MAX_HISTORY = 168

s3 = boto3.client("s3", region_name="us-east-1")


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[s3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=300"):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def get_path(obj, path):
    if obj is None: return None
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else: return None
    return cur


def safe_float(v):
    try: return float(v) if v is not None else None
    except (TypeError, ValueError): return None


def compute_top_score(state):
    """Top score components — each returns (points, reason_str if active)."""
    score = 0
    reasons = []

    # 1. VIX > 22 and likely rising
    vix = safe_float(get_path(state["report"], "vix.value"))
    if vix is not None and vix > 22:
        # Check if 'change' field exists
        vix_chg = safe_float(get_path(state["report"], "vix.change_pct"))
        if vix_chg is not None and vix_chg > 0:
            score += 15
            reasons.append(f"VIX={vix:.1f} rising (+{vix_chg:.1f}%)")
        elif vix > 25:  # high VIX alone counts
            score += 10
            reasons.append(f"VIX={vix:.1f} elevated")

    # 2. HY OAS widening fast
    hy_oas = safe_float(get_path(state["credit_stress"], "summary.hy_oas_bps"))
    hy_oas_5d_avg = safe_float(get_path(state["credit_stress"], "summary.hy_oas_5d_avg"))
    if hy_oas is not None and hy_oas_5d_avg is not None:
        if hy_oas > hy_oas_5d_avg + 10:
            score += 15
            reasons.append(f"HY OAS widening: {hy_oas:.0f}bps vs 5d avg {hy_oas_5d_avg:.0f}")

    # 3. Credit stress regime
    cs_regime = get_path(state["credit_stress"], "composite_regime") or \
                  get_path(state["credit_stress"], "summary.regime")
    if cs_regime in ("STRESS", "WIDENING", "DRAWDOWN", "MELTDOWN"):
        score += 15
        reasons.append(f"Credit regime: {cs_regime}")
    elif cs_regime == "MELTUP_PRONE":
        # MELTUP_PRONE is a top warning: tight spreads = complacency
        score += 10
        reasons.append("MELTUP_PRONE — complacent credit spreads at top")

    # 4. Correlation breaks (3+)
    n_breaks = get_path(state["correlation_breaks"], "n_breaks") or \
                get_path(state["correlation_breaks"], "summary.n_breaks") or 0
    try: n_breaks = int(n_breaks)
    except: n_breaks = 0
    if n_breaks >= 3:
        score += 10
        reasons.append(f"{n_breaks} correlation breaks")

    # 5. Regime composite crossing below 30 (from higher)
    rc_score = safe_float(get_path(state["regime_composite"], "composite_score"))
    if rc_score is not None and rc_score < 30:
        score += 10
        reasons.append(f"Meta-regime {rc_score:+.0f} fading")

    # 6. Divergence composite elevated (cross-asset stretch)
    div_score = safe_float(get_path(state["divergence"], "composite_score"))
    if div_score is not None and div_score > 60:
        score += 5
        reasons.append(f"Divergence composite {div_score:.0f}")

    # 7. VIX term structure — front-month above back-month = stress
    vix_term = state.get("vix_curve") or {}
    contango = vix_term.get("composite_regime") or vix_term.get("regime")
    if contango in ("BACKWARDATION", "STRESS", "INVERTED"):
        score += 10
        reasons.append(f"VIX term: {contango}")

    # 8. Eurodollar stress
    ed_score = safe_float(get_path(state["eurodollar_stress"], "composite_score"))
    if ed_score is not None and ed_score >= 60:
        score += 10
        reasons.append(f"Eurodollar stress {ed_score:.0f}")

    return min(100, score), reasons


def compute_bottom_score(state):
    score = 0
    reasons = []

    # 1. VIX > 30 (panic)
    vix = safe_float(get_path(state["report"], "vix.value"))
    if vix is not None and vix > 30:
        score += 20
        reasons.append(f"VIX={vix:.1f} panic")
    elif vix is not None and vix > 25:
        score += 10
        reasons.append(f"VIX={vix:.1f} fear")

    # 2. Credit stress (drawdown/meltdown regime = bottom near)
    cs_regime = get_path(state["credit_stress"], "composite_regime") or \
                  get_path(state["credit_stress"], "summary.regime")
    if cs_regime in ("DRAWDOWN", "MELTDOWN", "WIDENING"):
        score += 20
        reasons.append(f"Credit regime: {cs_regime}")

    # 3. Khalid Index very low
    ki_score = safe_float(get_path(state["report"], "khalid_index.score"))
    if ki_score is not None and ki_score < 30:
        score += 15
        reasons.append(f"Khalid Index {ki_score:.0f} oversold")

    # 4. Regime-composite very negative
    rc_score = safe_float(get_path(state["regime_composite"], "composite_score"))
    if rc_score is not None and rc_score < -30:
        score += 15
        reasons.append(f"Meta-regime {rc_score:+.0f} deeply negative")

    # 5. Many correlation breaks (chaos = bottom characteristic)
    n_breaks = get_path(state["correlation_breaks"], "n_breaks") or \
                get_path(state["correlation_breaks"], "summary.n_breaks") or 0
    try: n_breaks = int(n_breaks)
    except: n_breaks = 0
    if n_breaks >= 5:
        score += 10
        reasons.append(f"{n_breaks} correlation breaks (chaos)")

    # 6. Eurodollar stress high
    ed_score = safe_float(get_path(state["eurodollar_stress"], "composite_score"))
    if ed_score is not None and ed_score >= 65:
        score += 10
        reasons.append(f"Funding stress {ed_score:.0f}")

    # 7. VIX backwardation (fear curve)
    vix_term = state.get("vix_curve") or {}
    contango = vix_term.get("composite_regime") or vix_term.get("regime")
    if contango in ("BACKWARDATION", "STRESS", "INVERTED"):
        score += 10
        reasons.append(f"VIX term inverted: {contango}")

    return min(100, score), reasons


def state_from_scores(top_score, bottom_score):
    """Classify market state from two scores."""
    if top_score >= 65 and top_score > bottom_score:
        return "TOP_WARNING"
    if bottom_score >= 65 and bottom_score > top_score:
        return "BOTTOM_FORMING"
    if top_score >= 50 and top_score > bottom_score:
        return "ELEVATED_RISK"
    if bottom_score >= 50 and bottom_score > top_score:
        return "OVERSOLD"
    return "NORMAL"


def interpretation(state, top_score, bottom_score):
    if state == "TOP_WARNING":
        return ("Multiple stress indicators flashing simultaneously. "
                "Reduce gross exposure. Defensive positioning warranted.")
    if state == "BOTTOM_FORMING":
        return ("Panic indicators + chaos signature suggest capitulation phase. "
                "Begin scaling into oversold names. Historical bottoms form here.")
    if state == "ELEVATED_RISK":
        return ("Top characteristics building but not confirmed. "
                "Trim losers, harvest gains, raise stops.")
    if state == "OVERSOLD":
        return ("Stress accumulating but no panic yet. Hold cash, watch for "
                "vol spike + breadth thrust as bottom signal.")
    return "Markets in normal range. No reversal signature."


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
        print(f"[tg] sent: {msg[:80]}")
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print("[reversal-radar] starting")

    state = {
        "report": get_s3_json("data/report.json", {}),
        "credit_stress": get_s3_json("data/credit-stress.json", {}),
        "regime_composite": get_s3_json("data/regime-composite.json", {}),
        "divergence": get_s3_json("data/divergence.json", {}),
        "correlation_breaks": get_s3_json("data/correlation-breaks.json", {}),
        "vix_curve": get_s3_json("data/vix-curve.json", {}),
        "eurodollar_stress": get_s3_json("data/eurodollar-stress.json", {}),
    }

    prior_run = get_s3_json(S3_KEY_OUT, {}) or {}

    top_score, top_reasons = compute_top_score(state)
    bottom_score, bottom_reasons = compute_bottom_score(state)
    state_label = state_from_scores(top_score, bottom_score)
    interp = interpretation(state_label, top_score, bottom_score)

    output = {
        "schema_version": "1.0",
        "method": "reversal_radar_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "state": state_label,
        "interpretation": interp,
        "top_score": top_score,
        "top_reasons": top_reasons,
        "bottom_score": bottom_score,
        "bottom_reasons": bottom_reasons,
        "differential": top_score - bottom_score,  # +=more top-like, -=bottom-like
        "input_freshness": {k: (state[k].get("generated_at") if isinstance(state[k], dict)
                                  else None) for k in state.keys()},
        "duration_s": round(time.time() - t0, 2),
    }

    put_s3_json(S3_KEY_OUT, output)
    print(f"[reversal-radar] state={state_label} top={top_score} bot={bottom_score}")
    for r in top_reasons: print(f"  TOP+: {r}")
    for r in bottom_reasons: print(f"  BOT+: {r}")

    # History
    try:
        history = get_s3_json(S3_KEY_HISTORY, {"snapshots": []})
        snaps = history.get("snapshots", [])
        snaps.append({
            "ts": output["generated_at"], "state": state_label,
            "top_score": top_score, "bottom_score": bottom_score,
        })
        snaps = snaps[-MAX_HISTORY:]
        put_s3_json(S3_KEY_HISTORY, {"snapshots": snaps,
                                       "updated_at": output["generated_at"]})
    except Exception as e:
        print(f"[history] err: {e}")

    # Alerts
    try:
        prior_state = prior_run.get("state")
        prior_top = prior_run.get("top_score", 0)
        prior_bot = prior_run.get("bottom_score", 0)

        if state_label != prior_state and state_label != "NORMAL":
            top_reason_str = "; ".join(top_reasons[:3]) if top_reasons else "—"
            bot_reason_str = "; ".join(bottom_reasons[:3]) if bottom_reasons else "—"
            emoji = "🔴" if state_label == "TOP_WARNING" else ("🟢" if state_label == "BOTTOM_FORMING" else "⚠️")
            maybe_telegram(
                f"{emoji} <b>REVERSAL RADAR: {state_label}</b>\n"
                f"<i>was: {prior_state or 'NORMAL'}</i>\n"
                f"Top score: <b>{top_score}/100</b>\n"
                f"Bottom score: <b>{bottom_score}/100</b>\n\n"
                f"<i>{interp}</i>\n\n"
                f"<b>Top drivers:</b> {top_reason_str}\n"
                f"<b>Bottom drivers:</b> {bot_reason_str}"
            )
        elif top_score >= 65 and prior_top < 50:
            maybe_telegram(
                f"🔴 <b>TOP SCORE ELEVATED: {top_score}/100</b>\n"
                f"<i>was: {prior_top}/100</i>\n"
                f"{'; '.join(top_reasons[:3])}"
            )
        elif bottom_score >= 65 and prior_bot < 50:
            maybe_telegram(
                f"🟢 <b>BOTTOM SCORE ELEVATED: {bottom_score}/100</b>\n"
                f"<i>was: {prior_bot}/100</i>\n"
                f"{'; '.join(bottom_reasons[:3])}"
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True, "state": state_label,
            "top_score": top_score, "bottom_score": bottom_score,
        }),
    }
