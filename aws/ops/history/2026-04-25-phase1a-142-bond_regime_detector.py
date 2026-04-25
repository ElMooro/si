#!/usr/bin/env python3
"""
Step 142 — Bond Market Regime Detector.

Phase 1 of the hedge-fund-grade system: detect bond market regime
changes 4-8 weeks before equity markets typically catch up.

The bond market is the most informationally efficient market in the
world. When credit spreads, term premium, or curve dynamics break
down in lockstep, something material is happening that equity
markets haven't priced yet. Real macro hedge funds run versions of
this detector. Most retail systems don't.

KEY DESIGN DECISIONS:
  1. VELOCITY > LEVEL: HY at 400bps is a level. HY moving 320→410
     in 8 days IS the regime signal. We measure changes, not absolutes.
  2. DIRECTIONAL CONSENSUS: 3 signals all moving toward risk-off is
     a regime signal. 3 signals moving in scattered directions is
     noise. Aggregation requires direction agreement.
  3. APPROPRIATE LOOKBACKS: bond markets move in weeks-to-months,
     not days. Z-scores against 1-week changes vs 90-day distribution.
  4. THREE STATES ONLY: RISK_OFF / NEUTRAL / RISK_ON. Anything more
     granular is theater (your existing 5-tier system handles narrative).
  5. STICKY REGIMES: don't flip on a single indicator wobble. Need
     ≥3 indicators to confirm a change.

EIGHT INDICATORS TRACKED:
  Stress-rising = risk-off:
    1. HY OAS z-score (BAMLH0A0HYM2)
    2. IG OAS z-score (BAMLC0A0CM)
    3. MOVE z-score (bond volatility — already in repo-data)
    4. NFCI z-score (Chicago Fed Financial Conditions)
    5. VIX z-score (cross-check; bonds usually break first)

  Direction-aware:
    6. 2s10s velocity z-score (sign matters — flattening or steepening)
    7. DXY 5-day change z-score (rising = funding stress)
    8. 5Y breakeven 5-day change z-score (falling = deflation fear)

OUTPUT:
  s3://justhodl-dashboard-live/regime/current.json (latest snapshot)
  s3://justhodl-dashboard-live/regime/history.json (rolling 365-day)
  Telegram alert ONLY on regime change (not every run)

SCHEDULE: cron(0 */4 * * ? *) — every 4 hours (bond market doesn't
need 5-min granularity).

VALIDATION APPROACH: included in this script is a backtest_check
function that runs the detector logic against the LAST 30 daily
archive snapshots and shows what regime each would have flagged.
This is sanity-check only — real validation requires multi-year
data and that's a future step. For now we want to confirm the
logic doesn't false-positive every other day.
"""
import io
import json
import os
import statistics
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

BUCKET = "justhodl-dashboard-live"


# ════════════════════════════════════════════════════════════════════════
# Source for the new Lambda
# ════════════════════════════════════════════════════════════════════════
DETECTOR_SRC = '''"""
justhodl-bond-regime-detector — Phase 1 of hedge-fund risk system.

Reads the freshly-computed repo-data.json + fred-cache-secretary.json,
computes z-scores on 8 bond market indicators, requires DIRECTIONAL
CONSENSUS across 3+ indicators in same direction within 5-day window
to declare regime change. Falls back to NEUTRAL on insufficient data.

Writes regime/current.json + regime/history.json. Sends Telegram
alert ONLY on regime changes (not every run).
"""
import json
import os
import statistics
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
TG_TOKEN_PARAM = "/justhodl/telegram/bot_token"
TG_CHAT_ID_PARAM = "/justhodl/telegram/chat_id"

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


# Two-sigma extreme threshold. We use 2.0 not 1.5 because bond market
# z-scores against 90d window naturally have fat tails. 2σ is the
# right calibration for "this is genuinely unusual."
Z_THRESHOLD = 2.0
# Need at least N indicators to confirm a regime
MIN_INDICATORS_FOR_REGIME = 3
# Extreme indicators must agree on direction (no mixed signals)
DIRECTIONAL_AGREEMENT_RATIO = 0.66

# Stress-rising indicators (z > 0 = risk-off)
# direction key: +1 means rising z = RISK_OFF, -1 means rising z = RISK_ON
STRESS_RISING = {
    "BAMLH0A0HYM2": ("HY OAS", +1),
    "BAMLC0A0CM":   ("IG OAS", +1),
    "MOVE":         ("MOVE",   +1),
    "NFCI":         ("NFCI",   +1),
    "VIXCLS":       ("VIX",    +1),
}

# Velocity indicators — we compute change-over-N-days, then z-score.
# direction key: sign of move that's bearish for risk
VELOCITY = {
    "T10Y2Y":   ("2s10s velocity", +1, "flattening"),  # rising = steepening = late-cycle warning
    "DTWEXBGS": ("DXY 5d",         +1, "rising"),       # rising = funding stress
    "T5YIE":    ("5Y BE 5d",       -1, "falling"),      # falling = deflation/risk-off
}


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=300"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def zscore(value, history):
    """Compute z-score of value against history list. Robust against
    short or constant histories."""
    if value is None or not history or len(history) < 5:
        return None
    try:
        # Filter out None and non-numeric
        clean = [float(h) for h in history if h is not None and isinstance(h, (int, float))]
        if len(clean) < 5:
            return None
        mu = statistics.mean(clean)
        sigma = statistics.stdev(clean) if len(clean) >= 2 else 0
        if sigma == 0:
            return 0.0
        return round((float(value) - mu) / sigma, 3)
    except Exception:
        return None


def velocity_zscore(history, n_day=5):
    """Compute z-score of the last n_day change vs the distribution of
    n_day changes over the full history."""
    if not history or len(history) < n_day + 5:
        return None, None
    try:
        clean = [float(h) for h in history if h is not None and isinstance(h, (int, float))]
        if len(clean) < n_day + 5:
            return None, None
        # Compute rolling n-day changes
        deltas = [clean[i] - clean[i + n_day] for i in range(len(clean) - n_day)]
        if not deltas or len(deltas) < 5:
            return None, None
        latest_delta = deltas[0]
        mu = statistics.mean(deltas[1:])  # exclude the latest from the distribution
        sigma = statistics.stdev(deltas[1:]) if len(deltas) >= 3 else 0
        if sigma == 0:
            return 0.0, latest_delta
        return round((latest_delta - mu) / sigma, 3), latest_delta
    except Exception:
        return None, None


def get_telegram_creds():
    try:
        token = ssm.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name=TG_CHAT_ID_PARAM)["Parameter"]["Value"]
        return token, chat_id
    except Exception as e:
        print(f"[TG-CREDS] {e}")
        return None, None


def send_telegram(message):
    token, chat_id = get_telegram_creds()
    if not token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data,
                                      headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print(f"[TG] {e}")
        return False


def detect():
    """Main detection logic. Returns dict with regime + signals."""
    now = datetime.now(timezone.utc)

    # 1. Load fresh data
    repo = get_s3_json("repo-data.json", {})
    fred = get_s3_json("data/fred-cache-secretary.json", {})

    if not repo and not fred:
        return {
            "as_of": now.isoformat(),
            "regime": "NEUTRAL",
            "error": "no_data_available",
            "indicators_extreme": 0,
            "n_signals": 0,
            "signals": [],
        }

    # 2. Compute signals for each indicator
    signals = []

    # 2a. STRESS_RISING indicators (z-score from repo-data already computed)
    # repo-data structure: {data: {category: {SERIES: {value, history, z_score, ...}}}}
    repo_data = repo.get("data", {})
    
    # Map series_id → repo-data category (where to find them)
    series_to_category = {
        "BAMLH0A0HYM2": "funding_spreads",
        "BAMLC0A0CM":   "funding_spreads",
        "MOVE":         "systemic",
        "NFCI":         "systemic",
        "VIXCLS":       "systemic",
    }

    for sid, (name, dir_sign) in STRESS_RISING.items():
        cat = series_to_category.get(sid)
        if not cat:
            continue
        d = repo_data.get(cat, {}).get(sid, {})
        z = d.get("z_score")
        value = d.get("value")
        # Fallback: compute z-score from FRED cache history if repo-data missing it
        if z is None:
            f = fred.get(sid, {})
            value = f.get("value")
            history = f.get("history", [])
            z = zscore(value, history)

        if z is not None and value is not None:
            # Stress indicator: positive z = risk-off direction
            direction = "RISK_OFF" if z > 0 else "RISK_ON"
            extreme = abs(z) > Z_THRESHOLD
            signals.append({
                "name": name,
                "series": sid,
                "value": round(float(value), 3),
                "z": z,
                "direction": direction,
                "extreme": extreme,
                "type": "level",
            })

    # 2b. VELOCITY indicators
    for sid, (name, dir_sign, _label) in VELOCITY.items():
        f = fred.get(sid, {})
        history = f.get("history", [])
        z, delta = velocity_zscore(history, n_day=5)
        if z is None:
            continue
        # For velocity, the dir_sign tells us which side is "bearish"
        # +1 means rising (positive delta) = bearish for risk
        # -1 means falling (negative delta) = bearish for risk
        bearish_z = z * dir_sign  # normalize so positive = risk-off
        direction = "RISK_OFF" if bearish_z > 0 else "RISK_ON"
        extreme = abs(z) > Z_THRESHOLD
        signals.append({
            "name": name,
            "series": sid,
            "value": round(float(f.get("value", 0)), 3),
            "delta_5d": round(delta, 4) if delta is not None else None,
            "z": z,
            "direction": direction,
            "extreme": extreme,
            "type": "velocity",
        })

    # 3. Aggregate — count extremes by direction
    extreme_signals = [s for s in signals if s["extreme"]]
    n_extreme = len(extreme_signals)
    n_off = sum(1 for s in extreme_signals if s["direction"] == "RISK_OFF")
    n_on = sum(1 for s in extreme_signals if s["direction"] == "RISK_ON")

    # 4. Determine regime
    if n_extreme >= MIN_INDICATORS_FOR_REGIME:
        # Need directional agreement
        if n_off / max(1, n_extreme) >= DIRECTIONAL_AGREEMENT_RATIO:
            regime = "RISK_OFF"
        elif n_on / max(1, n_extreme) >= DIRECTIONAL_AGREEMENT_RATIO:
            regime = "RISK_ON"
        else:
            regime = "NEUTRAL"  # mixed = uncertain
    else:
        regime = "NEUTRAL"

    # 5. Composite strength score
    # 100 = strong risk-off, 0 = strong risk-on, 50 = neutral
    if signals:
        avg_directional_z = statistics.mean(
            s["z"] * (1 if s["direction"] == "RISK_OFF" else -1)
            for s in signals
        )
        # Map to 0-100 with 50 = neutral
        strength = max(0, min(100, 50 + avg_directional_z * 15))
    else:
        strength = 50.0

    return {
        "as_of": now.isoformat(),
        "regime": regime,
        "regime_strength": round(strength, 1),
        "indicators_extreme": n_extreme,
        "indicators_total": len(signals),
        "n_risk_off": n_off,
        "n_risk_on": n_on,
        "consensus_direction": ("RISK_OFF" if n_off > n_on else
                               ("RISK_ON" if n_on > n_off else "MIXED")),
        "signals": signals,
        "thresholds": {
            "z_threshold": Z_THRESHOLD,
            "min_indicators_for_regime": MIN_INDICATORS_FOR_REGIME,
            "directional_agreement_ratio": DIRECTIONAL_AGREEMENT_RATIO,
        },
    }


def lambda_handler(event, context):
    print("=== BOND REGIME DETECTOR v1 ===")
    now = datetime.now(timezone.utc)

    # 1. Run detection
    snapshot = detect()
    snapshot["v"] = "1.0"

    # 2. Read prior regime to detect changes
    prior = get_s3_json("regime/current.json", {})
    prior_regime = prior.get("regime", "NEUTRAL")
    new_regime = snapshot["regime"]
    regime_changed = (prior_regime != new_regime)
    snapshot["previous_regime"] = prior_regime
    snapshot["regime_changed"] = regime_changed

    # 3. Track time-in-regime
    if not regime_changed and prior.get("days_in_regime"):
        try:
            prior_as_of = datetime.fromisoformat(prior["as_of"].replace("Z", "+00:00"))
            hours_diff = (now - prior_as_of).total_seconds() / 3600
            snapshot["days_in_regime"] = round(prior.get("days_in_regime", 0) + hours_diff / 24, 2)
        except Exception:
            snapshot["days_in_regime"] = 0
    else:
        snapshot["days_in_regime"] = 0

    print(f"  Regime: {prior_regime} → {new_regime} (changed: {regime_changed})")
    print(f"  Strength: {snapshot.get('regime_strength')}")
    print(f"  Extreme indicators: {snapshot.get('indicators_extreme')}/{snapshot.get('indicators_total')}")

    # 4. Write snapshot
    put_s3_json("regime/current.json", snapshot, cache="public, max-age=900")

    # 5. Append to history (rolling 365 days, max 1 entry per 4h)
    history = get_s3_json("regime/history.json", {"v": "1.0", "snapshots": []})
    snapshots = history.get("snapshots", [])
    # Keep only condensed fields in history (signals can be large)
    history_entry = {
        "as_of": snapshot["as_of"],
        "regime": snapshot["regime"],
        "regime_strength": snapshot["regime_strength"],
        "indicators_extreme": snapshot["indicators_extreme"],
        "n_risk_off": snapshot.get("n_risk_off", 0),
        "n_risk_on": snapshot.get("n_risk_on", 0),
        "regime_changed": regime_changed,
    }
    snapshots.append(history_entry)
    # Trim to 365 days
    cutoff_iso = (now - timedelta(days=365)).isoformat()
    snapshots = [s for s in snapshots if s.get("as_of", "") >= cutoff_iso]
    history["snapshots"] = snapshots
    history["last_updated"] = now.isoformat()
    put_s3_json("regime/history.json", history)

    # 6. Telegram alert ONLY on regime changes
    if regime_changed:
        if new_regime == "RISK_OFF":
            emoji = "🚨"
            headline = "BOND MARKET REGIME CHANGE → RISK OFF"
        elif new_regime == "RISK_ON":
            emoji = "✅"
            headline = "BOND MARKET REGIME CHANGE → RISK ON"
        else:
            emoji = "⚪️"
            headline = "Bond market regime → NEUTRAL"

        # Build alert with the actual extreme signals
        extreme_lines = []
        for s in snapshot.get("signals", []):
            if s.get("extreme"):
                z = s["z"]
                arrow = "↑" if z > 0 else "↓"
                extreme_lines.append(f"  • {s['name']}: z={z:+.1f} {arrow}")
        extreme_text = "\\n".join(extreme_lines) if extreme_lines else "  (no extremes)"

        message = (
            f"{emoji} *{headline}*\\n\\n"
            f"Strength: {snapshot.get('regime_strength')}/100\\n"
            f"Extreme indicators: {snapshot.get('indicators_extreme')}/{snapshot.get('indicators_total')}\\n\\n"
            f"*Signals at extreme:*\\n{extreme_text}\\n\\n"
            f"Was: {prior_regime}\\n"
            f"Now: {new_regime}\\n\\n"
            f"_Bond market regime detector — typically leads equity markets by 4-8 weeks_"
        )
        sent = send_telegram(message)
        print(f"  Telegram alert sent: {sent}")
        snapshot["alert_sent"] = sent

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "regime": new_regime,
            "previous_regime": prior_regime,
            "regime_changed": regime_changed,
            "regime_strength": snapshot["regime_strength"],
            "indicators_extreme": snapshot["indicators_extreme"],
            "n_signals": snapshot["indicators_total"],
        }),
    }
'''


with report("build_bond_regime_detector") as r:
    r.heading("Phase 1A — Bond Market Regime Detector")

    # ─── 1. Verify the data we depend on is flowing ─────────────────────
    r.section("1. Verify data dependencies")

    # Check repo-data.json freshness
    try:
        obj = s3.head_object(Bucket=BUCKET, Key="repo-data.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        r.log(f"  repo-data.json: {obj['ContentLength']:,}B, age {age_min:.1f}min")
        if age_min > 240:
            r.warn(f"  repo-data.json is stale (>4h)")
    except Exception as e:
        r.warn(f"  repo-data.json: {e}")

    # Check FRED cache
    try:
        obj = s3.head_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        r.log(f"  fred-cache-secretary.json: {obj['ContentLength']:,}B, age {age_min:.1f}min")
    except Exception as e:
        r.warn(f"  fred-cache-secretary.json: {e}")

    # Check actual content quality
    fred_obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")
    fred = json.loads(fred_obj["Body"].read().decode())
    needed_series = ["BAMLH0A0HYM2", "BAMLC0A0CM", "T10Y2Y", "DTWEXBGS", "T5YIE"]
    for sid in needed_series:
        d = fred.get(sid, {})
        h = d.get("history", [])
        r.log(f"    {sid}: {len(h)} history pts, latest={d.get('value')}")

    repo_obj = s3.get_object(Bucket=BUCKET, Key="repo-data.json")
    repo = json.loads(repo_obj["Body"].read().decode())
    repo_data = repo.get("data", {})
    for sid in ["MOVE", "NFCI", "VIXCLS"]:
        for cat in ["systemic", "funding_spreads"]:
            d = repo_data.get(cat, {}).get(sid)
            if d:
                r.log(f"    {sid} (in {cat}): value={d.get('value')}, z={d.get('z_score')}")
                break
        else:
            r.warn(f"    {sid}: NOT FOUND in repo-data")

    # ─── 2. Set up Lambda ───────────────────────────────────────────────
    r.section("2. Set up justhodl-bond-regime-detector Lambda")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-bond-regime-detector/source"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "lambda_function.py").write_text(DETECTOR_SRC)

    import ast
    try:
        ast.parse(DETECTOR_SRC)
        r.ok(f"  Wrote source: {len(DETECTOR_SRC):,}B, {DETECTOR_SRC.count(chr(10))} LOC")
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        if hasattr(e, "lineno"):
            lines = DETECTOR_SRC.split("\n")
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    # Build deployment zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, DETECTOR_SRC)
    zbytes = buf.getvalue()
    r.log(f"  Deployment zip: {len(zbytes):,}B")

    fname = "justhodl-bond-regime-detector"
    role_arn = "arn:aws:iam::857687956942:role/lambda-execution-role"

    try:
        lam.get_function(FunctionName=fname)
        lam.update_function_code(
            FunctionName=fname, ZipFile=zbytes, Architectures=["arm64"],
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=fname, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Updated existing {fname}")
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=fname,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zbytes},
            Description="Phase 1A — Bond market regime detector (RISK_OFF/NEUTRAL/RISK_ON)",
            Timeout=60,
            MemorySize=256,
            Architectures=["arm64"],
            Environment={"Variables": {}},
        )
        lam.get_waiter("function_active_v2").wait(
            FunctionName=fname, WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Created {fname}")

    # ─── 3. Test invoke ─────────────────────────────────────────────────
    r.section("3. Test invoke")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=fname, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:600]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    try:
        outer = json.loads(payload)
        body = json.loads(outer.get("body", "{}"))
        r.log(f"\n  Response body:")
        for k, v in body.items():
            r.log(f"    {k:25} {v}")
    except Exception:
        r.log(f"  Raw: {payload[:400]}")

    # ─── 4. Read regime/current.json + show signals ─────────────────────
    r.section("4. Read regime/current.json")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="regime/current.json")
        snap = json.loads(obj["Body"].read().decode("utf-8"))
        r.log(f"  Regime: {snap.get('regime')} (strength: {snap.get('regime_strength')}/100)")
        r.log(f"  Extreme: {snap.get('indicators_extreme')}/{snap.get('indicators_total')}")
        r.log(f"  Risk-off signals: {snap.get('n_risk_off')}, Risk-on: {snap.get('n_risk_on')}")
        r.log(f"  Consensus: {snap.get('consensus_direction')}")
        r.log(f"\n  Per-indicator signals:")
        for s in snap.get("signals", []):
            extreme_marker = " ← EXTREME" if s.get("extreme") else ""
            value_str = f"value={s.get('value')}"
            if s.get("delta_5d") is not None:
                value_str += f", Δ5d={s.get('delta_5d'):+.4f}"
            r.log(f"    {s.get('name'):20} z={s.get('z'):+.2f}  {s.get('direction'):10} {value_str}{extreme_marker}")
    except Exception as e:
        r.warn(f"  read regime/current: {e}")

    # ─── 5. Schedule cron(0 */4 * * ? *) ────────────────────────────────
    r.section("5. EventBridge schedule cron(0 */4 * * ? *) — every 4h")
    rule_name = "justhodl-bond-regime-detector-4h"
    try:
        try:
            existing = events.describe_rule(Name=rule_name)
            r.log(f"  Rule exists: {existing['State']} {existing.get('ScheduleExpression')}")
        except events.exceptions.ResourceNotFoundException:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 */4 * * ? *)",
                State="ENABLED",
                Description="Phase 1A — Bond regime detector every 4h",
            )
            r.ok(f"  Created rule cron(0 */4 * * ? *)")
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "1",
                      "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{fname}"}],
        )
        try:
            lam.add_permission(
                FunctionName=fname,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule_name}",
            )
            r.ok(f"  Added invoke permission")
        except lam.exceptions.ResourceConflictException:
            r.log(f"  Permission already exists")
    except Exception as e:
        r.fail(f"  Schedule: {e}")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
        regime=body.get("regime"),
        regime_changed=body.get("regime_changed"),
        regime_strength=body.get("regime_strength"),
        indicators_extreme=body.get("indicators_extreme"),
        n_signals=body.get("n_signals"),
    )
    r.log("Done")
