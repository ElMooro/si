#!/usr/bin/env python3
"""
Step 144 — Phase 1B: Cross-asset divergence scanner.

Phase 2 of the hedge-fund-grade risk system. Watches 12 economically
meaningful cross-asset relationships and flags when one is >2σ from
its 12-month historical relationship — i.e. when one side of the
pair is mispriced relative to the other.

KEY DESIGN DECISIONS:

  1. ECONOMICALLY MEANINGFUL ONLY: 12 pairs where there's a mechanical
     or behavioral reason they SHOULD track each other. Random
     correlations of "SPY vs whatever" produce noise; we exclude those.

  2. RESIDUALS, NOT CORRELATIONS: rolling correlation tells you the
     relationship is unstable. What we want: "asset A is X% rich/cheap
     relative to historical relationship with B."
     Method: regress A on B over 252-day window; today's residual
     vs the residual distribution = z-score.

  3. NO RE-FLAGGING WHAT BOND DETECTOR HANDLES: the bond regime
     detector watches HY/IG/MOVE/NFCI/VIX directly. The divergence
     scanner handles CROSS-asset relationships (one risk asset vs
     another, etc) — different layer of information.

  4. GRACEFUL DEGRADATION: skip any pair where data is missing or
     short, don't false-fail the whole scanner.

  5. WEEKLY REPORT, NOT ALERTS: divergences typically resolve in
     2-8 weeks, so a Telegram firehose would be noise. Output is a
     ranked S3 file consumed via dashboard. Telegram alerts only for
     EXTREME (|z| > 3) cases.

THE 12 RELATIONSHIPS:

  Mechanical (driven by structural arbitrage):
    1. Gold vs real rates (GLD ~ DGS10-T10YIE)
    2. Small caps vs yield curve (IWM ~ T10Y2Y)
    3. EM vs dollar (EEM ~ -DTWEXBGS)
    4. Growth vs long rates (QQQ ~ -DGS10)
    5. Banks vs yield curve (XLF ~ T10Y2Y)

  Behavioral (positioning/flow):
    6. VIX vs HY (VIXCLS ~ BAMLH0A0HYM2)
    7. BTC vs Nasdaq (IBIT ~ QQQ)
    8. Gold vs BTC (GLD ~ IBIT)
    9. Defensives vs cyclicals (XLP/XLY ratio)

  Information-flow:
    10. Energy vs breakevens (XLE ~ T5YIE)
    11. TIPS vs nominal (TIP ~ IEF, ratio)
    12. Healthcare vs market (XLV ~ SPY) — defensive beta check

OUTPUT:
  s3://justhodl-dashboard-live/divergence/current.json
  s3://justhodl-dashboard-live/divergence/history.json (rolling 90d)
  Telegram alert ONLY for |z| > 3.0 (rare; ~1-2 per quarter)

SCHEDULE: cron(0 13 * * MON-FRI *) — daily at 13:00 UTC (post-EU close)
LAMBDA: justhodl-divergence-scanner (256MB arm64, 60s)

DATA DEPENDENCIES (already flowing):
  - data/report.json (stocks.<TICKER>.history with 120 daily bars)
  - data/fred-cache-secretary.json (FRED series with 30-60d history)
"""
import io
import json
import os
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

BUCKET = "justhodl-dashboard-live"


# ════════════════════════════════════════════════════════════════════════
# Source for the new Lambda
# ════════════════════════════════════════════════════════════════════════
SCANNER_SRC = '''"""
justhodl-divergence-scanner — Phase 1B cross-asset divergence detector.

Reads data/report.json (ETF history) + data/fred-cache-secretary.json
(FRED series). For each of 12 economically meaningful relationships,
computes a rolling 60-day regression residual z-score. Flags pairs
where today\\'s residual is >2σ from its own distribution.

Writes divergence/current.json + divergence/history.json. Sends
Telegram alert ONLY for extreme |z|>3.0 dislocations.
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


# Use a 60-day rolling window for residuals. Long enough for stable
# regression coefficients, short enough to adapt to regime changes.
ROLLING_WINDOW = 60
# Z-score threshold for "this is genuinely divergent"
Z_THRESHOLD = 2.0
# Above this, send a Telegram alert (rare events only)
EXTREME_Z_THRESHOLD = 3.0


# Each relationship: (id, name, asset_a_path, asset_b_path, expected_sign, description)
# Path tuples: (source, key) where source is "stocks" or "fred"
# expected_sign: +1 if A and B move together, -1 if inversely
RELATIONSHIPS = [
    # Mechanical
    ("gold_real_rates", "Gold vs Real Rates",
     ("stocks", "GLD"), ("synthetic", "real_rate_10y"), -1,
     "Gold should fall when real rates rise (opportunity cost)"),
    ("smallcap_curve", "Small Caps vs 2s10s Curve",
     ("stocks", "IWM"), ("fred", "T10Y2Y"), +1,
     "Small caps benefit from steepening curve (bank NIMs)"),
    ("em_dollar", "EM vs Dollar",
     ("stocks", "EEM"), ("fred", "DTWEXBGS"), -1,
     "Strong dollar hurts EM debt service + commodities"),
    ("nasdaq_long_rates", "Nasdaq vs 10Y Yield",
     ("stocks", "QQQ"), ("fred", "DGS10"), -1,
     "Rising 10Y compresses growth multiples"),
    ("banks_curve", "Banks vs 2s10s Curve",
     ("stocks", "XLF"), ("fred", "T10Y2Y"), +1,
     "Banks profit on the curve spread"),

    # Behavioral
    ("vix_hy", "VIX vs HY OAS",
     ("fred", "VIXCLS"), ("fred", "BAMLH0A0HYM2"), +1,
     "Both measure risk aversion; HY usually leads"),
    ("btc_nasdaq", "BTC vs Nasdaq",
     ("stocks", "IBIT"), ("stocks", "QQQ"), +1,
     "BTC trades as high-beta tech proxy"),
    ("gold_btc", "Gold vs BTC",
     ("stocks", "GLD"), ("stocks", "IBIT"), +1,
     "Both inflation hedges; divergence = narrative shift"),
    ("defensives_cyclicals", "XLP vs XLY (defensive/cyclical)",
     ("stocks", "XLP"), ("stocks", "XLY"), +1,
     "Risk appetite rotation indicator"),

    # Information-flow
    ("energy_breakevens", "Energy vs 5Y Breakevens",
     ("stocks", "XLE"), ("fred", "T5YIE"), +1,
     "Oil drives near-term inflation expectations"),
    ("tips_nominal", "TIP vs IEF",
     ("stocks", "TIP"), ("stocks", "IEF"), +1,
     "Divergence flags inflation regime change"),
    ("healthcare_market", "XLV vs SPY",
     ("stocks", "XLV"), ("stocks", "SPY"), +1,
     "Healthcare defensive beta check"),
]


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=600"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def extract_history(report, fred, source, key):
    """Extract a list of daily values for an asset.
    Returns list of floats (most recent first) or None if missing."""
    if source == "stocks":
        s = report.get("stocks", {}).get(key, {})
        history = s.get("history", [])
        if not history:
            return None
        # history shape: [{'d': '2025-04-01', 'c': 503.21}, ...]
        return [float(h["c"]) for h in history if isinstance(h, dict) and "c" in h]
    elif source == "fred":
        f = fred.get(key, {})
        history = f.get("history", [])
        if not history:
            return None
        return [float(h) for h in history if h is not None and isinstance(h, (int, float))]
    elif source == "synthetic":
        # Special case: 10Y real rate = DGS10 - T10YIE
        if key == "real_rate_10y":
            d10 = fred.get("DGS10", {}).get("history", [])
            be10 = fred.get("T10YIE", {}).get("history", [])
            if not d10 or not be10:
                return None
            n = min(len(d10), len(be10))
            return [float(d10[i]) - float(be10[i]) for i in range(n)
                    if d10[i] is not None and be10[i] is not None]
        return None
    return None


def regress_residual_z(a_history, b_history, expected_sign):
    """Compute z-score of today\\'s residual from a 60-day OLS regression of a on b.

    Returns (z_score, today_residual, slope, intercept, r_squared) or
    (None, ...) if there\\'s insufficient data.
    """
    if not a_history or not b_history:
        return None, None, None, None, None
    n = min(len(a_history), len(b_history), ROLLING_WINDOW)
    if n < 30:
        return None, None, None, None, None

    # Take the most-recent n points from each
    a = a_history[:n]
    b = b_history[:n]

    # Compute means and deviations
    a_mean = statistics.mean(a)
    b_mean = statistics.mean(b)
    cov = sum((a[i] - a_mean) * (b[i] - b_mean) for i in range(n)) / n
    b_var = sum((b[i] - b_mean) ** 2 for i in range(n)) / n
    a_var = sum((a[i] - a_mean) ** 2 for i in range(n)) / n

    if b_var == 0 or a_var == 0:
        return None, None, None, None, None

    # OLS slope and intercept
    slope = cov / b_var
    intercept = a_mean - slope * b_mean

    # Compute R² (just for reporting)
    r_sq = (cov ** 2) / (a_var * b_var) if (a_var * b_var) > 0 else 0

    # Sanity check: if expected_sign is -1, slope should be negative
    # (and vice versa). If the ACTUAL slope is opposite to expected,
    # the relationship has broken down at multi-month scale —
    # interesting but not what we\\'re measuring here.
    # We compute z-score regardless; it\\'s informative either way.

    # Compute residuals over the window
    residuals = [a[i] - (slope * b[i] + intercept) for i in range(n)]
    today_residual = residuals[0]  # most recent
    historical_residuals = residuals[1:]  # exclude today from the distribution

    if len(historical_residuals) < 10:
        return None, today_residual, slope, intercept, r_sq

    res_mean = statistics.mean(historical_residuals)
    res_std = statistics.stdev(historical_residuals) if len(historical_residuals) >= 2 else 0
    if res_std == 0:
        return 0.0, today_residual, slope, intercept, r_sq

    z = (today_residual - res_mean) / res_std
    return round(z, 3), round(today_residual, 4), round(slope, 6), round(intercept, 4), round(r_sq, 3)


def get_telegram_creds():
    try:
        token = ssm.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name=TG_CHAT_ID_PARAM)["Parameter"]["Value"]
        return token, chat_id
    except Exception as e:
        print(f"[TG] {e}")
        return None, None


def send_telegram(message):
    token, chat_id = get_telegram_creds()
    if not token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id, "text": message, "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data,
                                      headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception as e:
        print(f"[TG] {e}")
        return False


def lambda_handler(event, context):
    print("=== DIVERGENCE SCANNER v1 ===")
    now = datetime.now(timezone.utc)

    # 1. Load data
    rpt = get_s3_json("data/report.json", {})
    fred = get_s3_json("data/fred-cache-secretary.json", {})

    if not rpt and not fred:
        return {"statusCode": 500, "body": json.dumps({"error": "no_data"})}

    print(f"  report.json: {len(rpt.get('stocks', {}))} stocks, {len(rpt.get('fred', {}))} FRED")
    print(f"  fred-cache: {len(fred)} series")

    # 2. Process each relationship
    results = []
    extreme_count = 0
    for rel_id, name, src_a, src_b, expected_sign, desc in RELATIONSHIPS:
        a_hist = extract_history(rpt, fred, src_a[0], src_a[1])
        b_hist = extract_history(rpt, fred, src_b[0], src_b[1])

        if not a_hist or not b_hist:
            results.append({
                "id": rel_id, "name": name, "description": desc,
                "status": "missing_data",
                "asset_a": f"{src_a[0]}:{src_a[1]}",
                "asset_b": f"{src_b[0]}:{src_b[1]}",
                "a_len": len(a_hist) if a_hist else 0,
                "b_len": len(b_hist) if b_hist else 0,
            })
            continue

        z, today_res, slope, intercept, r_sq = regress_residual_z(a_hist, b_hist, expected_sign)

        if z is None:
            results.append({
                "id": rel_id, "name": name, "description": desc,
                "status": "insufficient_data",
                "a_len": len(a_hist), "b_len": len(b_hist),
            })
            continue

        # Determine direction of mispricing
        # Positive residual = A is HIGHER than the regression predicts
        # → either A is rich, or B will catch up (move toward A\\'s level)
        if z > 0:
            mispricing = f"{src_a[1]} appears RICH vs {src_b[1]}"
        elif z < 0:
            mispricing = f"{src_a[1]} appears CHEAP vs {src_b[1]}"
        else:
            mispricing = f"{src_a[1]} fairly priced vs {src_b[1]}"

        is_extreme = abs(z) > Z_THRESHOLD
        is_alert = abs(z) > EXTREME_Z_THRESHOLD
        if is_alert:
            extreme_count += 1

        # Latest values for context
        a_now = round(a_hist[0], 4)
        b_now = round(b_hist[0], 4)

        results.append({
            "id": rel_id, "name": name, "description": desc,
            "status": "ok",
            "z_score": z,
            "extreme": is_extreme,
            "alert_worthy": is_alert,
            "mispricing": mispricing,
            "asset_a": f"{src_a[0]}:{src_a[1]}",
            "asset_b": f"{src_b[0]}:{src_b[1]}",
            "a_value": a_now,
            "b_value": b_now,
            "today_residual": today_res,
            "slope": slope,
            "intercept": intercept,
            "r_squared": r_sq,
            "expected_sign": expected_sign,
            "actual_sign": +1 if slope > 0 else -1,
            "relationship_intact": (slope > 0) == (expected_sign > 0),
            "window_days": ROLLING_WINDOW,
        })

    # 3. Sort by absolute z-score descending (most divergent first)
    sorted_results = sorted(results, key=lambda r: abs(r.get("z_score") or 0), reverse=True)

    # 4. Build snapshot
    n_processed = sum(1 for r in results if r.get("status") == "ok")
    n_extreme = sum(1 for r in results if r.get("extreme"))
    n_alert = sum(1 for r in results if r.get("alert_worthy"))

    snapshot = {
        "as_of": now.isoformat(),
        "v": "1.0",
        "summary": {
            "n_relationships_total": len(RELATIONSHIPS),
            "n_processed": n_processed,
            "n_missing_data": len(RELATIONSHIPS) - n_processed,
            "n_extreme": n_extreme,
            "n_alert_worthy": n_alert,
        },
        "relationships": sorted_results,
        "thresholds": {
            "z_threshold": Z_THRESHOLD,
            "extreme_threshold": EXTREME_Z_THRESHOLD,
            "rolling_window_days": ROLLING_WINDOW,
        },
    }

    # 5. Write outputs
    put_s3_json("divergence/current.json", snapshot, cache="public, max-age=900")

    # 6. Append condensed entry to history (rolling 90 days)
    history = get_s3_json("divergence/history.json", {"v": "1.0", "snapshots": []})
    snapshots = history.get("snapshots", [])
    history_entry = {
        "as_of": snapshot["as_of"],
        "n_extreme": n_extreme,
        "n_alert": n_alert,
        "extremes": [
            {"id": r["id"], "name": r["name"], "z": r["z_score"], "mispricing": r.get("mispricing")}
            for r in sorted_results if r.get("extreme")
        ][:10],  # top 10 only
    }
    snapshots.append(history_entry)
    cutoff = (now - timedelta(days=90)).isoformat()
    snapshots = [s for s in snapshots if s.get("as_of", "") >= cutoff]
    history["snapshots"] = snapshots
    history["last_updated"] = now.isoformat()
    put_s3_json("divergence/history.json", history)

    print(f"  {n_processed}/{len(RELATIONSHIPS)} processed")
    print(f"  {n_extreme} at >2σ extreme")
    print(f"  {n_alert} at >3σ (alert-worthy)")

    # 7. Telegram alert ONLY for >3σ events
    alerts = [r for r in sorted_results if r.get("alert_worthy")]
    if alerts:
        lines = ["🚨 *Cross-Asset Divergence Alert*\\n"]
        for a in alerts[:5]:
            arrow = "↑" if a["z_score"] > 0 else "↓"
            lines.append(
                f"• *{a['name']}*: z={a['z_score']:+.1f} {arrow}\\n"
                f"  {a['mispricing']}\\n"
                f"  _Typically resolves in 2-8 weeks_\\n"
            )
        message = "\\n".join(lines) + "\\n_Cross-asset scanner — pairs >3σ from historical relationship_"
        sent = send_telegram(message)
        snapshot["alert_sent"] = sent
        print(f"  Telegram alert sent: {sent}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "n_processed": n_processed,
            "n_extreme": n_extreme,
            "n_alert": n_alert,
            "top_divergence": sorted_results[0].get("name") if sorted_results else None,
            "top_z": sorted_results[0].get("z_score") if sorted_results else None,
        }),
    }
'''


with report("build_divergence_scanner") as r:
    r.heading("Phase 1B — Cross-Asset Divergence Scanner")

    # ─── 1. Verify data dependencies ────────────────────────────────────
    r.section("1. Verify data dependencies")

    try:
        obj = s3.head_object(Bucket=BUCKET, Key="data/report.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        r.log(f"  data/report.json: {obj['ContentLength']:,}B, age {age_min:.1f}min")
    except Exception as e:
        r.warn(f"  data/report.json: {e}")

    try:
        obj = s3.head_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")
        age_min = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
        r.log(f"  data/fred-cache-secretary.json: {obj['ContentLength']:,}B, age {age_min:.1f}min")
    except Exception as e:
        r.warn(f"  data/fred-cache-secretary.json: {e}")

    # Quick sample: do we have history for the tickers we care about?
    rpt_obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
    rpt = json.loads(rpt_obj["Body"].read().decode())
    stocks = rpt.get("stocks", {})
    needed_tickers = ["GLD", "IWM", "EEM", "QQQ", "XLF", "IBIT", "XLP",
                      "XLY", "XLE", "TIP", "IEF", "XLV", "SPY"]
    r.log(f"\n  Stock history depth check:")
    for tk in needed_tickers:
        s = stocks.get(tk, {})
        h = s.get("history", [])
        marker = " ← short" if len(h) < 60 else ""
        r.log(f"    {tk:6} {len(h)} bars{marker}")

    # FRED check
    fred_obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")
    fred = json.loads(fred_obj["Body"].read().decode())
    needed_fred = ["DGS10", "T10YIE", "T10Y2Y", "DTWEXBGS", "T5YIE",
                   "VIXCLS", "BAMLH0A0HYM2"]
    r.log(f"\n  FRED history depth check:")
    for sid in needed_fred:
        d = fred.get(sid, {})
        h = d.get("history", [])
        marker = " ← short" if len(h) < 30 else ""
        r.log(f"    {sid:15} {len(h)} pts{marker}")

    # ─── 2. Set up Lambda ───────────────────────────────────────────────
    r.section("2. Set up justhodl-divergence-scanner Lambda")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-divergence-scanner/source"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "lambda_function.py").write_text(SCANNER_SRC)

    import ast
    try:
        ast.parse(SCANNER_SRC)
        r.ok(f"  Wrote source: {len(SCANNER_SRC):,}B, {SCANNER_SRC.count(chr(10))} LOC")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        if hasattr(e, "lineno"):
            lines = SCANNER_SRC.split("\n")
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, SCANNER_SRC)
    zbytes = buf.getvalue()

    fname = "justhodl-divergence-scanner"
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
            Description="Phase 1B — Cross-asset divergence scanner (12 macro pairs)",
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

    # ─── 4. Read divergence/current.json + show all pairs ───────────────
    r.section("4. divergence/current.json — full report")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="divergence/current.json")
        snap = json.loads(obj["Body"].read().decode("utf-8"))
        s = snap.get("summary", {})
        r.log(f"  Processed: {s.get('n_processed')}/{s.get('n_relationships_total')}")
        r.log(f"  Missing data: {s.get('n_missing_data')}")
        r.log(f"  At >2σ extreme: {s.get('n_extreme')}")
        r.log(f"  At >3σ alert-worthy: {s.get('n_alert_worthy')}")
        r.log(f"\n  All relationships (sorted by |z|):")
        for rel in snap.get("relationships", []):
            if rel.get("status") != "ok":
                r.log(f"    {rel['name']:35} {rel.get('status')}")
                continue
            z = rel.get("z_score") or 0
            extreme_marker = " ← EXTREME" if rel.get("extreme") else ""
            alert_marker = " ← ALERT" if rel.get("alert_worthy") else ""
            relat_intact = "✓" if rel.get("relationship_intact") else "⚠"
            r.log(f"    {rel['name']:35} z={z:+.2f} R²={rel.get('r_squared'):.2f}  "
                  f"slope={relat_intact}{extreme_marker}{alert_marker}")
            r.log(f"      → {rel.get('mispricing')}")
    except Exception as e:
        r.warn(f"  read divergence/current: {e}")

    # ─── 5. Schedule — daily 13:00 UTC weekdays ─────────────────────────
    r.section("5. EventBridge schedule cron(0 13 ? * MON-FRI *)")
    rule_name = "justhodl-divergence-scanner-daily"
    try:
        try:
            existing = events.describe_rule(Name=rule_name)
            r.log(f"  Rule exists: {existing['State']} {existing.get('ScheduleExpression')}")
        except events.exceptions.ResourceNotFoundException:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 13 ? * MON-FRI *)",
                State="ENABLED",
                Description="Phase 1B — daily cross-asset divergence scan post-EU-close",
            )
            r.ok(f"  Created rule cron(0 13 ? * MON-FRI *)")
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
        n_processed=body.get("n_processed"),
        n_extreme=body.get("n_extreme"),
        n_alert=body.get("n_alert"),
        top_divergence=body.get("top_divergence"),
        top_z=body.get("top_z"),
    )
    r.log("Done")
