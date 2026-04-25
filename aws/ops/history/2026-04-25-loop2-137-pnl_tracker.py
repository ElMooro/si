#!/usr/bin/env python3
"""
Step 137 — Loop 2A: Portfolio state + PnL tracker.

Loop 2's purpose: convert each prediction the system makes into a
TRACKED hypothetical decision. So at any time you can answer "if I'd
followed JustHodl.AI for the last 90 days, what would my returns look
like vs SPY?"

CRITICAL: This is a TRACKING layer, not a trading layer. We never
execute trades. We never auto-rebalance. We never integrate with a
broker. We just compute and record what WOULD have happened. The
output is informational.

Scope (minimum viable):

A. portfolio/state.json — your starting allocation
   {
     "as_of": "2026-04-25",
     "starting_value_usd": 100000,
     "allocations": {
       "SPY": 0.60,
       "TLT": 0.20,
       "GLD": 0.10,
       "CASH": 0.10
     },
     "managed_by": "buy_and_hold_baseline"
   }

   Then a parallel "khalid_strategy" tracker reads the system's
   regime calls (BEAR/BULL/CRISIS/EUPHORIA) and the action_required
   from morning briefs to derive what the system WOULD have allocated.

B. justhodl-pnl-tracker Lambda — runs daily at 22:00 UTC
   Reads:
     - portfolio/state.json (starting allocation)
     - intelligence-report.json (current regime + scores)
     - justhodl-outcomes table (what the system predicted, what
       happened)
   Computes:
     - buy_and_hold value today (current prices × starting weights)
     - khalid_strategy value today (regime-adjusted allocation since
       inception, applied at each regime change)
     - delta_pct (how much better/worse the system is doing)
   Writes:
     - portfolio/pnl-daily.json (today's snapshot)
     - portfolio/pnl-history.json (rolling 90-day history)

C. Schedule: EventBridge rate(1 day) at 22:00 UTC
   (after market close + after the daily report runs)

The hypothetical strategy logic:
  - When intelligence says CRISIS or BEAR phase → 40% equity, 60% cash
  - When NEUTRAL → match starting allocation (60/40)
  - When BULL or EUPHORIA → 80% equity, 20% cash
  - Re-evaluate daily; track every regime change as a virtual rebalance
  - Use Polygon for spot prices

This is genuinely useful WITHOUT being dangerous. You learn whether
the system's calls are good. You don't risk anything.
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)

BUCKET = "justhodl-dashboard-live"


# ════════════════════════════════════════════════════════════════════════
# Source for the new pnl-tracker Lambda
# ════════════════════════════════════════════════════════════════════════
PNL_TRACKER_SRC = '''"""
justhodl-pnl-tracker — Loop 2 hypothetical PnL tracker.

Runs daily at 22:00 UTC. Computes:
  - buy_and_hold portfolio value (starting allocation, drift-only)
  - khalid_strategy value (regime-adjusted allocation since inception)
  - delta_pct (system's value-add vs B&H)

Writes:
  - portfolio/pnl-daily.json   (today snapshot, full detail)
  - portfolio/pnl-history.json (rolling 365-day history)
"""
import json
import os
import time
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

s3 = boto3.client("s3", region_name=REGION)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def fetch_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[FETCH] {url[:80]}: {e}")
        return None


def get_spot_price(ticker):
    """Get latest closing price from Polygon."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_KEY}"
    data = fetch_json(url)
    if data and isinstance(data.get("results"), list) and data["results"]:
        return float(data["results"][0].get("c", 0))
    return None


def get_s3_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return None


def put_s3_json(key, body, cache="public, max-age=300"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def regime_to_allocation(regime, action_required):
    """Map JustHodl regime + action to a target allocation."""
    r = (regime or "").upper()
    a = (action_required or "").upper()

    # CRISIS / strong bearish action → defensive
    if "CRISIS" in r or "REDUCE ALL RISK" in a or "RAISE CASH" in a:
        return {"SPY": 0.30, "TLT": 0.20, "GLD": 0.10, "CASH": 0.40}

    # BEAR / cautious
    if "BEAR" in r or "PRE-CRISIS" in r or "REDUCE" in a or "DEFENSIVE" in a:
        return {"SPY": 0.40, "TLT": 0.20, "GLD": 0.15, "CASH": 0.25}

    # NEUTRAL — match starting baseline
    if "NEUTRAL" in r or not r:
        return {"SPY": 0.60, "TLT": 0.20, "GLD": 0.10, "CASH": 0.10}

    # BULL / risk-on
    if "BULL" in r or "OPTIMISTIC" in r or "RISK_ON" in r:
        return {"SPY": 0.75, "TLT": 0.10, "GLD": 0.05, "CASH": 0.10}

    # EUPHORIA — still some restraint (don't chase)
    if "EUPHORIA" in r:
        return {"SPY": 0.80, "TLT": 0.05, "GLD": 0.05, "CASH": 0.10}

    # Unknown → fall back to baseline
    return {"SPY": 0.60, "TLT": 0.20, "GLD": 0.10, "CASH": 0.10}


def compute_portfolio_value(allocations, starting_value, current_prices, baseline_prices):
    """Given current allocation + price ratios from baseline, compute today's value."""
    total = 0.0
    breakdown = {}
    for ticker, weight in allocations.items():
        if ticker == "CASH":
            # Cash earns ~0% (could add a tiny SOFR yield in v2)
            value = starting_value * weight
        else:
            cur = current_prices.get(ticker)
            base = baseline_prices.get(ticker)
            if not cur or not base or base == 0:
                value = starting_value * weight  # treat unknown as flat
            else:
                ratio = cur / base
                value = starting_value * weight * ratio
        breakdown[ticker] = round(value, 2)
        total += value
    return total, breakdown


def lambda_handler(event, context):
    print("=== JUSTHODL PNL TRACKER v1 ===")
    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    # 1. Read portfolio state (starting allocation + history)
    state = get_s3_json("portfolio/state.json")
    if not state:
        return {"statusCode": 500, "body": json.dumps({"error": "portfolio/state.json missing"})}

    starting = state.get("starting_value_usd", 100000)
    inception = state.get("as_of", today_str)
    baseline_alloc = state.get("allocations", {"SPY": 0.60, "TLT": 0.20, "GLD": 0.10, "CASH": 0.10})

    # 2. Read intelligence report → current regime
    intel = get_s3_json("intelligence-report.json") or {}
    phase = intel.get("phase", "UNKNOWN")
    regime = intel.get("regime", {}).get("khalid", "UNKNOWN") if isinstance(intel.get("regime"), dict) else "UNKNOWN"
    action = intel.get("action_required", "")
    print(f"  Current phase={phase}, khalid_regime={regime}, action={action[:80]}")

    # 3. Get baseline prices (what we paid at inception)
    # If state.json has baseline_prices, use them; else fetch + persist
    baseline_prices = state.get("baseline_prices", {})
    if not baseline_prices:
        print("  Baseline prices not set — capturing today's prices as baseline")
        for tk in ("SPY", "TLT", "GLD"):
            p = get_spot_price(tk)
            if p:
                baseline_prices[tk] = p
        # Persist the baseline back to state.json
        state["baseline_prices"] = baseline_prices
        state["as_of"] = today_str
        put_s3_json("portfolio/state.json", state, cache="no-cache")
        print(f"  Baseline captured: {baseline_prices}")

    # 4. Get current prices
    current_prices = {}
    for tk in ("SPY", "TLT", "GLD"):
        p = get_spot_price(tk)
        if p:
            current_prices[tk] = p
    print(f"  Current prices: {current_prices}")

    if not current_prices:
        return {"statusCode": 500, "body": json.dumps({"error": "could not fetch any current prices"})}

    # 5. Compute buy-and-hold value
    bh_value, bh_breakdown = compute_portfolio_value(
        baseline_alloc, starting, current_prices, baseline_prices,
    )

    # 6. Compute khalid_strategy current value
    # For v1 simplicity: apply CURRENT regime's allocation to TODAY's
    # price ratios from baseline. This is approximate (it doesn't model
    # historical regime changes mid-period — that requires regime history
    # which we'll add in v2). Conservative, but easy to reason about.
    khalid_alloc = regime_to_allocation(regime, action)
    ks_value, ks_breakdown = compute_portfolio_value(
        khalid_alloc, starting, current_prices, baseline_prices,
    )

    # 7. Compute deltas
    bh_return_pct = ((bh_value - starting) / starting) * 100
    ks_return_pct = ((ks_value - starting) / starting) * 100
    delta_pct = ks_return_pct - bh_return_pct

    snapshot = {
        "as_of": today_str,
        "generated_at": now.isoformat(),
        "inception": inception,
        "days_since_inception": max(0, (now.date() - datetime.fromisoformat(inception).date()).days)
                                if inception else 0,
        "starting_value_usd": starting,
        "current_phase": phase,
        "current_regime": regime,
        "current_action_required": action[:200],
        "buy_and_hold": {
            "allocation": baseline_alloc,
            "current_value_usd": round(bh_value, 2),
            "return_pct": round(bh_return_pct, 2),
            "breakdown": bh_breakdown,
        },
        "khalid_strategy": {
            "allocation": khalid_alloc,
            "current_value_usd": round(ks_value, 2),
            "return_pct": round(ks_return_pct, 2),
            "breakdown": ks_breakdown,
            "_note": "v1 approximation: current regime applied to current prices; doesn't model historical rebalances",
        },
        "delta_pct": round(delta_pct, 2),
        "system_alpha": round(delta_pct, 2),
        "prices": {
            "current": current_prices,
            "baseline": baseline_prices,
        },
        "v": "1.0",
        "DISCLAIMER": "HYPOTHETICAL — for tracking only. Not investment advice. Past hypothetical performance does not predict future returns.",
    }

    # 8. Write today's snapshot
    put_s3_json("portfolio/pnl-daily.json", snapshot, cache="public, max-age=300")
    print(f"  Wrote portfolio/pnl-daily.json ({bh_return_pct:+.2f}% B&H, {ks_return_pct:+.2f}% Khalid, Δ {delta_pct:+.2f}%)")

    # 9. Append to history (rolling 365 days)
    history = get_s3_json("portfolio/pnl-history.json") or {"v": "1.0", "snapshots": []}
    snapshots = history.get("snapshots", [])
    # Keep one snapshot per day — replace today's if it already exists
    snapshots = [s for s in snapshots if s.get("as_of") != today_str]
    snapshots.append({
        "as_of": today_str,
        "buy_and_hold_value_usd": round(bh_value, 2),
        "khalid_strategy_value_usd": round(ks_value, 2),
        "buy_and_hold_return_pct": round(bh_return_pct, 2),
        "khalid_return_pct": round(ks_return_pct, 2),
        "delta_pct": round(delta_pct, 2),
        "regime": regime,
        "phase": phase,
    })
    # Trim to last 365 days
    cutoff = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    snapshots = [s for s in snapshots if s.get("as_of", "") >= cutoff]
    history["snapshots"] = sorted(snapshots, key=lambda s: s.get("as_of", ""))
    history["last_updated"] = now.isoformat()
    put_s3_json("portfolio/pnl-history.json", history, cache="public, max-age=600")
    print(f"  History updated: {len(snapshots)} daily snapshots in last 365 days")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "as_of": today_str,
            "buy_and_hold_return_pct": round(bh_return_pct, 2),
            "khalid_return_pct": round(ks_return_pct, 2),
            "delta_pct": round(delta_pct, 2),
            "phase": phase,
            "regime": regime,
        }),
    }
'''


with report("loop2_pnl_tracker") as r:
    r.heading("Loop 2A — Portfolio state + PnL tracker Lambda")

    # ─── 1. Initialize portfolio/state.json ─────────────────────────────
    r.section("1. Initialize portfolio/state.json (only if missing)")
    try:
        existing = s3.get_object(Bucket=BUCKET, Key="portfolio/state.json")
        body = json.loads(existing["Body"].read().decode("utf-8"))
        r.log(f"  portfolio/state.json already exists ({existing['ContentLength']}B)")
        r.log(f"    starting_value_usd: {body.get('starting_value_usd')}")
        r.log(f"    inception: {body.get('as_of')}")
        r.log(f"    allocations: {body.get('allocations')}")
        r.log(f"    baseline_prices: {body.get('baseline_prices', 'not yet set')}")
    except s3.exceptions.NoSuchKey:
        # Create with sensible defaults
        initial_state = {
            "v": "1.0",
            "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "starting_value_usd": 100000,
            "allocations": {
                "SPY": 0.60,
                "TLT": 0.20,
                "GLD": 0.10,
                "CASH": 0.10,
            },
            "baseline_prices": {},  # will be filled on first run
            "managed_by": "loop2_pnl_tracker_v1",
            "_note": "Hypothetical $100k starting portfolio. Edit allocations to match your real allocation if desired. baseline_prices auto-captured on first tracker run.",
        }
        s3.put_object(
            Bucket=BUCKET, Key="portfolio/state.json",
            Body=json.dumps(initial_state, indent=2).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=600",
        )
        r.ok(f"  Initialized portfolio/state.json with $100k baseline (60/20/10/10)")
    except Exception as e:
        r.fail(f"  s3 check: {e}")
        raise SystemExit(1)

    # ─── 2. Set up Lambda source folder ─────────────────────────────────
    r.section("2. Set up Lambda source folder")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-pnl-tracker/source"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "lambda_function.py").write_text(PNL_TRACKER_SRC)
    r.ok(f"  Wrote {src_dir / 'lambda_function.py'} ({len(PNL_TRACKER_SRC):,}B)")

    # Validate
    import ast
    try:
        ast.parse(PNL_TRACKER_SRC)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        raise SystemExit(1)

    # ─── 3. Create or update Lambda function ────────────────────────────
    r.section("3. Create/update Lambda")

    # Build deployment zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, PNL_TRACKER_SRC)
    zbytes = buf.getvalue()
    r.log(f"  Deployment zip: {len(zbytes):,}B")

    function_name = "justhodl-pnl-tracker"
    role_arn = "arn:aws:iam::857687956942:role/lambda-execution-role"

    try:
        existing = lam.get_function(FunctionName=function_name)
        # Update existing
        lam.update_function_code(
            FunctionName=function_name, ZipFile=zbytes,
            Architectures=["arm64"],
        )
        lam.get_waiter("function_updated").wait(
            FunctionName=function_name,
            WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Updated existing Lambda {function_name}")
    except lam.exceptions.ResourceNotFoundException:
        # Create new
        resp = lam.create_function(
            FunctionName=function_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zbytes},
            Description="Loop 2 — hypothetical PnL tracker (B&H vs khalid_strategy)",
            Timeout=60,
            MemorySize=256,
            Architectures=["arm64"],
            Environment={
                "Variables": {
                    "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
                }
            },
        )
        r.ok(f"  Created new Lambda {function_name}")
        # Wait for it to be active
        lam.get_waiter("function_active_v2").wait(
            FunctionName=function_name,
            WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )

    # ─── 4. Test invoke ─────────────────────────────────────────────────
    r.section("4. Test invoke")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=function_name, InvocationType="RequestResponse")
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
            r.log(f"    {k:30} {v}")
    except Exception:
        r.log(f"  Raw payload: {payload[:400]}")

    # ─── 5. Verify S3 outputs ───────────────────────────────────────────
    r.section("5. Verify S3 outputs")
    for key in ("portfolio/pnl-daily.json", "portfolio/pnl-history.json", "portfolio/state.json"):
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 60
            r.ok(f"  {key:35} {obj['ContentLength']:>8}B age {age:.1f}m")
        except Exception as e:
            r.warn(f"  {key}: {e}")

    # ─── 6. Schedule with EventBridge (rate(1 day)) ─────────────────────
    r.section("6. Schedule with EventBridge — daily at 22:00 UTC")
    rule_name = "justhodl-pnl-tracker-daily"
    try:
        # Existing?
        try:
            existing_rule = events.describe_rule(Name=rule_name)
            r.log(f"  Rule {rule_name} already exists: {existing_rule['State']}")
        except events.exceptions.ResourceNotFoundException:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 22 * * ? *)",  # 22:00 UTC daily
                State="ENABLED",
                Description="Loop 2 — daily hypothetical PnL snapshot at 22:00 UTC",
            )
            r.ok(f"  Created EventBridge rule: cron(0 22 * * ? *)")

        # Add Lambda as target
        events.put_targets(
            Rule=rule_name,
            Targets=[{
                "Id": "1",
                "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{function_name}",
            }],
        )
        # Allow EventBridge to invoke
        try:
            lam.add_permission(
                FunctionName=function_name,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule_name}",
            )
            r.ok(f"  Added invoke permission for EventBridge")
        except lam.exceptions.ResourceConflictException:
            r.log(f"  Invoke permission already exists")
    except Exception as e:
        r.fail(f"  Schedule setup: {e}")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
        function_name=function_name,
        schedule="cron(0 22 * * ? *)",
    )
    r.log("Done")
