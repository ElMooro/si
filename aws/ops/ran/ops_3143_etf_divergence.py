"""ops 3143 — Flow-Price Divergence layer in etf-fund-flows.

Khalid's question: does ETF AUM identify industry capital flows?
Audit answer: the engine already computes TRUE flows (Polygon
creation/redemption), %AUM windows, 90d z-scores and category rotation.
The genuine gap was the highest-alpha construct: PRICE-FLOW DIVERGENCE.

Shipped in the engine (nav history rides in the same API rows → zero
extra calls):
  • per-ETF ret_5d/21d from nav, quadrant (STEALTH_ACCUMULATION /
    DISTRIBUTION_RALLY / TREND_CONFIRMED / CAPITULATION),
    divergence_score = z * -tanh(ret/8)
  • divergence_board (top-10 each side, broad ETFs excluded) →
    composite.json + rotation.json
  • DynamoDB emission (justhodl-signals, schema v2, |z|>=1.5, <=12/run)
    as etf_stealth_accum / etf_distribution_rally → outcome-checker
    grades at 5/21/63d → scorecard + magdist learn whether stealth
    accumulation leads. flows.html renders the board.

Gates: >=60% of scored ETFs carry ret_21d+quadrant · board present ·
emission count kv'd (Dynamo errors surface in CW/log) · industry-rotation
join untouched (daily.json metrics fields only ADDED).
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-etf-fund-flows"

HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]
SRC = AWS_DIR / "lambdas" / FN / "source"
CFG = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())

S3 = boto3.client("s3", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3143_etf_divergence") as rep:
    fails, warns = [], []
    t0 = datetime.now(timezone.utc)
    rep.heading("ops 3143 — Flow-Price Divergence layer")

    rep.section("1. Deploy (env preserved from live function)")
    lam = boto3.client("lambda", region_name=REGION)
    live_env = (lam.get_function_configuration(FunctionName=FN)
                .get("Environment") or {}).get("Variables") or {}
    rep.log(f"preserving env keys: {sorted(live_env)}")
    sched = CFG.get("schedule") or {}
    try:
        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC,
            env_vars=live_env,
            eb_rule_name=(sched.get("name") or sched.get("rule_name")),
            eb_schedule=(sched.get("expression") or sched.get("cron")),
            timeout=CFG.get("timeout", 300), memory=CFG.get("memory", 1024),
            description=CFG.get("description", "")[:250],
        )
    except Exception as e:
        rep.fail(f"deploy failed: {str(e)[:200]}")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)

    rep.section("2. Fresh composite with divergence board")
    comp = None
    deadline = time.time() + 360
    while time.time() < deadline:
        try:
            d = s3_json("etf-flows/composite.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                comp = d
                break
        except Exception:
            pass
        time.sleep(12)
    if comp is None:
        rep.fail("composite.json never freshened")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)
    board = comp.get("divergence_board")
    if not board:
        rep.fail("divergence_board missing from composite.json")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)

    rep.section("3. Gates")
    daily = s3_json("etf-flows/daily.json")
    ms = [m for m in daily.get("metrics") or [] if not m.get("error")]
    with_q = [m for m in ms if m.get("quadrant") is not None
              and m.get("ret_21d_pct") is not None]
    cov = round(100 * len(with_q) / len(ms), 1) if ms else 0
    rep.kv(etfs_ok=len(ms), quadrant_coverage_pct=cov,
           n_scored=board.get("n_scored"),
           stealth=len(board.get("stealth_accumulation") or []),
           distribution=len(board.get("distribution_rally") or []),
           trend_confirmed=board.get("trend_confirmed"),
           capitulation=board.get("capitulation"),
           signals_logged=comp.get("divergence_signals_logged"))
    if cov < 60:
        fails.append(f"quadrant coverage {cov}% (<60) — nav history thin?")
    else:
        rep.ok(f"quadrant coverage {cov}% of {len(ms)} ETFs")
    for side in ("stealth_accumulation", "distribution_rally"):
        for m in (board.get(side) or [])[:5]:
            rep.log(f"  {side[:7]}: {m['ticker']} ({m.get('subcategory')}) "
                    f"z={m.get('flow_zscore_90d')} "
                    f"ret21d={m.get('ret_21d_pct')}% "
                    f"aum21d={m.get('pct_aum_21d')}% "
                    f"score={m.get('divergence_score')}")
    if not (board.get("stealth_accumulation") or
            board.get("distribution_rally")):
        warns.append("both divergence sides empty today — market-state "
                     "dependent, verify again after a trending week")
    if comp.get("divergence_signals_logged") is None:
        fails.append("signals_logged missing from composite")
    # blast-radius: legacy fields still intact for industry-rotation join
    sample = ms[0] if ms else {}
    for k in ("flow_5d_usd", "flow_21d_usd", "pct_aum_21d",
              "flow_zscore_90d", "signal_label"):
        if k not in sample:
            fails.append(f"legacy metric field lost: {k}")
    rep.ok("legacy metric fields intact (industry-rotation join safe)")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
