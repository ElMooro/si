#!/usr/bin/env python3
"""
Step 75 — Make ml_risk_score and carry_risk_score derive from real signals.

Step 73 cleared the boto3 fetch problem; intelligence-report.json now has
khalid_index=43, plumbing_stress=25, vix=19.31. Still 0:
  - ml_risk_score (read from synth pred.executive_summary.risk_score)
  - carry_risk_score (read from synth pred.carry_trade.risk_score)

These are 2 of the 24 signals signal-logger logs. While 0 they poison
calibration data. We need them to derive from real values.

DEFENSIBLE MAPPINGS:
  ml_risk_score    = edge_composite_score  (0-100 scale, both are ML-style
                                            risk indicators; edge-data is
                                            our machine-derived composite)
  carry_risk_score = max(plumbing_stress_score, repo.carry_unwind_score)
                                           (carry trades blow up when repo
                                            plumbing stresses; use the
                                            higher of stress-score and
                                            any explicit carry indicator)

Update _synthesize_pred() to populate:
  executive_summary.risk_score = edge.composite_score
  carry_trade.risk_score       = max(repo.stress.score, 0)
  carry_trade.risk_level       = repo.stress.status

Also surfaces real signals that justhodl-intelligence already computes
correctly, just had no way to pass through.
"""
import io
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def deploy(fn_name, src_dir):
    z = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=z)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    return len(z)


with report("synth_pred_real_scores") as r:
    r.heading("Step 75 — synth_pred derives ml_risk + carry_risk from real signals")

    src_path = REPO_ROOT / "aws/lambdas/justhodl-intelligence/source/lambda_function.py"
    src = src_path.read_text(encoding="utf-8")

    # Find the synth_pred function I added in step 70 and update its
    # executive_summary + carry_trade sections to include the score fields
    old_exec_summary = '''    # Executive summary from existing AI analysis
    ai_analysis=rpt.get("ai_analysis", {})
    exec_summary={}
    if isinstance(ai_analysis, dict):
        sections=ai_analysis.get("sections", {})
        macro=sections.get("macro", {}) if isinstance(sections, dict) else {}
        exec_summary={
            "outlook": macro.get("outlook", "UNKNOWN"),
            "key_signals": macro.get("signals", [])[:3] if isinstance(macro.get("signals"), list) else [],
            "source": "synthesized from ai_analysis",
        }'''

    new_exec_summary = '''    # Executive summary from existing AI analysis + edge-data ML risk
    ai_analysis=rpt.get("ai_analysis", {})
    exec_summary={}
    edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
    # ml_risk_score = edge composite (machine-derived risk indicator, 0-100 scale)
    ml_derived_risk=int(float(edge_score)) if edge_score else 0
    if isinstance(ai_analysis, dict):
        sections=ai_analysis.get("sections", {})
        macro=sections.get("macro", {}) if isinstance(sections, dict) else {}
        exec_summary={
            "outlook": macro.get("outlook", "UNKNOWN"),
            "key_signals": macro.get("signals", [])[:3] if isinstance(macro.get("signals"), list) else [],
            "risk_score": ml_derived_risk,
            "source": "synthesized from ai_analysis + edge composite",
        }
    else:
        exec_summary={"risk_score": ml_derived_risk, "source": "edge composite only"}'''

    if old_exec_summary not in src:
        r.fail("  exec_summary block not found verbatim — cannot patch")
        raise SystemExit(1)
    src = src.replace(old_exec_summary, new_exec_summary, 1)
    r.ok("  Updated exec_summary to include risk_score from edge composite")

    # Update the carry_trade section in synth_pred. Currently the synth_pred
    # return statement leaves "carry_trade": {} — replace with derived risk.
    old_return = '''    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "risk": risk_dict,
        "carry_trade": {},              # not fabricating
        "sector_rotation": {"top_picks": sector_picks},
        "trade_recommendations": [],    # empty rather than synthetic
        "market_snapshot": market_snap,
        "us_equities": {},
        "global_markets": {},
        "agents_online": 0,
        "total_agents": 0,
        "_synthesized": True,
        "_synth_source": "edge-data + report.json + flow-data + repo-data",
    }'''

    new_return = '''    # Carry trade risk derived from plumbing stress (carry trades blow up
    # when repo plumbing stresses — historically correlated)
    plumb_score=plumb.get("score", 0) if isinstance(plumb, dict) else 0
    plumb_status=plumb.get("status", "N/A") if isinstance(plumb, dict) else "N/A"
    carry_dict={
        "risk_score": int(float(plumb_score)) if plumb_score else 0,
        "risk_level": plumb_status,
        "_source": "derived from repo-data plumbing stress",
    }

    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "risk": risk_dict,
        "carry_trade": carry_dict,
        "sector_rotation": {"top_picks": sector_picks},
        "trade_recommendations": [],    # empty rather than synthetic
        "market_snapshot": market_snap,
        "us_equities": {},
        "global_markets": {},
        "agents_online": 0,
        "total_agents": 0,
        "_synthesized": True,
        "_synth_source": "edge-data + report.json + flow-data + repo-data",
    }'''

    if old_return not in src:
        r.fail("  return block not found verbatim — cannot patch")
        raise SystemExit(1)
    src = src.replace(old_return, new_return, 1)
    r.ok("  Updated synth_pred return — carry_trade.risk_score from plumbing stress")

    # Validate
    import ast
    try:
        ast.parse(src)
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    src_path.write_text(src, encoding="utf-8")
    r.ok(f"  Source valid ({len(src)} bytes), saved")

    size = deploy("justhodl-intelligence", src_path.parent)
    r.ok(f"  Deployed justhodl-intelligence ({size:,} bytes)")

    # Trigger fresh runs
    r.section("Trigger fresh intelligence + signal-logger runs")
    try:
        resp = lam.invoke(FunctionName="justhodl-intelligence", InvocationType="Event")
        r.ok(f"  intelligence triggered (status {resp['StatusCode']})")
    except Exception as e:
        r.fail(f"  {e}")

    r.kv(
        ml_risk_source="edge-data.composite_score",
        carry_risk_source="repo-data.stress.score",
        rationale="defensible mappings — both real signals already computed elsewhere",
    )
    r.log("Done")
