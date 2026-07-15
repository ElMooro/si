"""ops 3357 — TWO tasks:
  A. Deploy justhodl-sovereign-stress (no config existed → likely never scheduled). Invoke
     it, capture whether it writes data/sovereign-stress.json and its error state, and read
     its europe_stress.score_0_100 for a potential JSI overlay add.
  B. Mine the THESIS-category risk notes specifically (vs philosophy explainers) for
     TRADEABLE triggers: notes containing price levels, thresholds, or conditional risk
     rules ("if X above/below Y", "when Z crosses"). These are the operator's actionable
     risk signals, distinct from the conceptual notes.
"""
import json
import re
import time
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1", config=LONG)
lam = boto3.client("lambda", region_name="us-east-1", config=LONG)


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode())
    except Exception as e:
        return {"__err__": type(e).__name__}


with report("3357_sovereign_and_thesis") as r:
    # ── A. SOVEREIGN-STRESS ──
    r.section("A. Deploy + diagnose sovereign-stress")
    scfg = json.loads(Path("aws/lambdas/justhodl-sovereign-stress/config.json").read_text())
    deploy_lambda(
        report=r, function_name="justhodl-sovereign-stress",
        source_dir=Path("aws/lambdas/justhodl-sovereign-stress/source"),
        env_vars=scfg.get("env", {}),
        eb_rule_name=scfg["schedule"]["rule_name"], eb_schedule=scfg["schedule"]["cron"],
        timeout=scfg["timeout"], memory=scfg["memory"],
        description=(scfg.get("description") or "")[:256],
        create_function_url=False, smoke=False,
    )
    # synchronous invoke to capture error state directly
    r.log("invoking sovereign-stress synchronously to capture result…")
    try:
        resp = lam.invoke(FunctionName="justhodl-sovereign-stress",
                          InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode()
        r.log(f"  return: {body[:300]}")
        if resp.get("FunctionError"):
            r.fail(f"FunctionError: {resp['FunctionError']} — {body[:200]}")
    except Exception as e:
        r.log(f"  invoke exception: {type(e).__name__}: {str(e)[:200]}")
    time.sleep(3)
    ss = gj("data/sovereign-stress.json")
    if ss.get("__err__"):
        r.fail(f"still no output: {ss['__err__']}")
    else:
        es = ss.get("europe_stress") or {}
        r.ok(f"sovereign-stress WRITING now — europe score_0_100={es.get('score_0_100')} regime={es.get('regime')} errors={len(ss.get('errors', []))}")
        r.log(f"  most-stressed sovereign: {ss.get('most_stressed_sovereign')}")

    # ── B. THESIS RISK NOTES — tradeable triggers ──
    r.section("B. Thesis-category risk notes — tradeable triggers")
    brain = gj("data/brain.json")
    notes = (brain.get("notes") or []) if isinstance(brain, dict) else []
    RISK = ["risk", "crisis", "stress", "tail", "contagion", "carry", "unwind", "sovereign",
            "spread", "liquidity", "drawdown", "recession", "vix", "hedge", "credit",
            "repo", "rrp", "funding", "hyg", "lqd", "crash", "sell", "defensive"]
    # tradeable trigger patterns: levels, thresholds, conditionals
    LEVEL = re.compile(r"(above|below|under|over|cross|breaks?|reclaim|hold|when|if|>|<|"
                       r"\$\d|\d+\s*bps?|\d+%|\bthreshold\b|\btrigger\b)", re.I)
    thesis_risk = []
    for n in notes:
        cat = (n.get("cat") or n.get("category") or "").lower()
        txt = n.get("text") or n.get("note") or ""
        low = txt.lower()
        if cat == "thesis" and any(w in low for w in RISK) and LEVEL.search(txt):
            thesis_risk.append(txt[:320])
    r.log(f"thesis risk-notes with tradeable triggers: {len(thesis_risk)}")
    for t in thesis_risk[:30]:
        r.log(f"  • {t}")
