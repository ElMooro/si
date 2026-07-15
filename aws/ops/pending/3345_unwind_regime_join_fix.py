"""ops 3345 — carry-surface v1.3.1: fix the unwind overlay's risk-regime join.

3343 shipped the overlay but load_risk_regime() read the wrong field names, so the regime
came back None and the overlay ran on the default 1.0 multiplier (no live escalation).
Probe 3344 revealed the real schema: risk_regime_score / risk_regime / components.vix.vix
(+ eurodollar-stress.composite_score, eurodollar-plumbing.stress_score). Fixed to those.

VERIFY (strict this time): assert regime.available is True AND roro_score is not None AND
the regime label is non-null — a null join must FAIL, not pass.
"""
import json
import time
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

FN = "justhodl-carry-surface"
SRC = Path(f"aws/lambdas/{FN}/source")
CFG = json.loads(Path(f"aws/lambdas/{FN}/config.json").read_text())
ENV = CFG["env"]
BUCKET = ENV["S3_BUCKET"]
OUT_KEY = ENV["OUT_KEY"]
DESCRIPTION = (CFG.get("description") or "")[:256]

with report("3345_unwind_regime_join_fix") as r:
    r.section("Deploy carry-surface v1.3.1")
    deploy_lambda(
        report=r, function_name=FN, source_dir=SRC, env_vars=ENV,
        eb_rule_name=CFG["schedule"]["rule_name"], eb_schedule=CFG["schedule"]["cron"],
        timeout=CFG["timeout"], memory=CFG["memory"], description=DESCRIPTION,
        create_function_url=True, smoke=True,
    )

    r.section("Verify: regime join now populated")
    lam = boto3.client("lambda", region_name=CFG["region"])
    lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    time.sleep(3)
    s3 = boto3.client("s3", region_name=CFG["region"])
    payload = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode())

    u = payload.get("unwind_overlay") or {}
    rr = u.get("regime") or {}
    r.log(f"version={payload.get('version')}")
    r.log(f"regime available={rr.get('available')} label={rr.get('regime')} "
          f"roro={rr.get('roro_score')} vix={rr.get('vix')} stress={rr.get('stress_0_100')}")
    r.log(f"eurodollar: stress={rr.get('eurodollar_stress')} regime={rr.get('eurodollar_regime')} "
          f"plumbing={rr.get('plumbing_stress')}")
    r.log(f"regime_multiplier={u.get('regime_multiplier')} cohort_fragility={u.get('cohort_fragility')}")
    r.log(f"verdict={u.get('verdict')}")
    r.log(f"n_fragile={u.get('n_fragile')} n_crowded={u.get('n_crowded')}")

    # STRICT: a null join must fail.
    if rr.get("available") and rr.get("roro_score") is not None and rr.get("regime"):
        r.ok(f"REGIME JOINED — {rr.get('regime')} (RORO {rr.get('roro_score')}, "
             f"mult {u.get('regime_multiplier')}×). Overlay now regime-aware.")
    else:
        r.fail(f"regime STILL null: available={rr.get('available')} "
               f"roro={rr.get('roro_score')} label={rr.get('regime')}")

    if payload.get("version") == "1.3.1":
        r.ok("version 1.3.1 live")
    else:
        r.fail(f"version mismatch: {payload.get('version')}")
