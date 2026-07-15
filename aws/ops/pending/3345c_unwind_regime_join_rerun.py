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
from botocore.config import Config

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

FN = "justhodl-carry-surface"
SRC = Path(f"aws/lambdas/{FN}/source")
CFG = json.loads(Path(f"aws/lambdas/{FN}/config.json").read_text())
ENV = CFG["env"]
BUCKET = ENV["S3_BUCKET"]
OUT_KEY = ENV["OUT_KEY"]
DESCRIPTION = (CFG.get("description") or "")[:256]

# Long read-timeout client — the smoke invoke on a 170-asset run exceeds botocore's
# default 60s read timeout and triggers a retry storm (3345/3345b hung ~320s). We deploy
# WITHOUT the helper's smoke invoke, then do ONE Event(async) invoke and poll S3 for the
# fresh object instead of blocking on a RequestResponse read.
LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})

with report("3345c_unwind_regime_join_rerun") as r:
    r.section("Deploy carry-surface v1.3.1 (no blocking smoke)")
    deploy_lambda(
        report=r, function_name=FN, source_dir=SRC, env_vars=ENV,
        eb_rule_name=CFG["schedule"]["rule_name"], eb_schedule=CFG["schedule"]["cron"],
        timeout=CFG["timeout"], memory=CFG["memory"], description=DESCRIPTION,
        create_function_url=True, smoke=False,
    )

    r.section("Verify: async invoke + poll S3")
    lam = boto3.client("lambda", region_name=CFG["region"], config=LONG)
    s3 = boto3.client("s3", region_name=CFG["region"], config=LONG)

    # Record current object version so we can detect the fresh write.
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode())
        prev_gen = prev.get("generated_at")
    except Exception:
        prev_gen = None

    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")  # async, returns 202
    r.log("async invoke fired; polling S3 for fresh write…")

    payload = None
    for attempt in range(24):  # up to ~2 min
        time.sleep(5)
        try:
            obj = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode())
        except Exception:
            continue
        if obj.get("generated_at") and obj.get("generated_at") != prev_gen:
            payload = obj
            r.log(f"fresh write detected after ~{(attempt+1)*5}s")
            break
    if payload is None:
        r.fail("no fresh carry-surface.json within 2 min of async invoke")
        raise SystemExit(0)

    u = payload.get("unwind_overlay") or {}
    rr = u.get("regime") or {}
    r.log(f"version={payload.get('version')} n_assets={payload.get('n_assets')}")
    r.log(f"regime available={rr.get('available')} label={rr.get('regime')} "
          f"roro={rr.get('roro_score')} vix={rr.get('vix')} stress={rr.get('stress_0_100')}")
    r.log(f"eurodollar: stress={rr.get('eurodollar_stress')} regime={rr.get('eurodollar_regime')} "
          f"plumbing={rr.get('plumbing_stress')}")
    r.log(f"regime_multiplier={u.get('regime_multiplier')} cohort_fragility={u.get('cohort_fragility')}")
    r.log(f"verdict={u.get('verdict')}")
    r.log(f"n_fragile={u.get('n_fragile')} n_crowded={u.get('n_crowded')}")

    if rr.get("available") and rr.get("roro_score") is not None and rr.get("regime"):
        r.ok(f"REGIME JOINED — {rr.get('regime')} (RORO {rr.get('roro_score')}, "
             f"mult {u.get('regime_multiplier')}×). Overlay is regime-aware.")
    else:
        r.fail(f"regime STILL null: available={rr.get('available')} "
               f"roro={rr.get('roro_score')} label={rr.get('regime')}")

    if payload.get("version") == "1.3.1":
        r.ok("version 1.3.1 live")
    else:
        r.fail(f"version mismatch: {payload.get('version')}")
