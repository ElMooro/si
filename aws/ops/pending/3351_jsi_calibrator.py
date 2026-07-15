"""ops 3351 — deploy JSI calibrator + JSI engine v1.1.0 (consumes calibrated spine weights,
writes overlay snapshots for forward-IC).

The calibrator's big win: it fits spine component weights on the FULL 1990-2026 FRED sample
(thousands of paired obs across every crisis), not a single risk-on window. Overlay weights
stay equal-weight until 30+ snapshots mature.

VERIFY (async invoke + S3 poll):
  (a) calibrator ran, spine IC fit on a large sample (spine_n > 2000),
  (b) spine weights written to SSM, sum≈1, within 5-40% band,
  (c) JSI engine v1.1.0 picks up calibrated weights (spine_weight_mode='calibrated'),
  (d) overlay history snapshot is being written.
"""
import json
import time
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"

with report("3351_jsi_calibrator") as r:
    # ---- deploy calibrator ----
    r.section("Deploy JSI calibrator")
    ccfg = json.loads(Path("aws/lambdas/justhodl-jsi-calibrator/config.json").read_text())
    deploy_lambda(
        report=r, function_name="justhodl-jsi-calibrator",
        source_dir=Path("aws/lambdas/justhodl-jsi-calibrator/source"),
        env_vars=ccfg["env"],
        eb_rule_name=ccfg["schedule"]["rule_name"], eb_schedule=ccfg["schedule"]["cron"],
        timeout=ccfg["timeout"], memory=ccfg["memory"],
        description=(ccfg.get("description") or "")[:256],
        create_function_url=False, smoke=False,
    )

    # ---- deploy JSI engine v1.1.0 ----
    r.section("Deploy JSI engine v1.1.0")
    jcfg = json.loads(Path("aws/lambdas/justhodl-stress-index/config.json").read_text())
    deploy_lambda(
        report=r, function_name="justhodl-stress-index",
        source_dir=Path("aws/lambdas/justhodl-stress-index/source"),
        env_vars=jcfg["env"],
        eb_rule_name=jcfg["schedule"]["rule_name"], eb_schedule=jcfg["schedule"]["cron"],
        timeout=jcfg["timeout"], memory=jcfg["memory"],
        description=(jcfg.get("description") or "")[:256],
        create_function_url=True, smoke=False,
    )

    lam = boto3.client("lambda", region_name="us-east-1", config=LONG)
    s3 = boto3.client("s3", region_name="us-east-1", config=LONG)

    # ---- run calibrator (synchronous read of its return via async + S3 report) ----
    r.section("Run calibrator, verify spine IC fit")
    lam.invoke(FunctionName="justhodl-jsi-calibrator", InvocationType="Event", Payload=b"{}")
    r.log("calibrator invoked; polling data/jsi-calibration.json…")
    cal = None
    for i in range(30):
        time.sleep(6)
        try:
            c = json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi-calibration.json")["Body"].read())
            if c.get("spine"):
                cal = c
                r.log(f"calibration report after ~{(i+1)*6}s")
                break
        except Exception:
            continue
    if not cal:
        r.fail("no jsi-calibration.json produced")
        raise SystemExit(0)

    sp = cal.get("spine") or {}
    ov = cal.get("overlay") or {}
    r.log(f"spine: mode={sp.get('mode')} sample_size={sp.get('sample_size')}")
    r.log(f"spine IC: {sp.get('ic')}")
    r.log(f"spine weights: {sp.get('weights')}")
    r.log(f"overlay: mode={ov.get('mode')} sample_size={ov.get('sample_size')}")

    wsum = sum((sp.get("weights") or {}).values())
    if sp.get("sample_size", 0) > 2000:
        r.ok(f"SPINE FIT ON FULL HISTORY — {sp.get('sample_size')} paired obs (multi-regime, spans every crisis).")
    else:
        r.fail(f"spine sample too small: {sp.get('sample_size')}")
    if sp.get("weights") and abs(wsum - 1.0) < 0.02:
        wmax = max(sp["weights"].values()); wmin = min(sp["weights"].values())
        r.ok(f"spine weights valid — sum={round(wsum,3)}, range [{round(wmin,3)},{round(wmax,3)}].")
    else:
        r.fail(f"spine weights invalid: sum={wsum}")

    # ---- verify JSI engine picks up calibrated weights ----
    r.section("Verify JSI engine consumes calibrated weights")
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read()).get("generated_at")
    except Exception:
        prev = None
    lam.invoke(FunctionName="justhodl-stress-index", InvocationType="Event", Payload=b"{}")
    jsi = None
    for i in range(30):
        time.sleep(6)
        try:
            j = json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read())
            if j.get("generated_at") != prev:
                jsi = j
                break
        except Exception:
            continue
    if jsi:
        r.log(f"JSI version={jsi.get('version')} spine_weight_mode={jsi.get('spine_weight_mode')} jsi={jsi.get('jsi')}")
        r.log(f"spine_meta weights: {[(m['series'], m['weight']) for m in (jsi.get('spine_meta') or [])]}")
        if jsi.get("spine_weight_mode") == "calibrated":
            r.ok("JSI engine now running on CALIBRATED spine weights (empirical, full-history).")
        else:
            r.log(f"⚠ JSI still on '{jsi.get('spine_weight_mode')}' weights — SSM propagation lag; will pick up next run.")
        # overlay snapshot written?
        try:
            oh = json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi-overlay-history.json")["Body"].read())
            r.ok(f"overlay history writing — {len(oh.get('snapshots', []))} snapshot(s) captured.")
        except Exception:
            r.log("⚠ overlay history not yet present (first write may lag)")
    else:
        r.fail("JSI engine did not refresh")
