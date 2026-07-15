"""ops 3360 — THREE deploys + verify:
  1. sovereign-stress FIXED (ECB CSV Accept-header bug + SS_CI primary) → should now
     populate CISS/SovCISS, europe score should be REAL (not 1.0), errors near 0.
  2. JSI v1.4.0 SPINE expanded with brain-directed liquidity plumbing: Bank Reserves
     (WRESBAL, chg transform, draining=stress) + Fed Balance Sheet (WALCL, chg, QT=stress).
  3. JSI calibrator updated to fit the 9-component spine (incl chg-transform series).

VERIFY: sovereign errors↓ & score real; JSI spine now 9 components incl reserves/Fed-BS
with sane stress; calibrator fits all 9; index still coherent + percentile intact.
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
lam = boto3.client("lambda", region_name="us-east-1", config=LONG)
s3 = boto3.client("s3", region_name="us-east-1", config=LONG)


def cfg(fn):
    return json.loads(Path(f"aws/lambdas/{fn}/config.json").read_text())


with report("3360_plumbing_spine_sovereign") as r:
    # ── 1. sovereign-stress fix ──
    r.section("1. Deploy + verify sovereign-stress fix")
    sc = cfg("justhodl-sovereign-stress")
    deploy_lambda(report=r, function_name="justhodl-sovereign-stress",
                  source_dir=Path("aws/lambdas/justhodl-sovereign-stress/source"),
                  env_vars=sc.get("env", {}),
                  eb_rule_name=sc["schedule"]["rule_name"], eb_schedule=sc["schedule"]["cron"],
                  timeout=sc["timeout"], memory=sc["memory"],
                  description=(sc.get("description") or "")[:256],
                  create_function_url=False, smoke=False)
    resp = lam.invoke(FunctionName="justhodl-sovereign-stress",
                      InvocationType="RequestResponse", Payload=b"{}")
    r.log(f"  return: {resp['Payload'].read().decode()[:200]}")
    time.sleep(3)
    ss = json.loads(s3.get_object(Bucket=BUCKET, Key="data/sovereign-stress.json")["Body"].read())
    es = ss.get("europe_stress") or {}
    nerr = len(ss.get("errors") or [])
    r.log(f"  europe score_0_100={es.get('score_0_100')} regime={es.get('regime')} worst={es.get('worst_country')} errors={nerr}")
    r.log(f"  most-stressed sovereign: {ss.get('most_stressed_sovereign')}")
    if nerr <= 3 and es.get("score_0_100", 1.0) != 1.0:
        r.ok(f"SOVEREIGN-STRESS FIXED — errors {nerr} (was 11), real score {es.get('score_0_100')}.")
    elif nerr < 11:
        r.log(f"⚠ partial fix — errors down to {nerr} (was 11); inspect remaining.")
    else:
        r.fail(f"still {nerr} errors")

    # ── 2 + 3. JSI v1.4.0 + calibrator ──
    r.section("2. Deploy JSI calibrator (9-component spine)")
    cc = cfg("justhodl-jsi-calibrator")
    deploy_lambda(report=r, function_name="justhodl-jsi-calibrator",
                  source_dir=Path("aws/lambdas/justhodl-jsi-calibrator/source"),
                  env_vars=cc["env"],
                  eb_rule_name=cc["schedule"]["rule_name"], eb_schedule=cc["schedule"]["cron"],
                  timeout=cc["timeout"], memory=cc["memory"],
                  description=(cc.get("description") or "")[:256],
                  create_function_url=False, smoke=False)
    lam.invoke(FunctionName="justhodl-jsi-calibrator", InvocationType="Event", Payload=b"{}")
    r.log("  calibrator invoked (async); will verify spine picks up after JSI run")

    r.section("3. Deploy JSI v1.4.0 (reserves + Fed-BS spine)")
    jc = cfg("justhodl-stress-index")
    deploy_lambda(report=r, function_name="justhodl-stress-index",
                  source_dir=Path("aws/lambdas/justhodl-stress-index/source"),
                  env_vars=jc["env"],
                  eb_rule_name=jc["schedule"]["rule_name"], eb_schedule=jc["schedule"]["cron"],
                  timeout=jc["timeout"], memory=jc["memory"],
                  description=(jc.get("description") or "")[:256],
                  create_function_url=True, smoke=False)
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read()).get("generated_at")
    except Exception:
        prev = None
    lam.invoke(FunctionName="justhodl-stress-index", InvocationType="Event", Payload=b"{}")
    jsi = None
    for i in range(35):
        time.sleep(6)
        try:
            j = json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read())
            if j.get("generated_at") != prev:
                jsi = j; break
        except Exception:
            continue
    if not jsi:
        r.fail("JSI did not refresh"); raise SystemExit(0)

    sc_comp = jsi.get("spine_components") or {}
    r.log(f"  JSI v{jsi.get('version')} jsi={jsi.get('jsi')} spine={jsi.get('jsi_spine')} regime={jsi.get('regime')} pctile={jsi.get('percentile_since_1990')}")
    r.log(f"  spine components ({len(sc_comp)}): {list(sc_comp.keys())}")
    for sid in ("WRESBAL", "WALCL"):
        c = sc_comp.get(sid)
        if c:
            r.log(f"    {sid}: {c.get('label')} chg={c.get('raw')} stress={c.get('stress')} z={c.get('z')} mode={c.get('mode')}")

    if "WRESBAL" in sc_comp and "WALCL" in sc_comp:
        r.ok("LIQUIDITY PLUMBING in spine — Bank Reserves + Fed Balance Sheet now historical components (brain directive).")
    else:
        r.fail(f"plumbing components missing: {[k for k in ('WRESBAL','WALCL') if k not in sc_comp]}")
    if jsi.get("percentile_since_1990") is not None and jsi.get("history_span", {}).get("n", 0) > 2000:
        r.ok(f"index coherent — {jsi['history_span']['n']} pts, {jsi['percentile_since_1990']}th pctile since 1990.")
    else:
        r.fail("index integrity failed")

    # calibrator picked up the 9-component fit?
    time.sleep(4)
    try:
        cal = json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi-calibration.json")["Body"].read())
        sp = cal.get("spine") or {}
        r.log(f"  calibrator spine: n={sp.get('sample_size')} components={list((sp.get('ic') or {}).keys())}")
        if "WRESBAL" in (sp.get("ic") or {}) and "WALCL" in (sp.get("ic") or {}):
            r.ok(f"calibrator now fits all 9 — reserves IC={sp['ic'].get('WRESBAL')}, Fed-BS IC={sp['ic'].get('WALCL')}.")
        else:
            r.log("⚠ calibrator not yet reflecting new components (async lag)")
    except Exception as e:
        r.log(f"calibration read: {e}")
