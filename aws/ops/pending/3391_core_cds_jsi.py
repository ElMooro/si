"""ops 3391 — cross-link Global Sovereign Desk → JSI. Deploy global-sovereign v1.1.0 (adds
core_dm_cds_stress = core developed-market sovereign CDS, the systemic flight-to-quality
signal) + JSI v1.7.0 (reads it as 20th overlay feed, Global Risk group).
VERIFY: core_dm_cds computed; JSI overlay=20 with Core Sovereign CDS live."""
import json, time
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1", config=LONG)
s3 = boto3.client("s3", region_name="us-east-1", config=LONG)
def cfg(fn): return json.loads(Path(f"aws/lambdas/{fn}/config.json").read_text())

with report("3391_core_cds_jsi") as r:
    # 1. global-sovereign v1.1.0
    r.section("Deploy global-sovereign v1.1.0 (core-DM CDS)")
    gc = cfg("justhodl-global-sovereign")
    deploy_lambda(report=r, function_name="justhodl-global-sovereign",
                  source_dir=Path("aws/lambdas/justhodl-global-sovereign/source"),
                  env_vars=gc.get("env", {}),
                  eb_rule_name=gc["schedule"]["rule_name"], eb_schedule=gc["schedule"]["cron"],
                  timeout=gc["timeout"], memory=gc["memory"],
                  description=(gc.get("description") or "")[:256],
                  create_function_url=True, smoke=False)
    lam.invoke(FunctionName="justhodl-global-sovereign", InvocationType="Event", Payload=b"{}")
    r.log("harvesting…")
    gs=None
    for i in range(30):
        time.sleep(6)
        try:
            j=json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-sovereign.json")["Body"].read())
            if j.get("core_dm_cds_stress_0_100") is not None:
                gs=j; break
        except Exception: continue
    if gs:
        r.ok(f"core-DM CDS = {gs.get('core_dm_cds_bp')}bp → stress {gs.get('core_dm_cds_stress_0_100')} (n={gs.get('core_dm_cds_n')})")
    else:
        r.fail("core_dm_cds not computed"); raise SystemExit(0)

    # 2. JSI v1.7.0
    r.section("Deploy JSI v1.7.0 (20th feed: Core Sovereign CDS)")
    jc = cfg("justhodl-stress-index")
    deploy_lambda(report=r, function_name="justhodl-stress-index",
                  source_dir=Path("aws/lambdas/justhodl-stress-index/source"),
                  env_vars=jc["env"],
                  eb_rule_name=jc["schedule"]["rule_name"], eb_schedule=jc["schedule"]["cron"],
                  timeout=jc["timeout"], memory=jc["memory"],
                  description=(jc.get("description") or "")[:256],
                  create_function_url=True, smoke=False)
    try:
        prev=json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read()).get("generated_at")
    except Exception: prev=None
    lam.invoke(FunctionName="justhodl-stress-index", InvocationType="Event", Payload=b"{}")
    jsi=None
    for i in range(35):
        time.sleep(6)
        try:
            j=json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read())
            if j.get("generated_at")!=prev: jsi=j; break
        except Exception: continue
    if not jsi:
        r.fail("JSI did not refresh"); raise SystemExit(0)
    oc=jsi.get("overlay_components") or []
    cds=next((c for c in oc if c.get("label")=="Core Sovereign CDS"), None)
    r.log(f"JSI v{jsi.get('version')} overlay={len(oc)} live={jsi.get('n_overlay_live')} jsi={jsi.get('jsi')}")
    r.log(f"  Core Sovereign CDS: {cds.get('stress') if cds else 'MISSING'}")
    if cds and cds.get("stress") is not None:
        r.ok(f"CORE SOVEREIGN CDS wired as JSI 20th feed — stress {cds['stress']} (systemic flight-to-quality signal, Global Risk group).")
    else:
        r.fail("core CDS feed not live")
    if len(oc)==20:
        r.ok("JSI overlay now 20 feeds.")
    else:
        r.log(f"overlay has {len(oc)} feeds")
