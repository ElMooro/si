"""ops 3392 — replace 'all sovereign' core-CDS with EURODOLLAR-HUB funding stress: only the
offshore-USD system's core funding centers (US/UK/EU core+periphery/CH/JP/HK/SG/KR/TW/CA/AU).
Danger-first composite = 0.6*pack-avg + 0.4*worst-hub, so a lone canary (France/Italy 2011,
CS-era Switzerland) lights it up rather than being diluted. global-sovereign v1.2.0 + JSI v1.8.0.
VERIFY: eurodollar_hub_stress computed with worst-hub; JSI 20th feed = 'Eurodollar Hub Stress'."""
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

with report("3392_eurodollar_hub") as r:
    r.section("Deploy global-sovereign v1.2.0 (eurodollar-hub stress)")
    gc = cfg("justhodl-global-sovereign")
    deploy_lambda(report=r, function_name="justhodl-global-sovereign",
                  source_dir=Path("aws/lambdas/justhodl-global-sovereign/source"),
                  env_vars=gc.get("env", {}),
                  eb_rule_name=gc["schedule"]["rule_name"], eb_schedule=gc["schedule"]["cron"],
                  timeout=gc["timeout"], memory=gc["memory"],
                  description=(gc.get("description") or "")[:256],
                  create_function_url=True, smoke=False)
    lam.invoke(FunctionName="justhodl-global-sovereign", InvocationType="Event", Payload=b"{}")
    r.log("harvesting eurodollar hubs…")
    gs=None
    for i in range(30):
        time.sleep(6)
        try:
            j=json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-sovereign.json")["Body"].read())
            if j.get("eurodollar_hub_stress_0_100") is not None:
                gs=j; break
        except Exception: continue
    if not gs:
        r.fail("eurodollar_hub_stress not computed"); raise SystemExit(0)
    worst=gs.get("eurodollar_hub_worst") or {}
    r.ok(f"EURODOLLAR-HUB STRESS = {gs.get('eurodollar_hub_stress_0_100')} (avg CDS {gs.get('eurodollar_hub_avg_cds_bp')}bp, n={gs.get('eurodollar_hub_n')})")
    r.log(f"  worst hub (the canary): {worst.get('country')} @ {worst.get('cds_bp')}bp → stress {worst.get('stress')}")
    r.log("  hub detail (most→least stressed):")
    for h in (gs.get("eurodollar_hub_detail") or [])[:12]:
        r.log(f"    {h['country']}: {h['cds_bp']}bp (stress {h['stress']})")

    r.section("Deploy JSI v1.8.0 (Eurodollar Hub Stress feed)")
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
    ed=next((c for c in oc if c.get("label")=="Eurodollar Hub Stress"), None)
    r.log(f"JSI v{jsi.get('version')} overlay={len(oc)} live={jsi.get('n_overlay_live')} jsi={jsi.get('jsi')}")
    if ed and ed.get("stress") is not None:
        r.ok(f"EURODOLLAR HUB STRESS wired into JSI — {ed['stress']} (danger-first: pack-avg + worst-hub, Global Risk group).")
    else:
        r.fail("eurodollar hub feed not live")
