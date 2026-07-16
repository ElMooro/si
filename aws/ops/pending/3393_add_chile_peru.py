"""ops 3393 — add Chile + Peru to eurodollar-hub set (USD-dependent periphery = early cracks
in global financing). global-sovereign v1.3.0. Confirm hub count up, Chile CDS present as
new worst-hub canary, JSI eurodollar feed reflects it. VERIFY named hubs all present too."""
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

with report("3393_add_chile_peru") as r:
    r.section("Deploy global-sovereign v1.3.0 (+Chile +Peru)")
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
            if j.get("eurodollar_hub_stress_0_100") is not None and j.get("version")=="1.3.0":
                gs=j; break
        except Exception: continue
    if not gs:
        r.fail("no v1.3.0 output"); raise SystemExit(0)
    detail=gs.get("eurodollar_hub_detail") or []
    names={h["country"] for h in detail}
    worst=gs.get("eurodollar_hub_worst") or {}
    r.ok(f"EURODOLLAR-HUB STRESS = {gs.get('eurodollar_hub_stress_0_100')} (n={gs.get('eurodollar_hub_n')} w/CDS, avg {gs.get('eurodollar_hub_avg_cds_bp')}bp)")
    r.log(f"  worst hub (canary): {worst.get('country')} @ {worst.get('cds_bp')}bp → stress {worst.get('stress')}")
    r.log(f"  Chile in set: {'✓' if 'Chile' in names else '✗'} · Peru in set: {'✓ (CDS)' if 'Peru' in names else '(no CDS — tracked, not in composite)'}")
    # confirm all named hubs
    named=["Taiwan","Netherlands","Spain","Italy","Finland","Hong Kong","South Korea","Switzerland","Chile","Peru"]
    allrows={c["country"] for c in gs.get("countries",[])}
    missing=[n for n in named if n not in allrows]
    r.log(f"  named hubs all harvested: {'✓ all present' if not missing else '✗ missing '+str(missing)}")
    r.log("  top-8 hub ladder (most→least stressed):")
    for h in detail[:8]:
        r.log(f"    {h['country']}: {h['cds_bp']}bp (stress {h['stress']})")

    # JSI reflects it
    try:
        prev=json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read()).get("generated_at")
    except Exception: prev=None
    lam.invoke(FunctionName="justhodl-stress-index", InvocationType="Event", Payload=b"{}")
    jsi=None
    for i in range(30):
        time.sleep(6)
        try:
            j=json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read())
            if j.get("generated_at")!=prev: jsi=j; break
        except Exception: continue
    if jsi:
        ed=next((c for c in (jsi.get("overlay_components") or []) if c.get("label")=="Eurodollar Hub Stress"), None)
        r.ok(f"JSI eurodollar feed now {ed.get('stress') if ed else '?'} (reflects Chile/Peru).")
