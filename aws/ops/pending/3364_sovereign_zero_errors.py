"""ops 3364 — deploy sovereign-stress fix: CISS china/UK 404 now falls through to the
SS_CIN.IDX fallback (they only exist under that variant). Target: errors 2 → 0, all 4
CISS regions populate.
"""
import json, time
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"
sc = json.loads(Path("aws/lambdas/justhodl-sovereign-stress/config.json").read_text())

with report("3364_sovereign_zero_errors") as r:
    r.section("Deploy sovereign-stress CISS fallback fix")
    deploy_lambda(report=r, function_name="justhodl-sovereign-stress",
                  source_dir=Path("aws/lambdas/justhodl-sovereign-stress/source"),
                  env_vars=sc.get("env", {}),
                  eb_rule_name=sc["schedule"]["rule_name"], eb_schedule=sc["schedule"]["cron"],
                  timeout=sc["timeout"], memory=sc["memory"],
                  description=(sc.get("description") or "")[:256],
                  create_function_url=False, smoke=False)
    lam = boto3.client("lambda", region_name="us-east-1", config=LONG)
    s3 = boto3.client("s3", region_name="us-east-1", config=LONG)
    resp = lam.invoke(FunctionName="justhodl-sovereign-stress",
                      InvocationType="RequestResponse", Payload=b"{}")
    r.log(f"  return: {resp['Payload'].read().decode()[:200]}")
    time.sleep(3)
    ss = json.loads(s3.get_object(Bucket=BUCKET, Key="data/sovereign-stress.json")["Body"].read())
    errs = ss.get("errors") or []
    ciss = ss.get("systemic_stress_ciss") or {}
    r.log(f"  errors: {len(errs)} → {errs}")
    r.log(f"  CISS regions: {list(ciss.keys())}")
    es = ss.get("europe_stress") or {}
    r.log(f"  europe score={es.get('score_0_100')} regime={es.get('regime')}")
    if len(errs) == 0:
        r.ok("SOVEREIGN-STRESS CLEAN — 0 errors, all 4 CISS regions populate.")
    elif len(errs) < 2:
        r.ok(f"improved to {len(errs)} error(s): {errs}")
    else:
        r.fail(f"still {len(errs)} errors: {errs}")
    if set(ciss.keys()) >= {"euro_area","united_states","china","united_kingdom"}:
        r.ok("all 4 CISS regions (EA/US/CN/UK) now live.")
    else:
        r.log(f"⚠ missing CISS regions: {{'euro_area','united_states','china','united_kingdom'} - set(ciss.keys())}")
