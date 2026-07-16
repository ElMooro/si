"""ops 3397 — fuse eurodollar-hub distress into the risk + black-swan engines:
  · risk-regime: new risk-off block (0.12 weight), high hub distress = flight-to-quality
  · crisis-composite: new DEFCON component (0.15 weight), 'Eurodollar-hub sovereign distress'
tail-risk deliberately LEFT CLEAN (self-contained options-implied density; CDS would pollute).
VERIFY: both engines pick up the signal and recompute."""
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

def deploy_and_run(r, fn, out_key, checks):
    c = cfg(fn)
    sch = c.get("schedule", {})
    rule = sch.get("rule_name") or sch.get("name")
    cron = sch.get("cron") or sch.get("expression")
    env = c.get("env") or c.get("environment") or {}
    deploy_lambda(report=r, function_name=fn,
                  source_dir=Path(f"aws/lambdas/{fn}/source"),
                  env_vars=env,
                  eb_rule_name=rule, eb_schedule=cron,
                  timeout=c["timeout"], memory=c["memory"],
                  description=(c.get("description") or "")[:256],
                  create_function_url=c.get("create_function_url", False), smoke=False)
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key=out_key)["Body"].read()).get("generated_at")
    except Exception:
        prev = None
    lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
    for i in range(30):
        time.sleep(6)
        try:
            j = json.loads(s3.get_object(Bucket=BUCKET, Key=out_key)["Body"].read())
            if j.get("generated_at") != prev:
                return j
        except Exception:
            continue
    return None

with report("3397_fuse_risk_crisis") as r:
    r.section("Fuse into risk-regime (risk-off block)")
    rr = deploy_and_run(r, "justhodl-risk-regime", "data/risk-regime.json", None)
    if rr:
        blocks = rr.get("blocks") or rr.get("components") or {}
        eh = (rr.get("eurodollar_hub") or (isinstance(blocks, dict) and blocks.get("eurodollar_hub")))
        # the block detail is under results in the payload
        detail = rr.get("eurodollar_hub") or {}
        r.log(f"  risk-regime score={rr.get('score')} regime={rr.get('regime')}")
        if detail or "eurodollar_hub" in json.dumps(rr):
            r.ok(f"eurodollar_hub block LIVE in risk-regime: {json.dumps(detail)[:120] if detail else 'present in payload'}")
        else:
            r.log(f"  ⚠ eurodollar_hub not visibly in payload — check keys: {list(rr.keys())[:15]}")
    else:
        r.fail("risk-regime did not refresh")

    r.section("Fuse into crisis-composite (DEFCON component)")
    cc = deploy_and_run(r, "justhodl-crisis-composite", "data/crisis-composite.json", None)
    if cc:
        comps = cc.get("components") or []
        hub = next((c for c in comps if "hub" in (c.get("label","").lower()) or "eurodollar-hub" in json.dumps(c).lower()), None)
        r.log(f"  crisis-composite master={cc.get('master_score') or cc.get('score')} DEFCON={cc.get('defcon') or cc.get('level')}")
        if hub:
            r.ok(f"eurodollar-hub component LIVE in crisis-composite: {hub.get('label')} = {hub.get('value') or hub.get('score')} (weight {hub.get('weight')})")
        else:
            names=[c.get('label') for c in comps]
            r.log(f"  ⚠ hub component not found. components: {names}")
    else:
        r.fail("crisis-composite did not refresh")

    r.section("tail-risk left clean (by design)")
    r.ok("tail-risk NOT modified — self-contained options-implied density; CDS would pollute the model.")
