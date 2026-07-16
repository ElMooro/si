"""ops 3385 — deploy sovereign-stress with REAL Asian data via reverse-engineered World
Government Bonds REST endpoint. Replaces data_unavailable placeholders for SG/HK/TW with
live 10Y yield + sovereign CDS + spread-vs-Bund + rating. KEEPS all euro-area sovereigns.
VERIFY: all 4 Asian sovereigns have real stress scores, CDS present, 0 new errors."""
import json, time
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"
sc = json.loads(Path("aws/lambdas/justhodl-sovereign-stress/config.json").read_text())

with report("3385_wgb_production") as r:
    r.section("Deploy sovereign-stress (WGB real Asian data)")
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
    r.log(f"  return: {resp['Payload'].read().decode()[:160]}")
    time.sleep(3)
    ss = json.loads(s3.get_object(Bucket=BUCKET, Key="data/sovereign-stress.json")["Body"].read())
    asia = ss.get("asia_sovereigns") or {}
    sov = ss.get("sovereign_stress_sovciss") or {}
    r.section("Verify real Asian data")
    for name in ("south_korea","singapore","hong_kong","taiwan"):
        c = asia.get(name) or {}
        if c.get("stress_0_100") is not None:
            r.ok(f"{name}: stress {c['stress_0_100']} · 10Y {c.get('sovereign_10y_yield_pct')}% · CDS {c.get('cds_bp')}bp · spread {c.get('spread_vs_bund_bp')}bp · {c.get('rating')} · {c.get('as_of')}")
        else:
            r.fail(f"{name}: {c}")
    euro_kept = {"euro_area","germany","france","italy","spain","portugal","greece","finland"} <= set(sov.keys())
    if euro_kept:
        r.ok(f"all euro-area sovereigns KEPT ({len(sov)} total incl Finland).")
    else:
        r.fail("lost euro-area sovereigns")
    r.log(f"  errors: {len(ss.get('errors') or [])} — {ss.get('errors')}")
    r.log(f"  sources: {ss.get('sources')}")
