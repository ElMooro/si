"""ops 3369 — additive: add Finland (ECB SovCISS + Eurostat), Spain (already in), and
South Korea (FRED 10Y yield proxy) to sovereign-stress; Singapore/HK/Taiwan listed honestly
as data_unavailable (no sovereign-yield feed). KEEPS all 7 existing euro-area sovereigns.
VERIFY: Finland in SovCISS; South Korea has real yield-based stress; SG/HK/TW named but flagged."""
import json, time
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"
sc = json.loads(Path("aws/lambdas/justhodl-sovereign-stress/config.json").read_text())

with report("3369_add_sovereigns") as r:
    r.section("Deploy sovereign-stress (Finland + Asia additive)")
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
    r.log(f"  return: {resp['Payload'].read().decode()[:180]}")
    time.sleep(3)
    ss = json.loads(s3.get_object(Bucket=BUCKET, Key="data/sovereign-stress.json")["Body"].read())

    sov = ss.get("sovereign_stress_sovciss") or {}
    asia = ss.get("asia_sovereigns") or {}
    r.section("Verify additions (existing 7 kept?)")
    r.log(f"  SovCISS countries: {list(sov.keys())}")
    r.log(f"  Asia sovereigns: {list(asia.keys())}")
    existing = {"euro_area","germany","france","italy","spain","portugal","greece"}
    if existing <= set(sov.keys()):
        r.ok(f"all 7 original euro-area sovereigns KEPT.")
    else:
        r.fail(f"lost some originals: {existing - set(sov.keys())}")
    if "finland" in sov:
        r.ok(f"FINLAND added — SovCISS {sov['finland'].get('level')} pctile {sov['finland'].get('percentile_5y')} {sov['finland'].get('status')}")
    else:
        r.fail("finland missing from SovCISS")
    kr = asia.get("south_korea") or {}
    if kr.get("stress_0_100") is not None:
        r.ok(f"SOUTH KOREA added — 10Y {kr.get('sovereign_10y_yield_pct')}% stress {kr['stress_0_100']} ({kr.get('status')}) via {kr.get('basis')}")
    else:
        r.fail(f"south korea not computed: {kr}")
    for c in ("singapore","hong_kong","taiwan"):
        e = asia.get(c) or {}
        r.log(f"  {c}: {'data_unavailable (honest, no yield feed)' if e.get('data_unavailable') else e}")
    r.log(f"  errors: {len(ss.get('errors') or [])} — {ss.get('errors')}")
    r.log(f"  europe score: {(ss.get('europe_stress') or {}).get('score_0_100')}")
