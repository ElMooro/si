"""ops 3389 — deploy the dedicated Global Sovereign Desk engine + page. Harvests live 10Y
yield, sovereign CDS, spread, rating, CB rate for 45 economies from WGB. VERIFY: engine
returns 40+ countries with real CDS, ranked stress, regional aggregates."""
import json, time
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"
cfg = json.loads(Path("aws/lambdas/justhodl-global-sovereign/config.json").read_text())

with report("3389_global_sovereign") as r:
    r.section("Deploy justhodl-global-sovereign")
    deploy_lambda(report=r, function_name="justhodl-global-sovereign",
                  source_dir=Path("aws/lambdas/justhodl-global-sovereign/source"),
                  env_vars=cfg.get("env", {}),
                  eb_rule_name=cfg["schedule"]["rule_name"], eb_schedule=cfg["schedule"]["cron"],
                  timeout=cfg["timeout"], memory=cfg["memory"],
                  description=(cfg.get("description") or "")[:256],
                  create_function_url=True, smoke=False)
    lam = boto3.client("lambda", region_name="us-east-1", config=LONG)
    s3 = boto3.client("s3", region_name="us-east-1", config=LONG)
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-sovereign.json")["Body"].read()).get("generated_at")
    except Exception:
        prev = None
    lam.invoke(FunctionName="justhodl-global-sovereign", InvocationType="Event", Payload=b"{}")
    r.log("harvesting 45 sovereigns (~20-30s)…")
    d = None
    for i in range(30):
        time.sleep(6)
        try:
            j = json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-sovereign.json")["Body"].read())
            if j.get("generated_at") != prev and j.get("countries"):
                d = j; break
        except Exception:
            continue
    if not d:
        r.fail("no global-sovereign.json produced"); raise SystemExit(0)

    r.section("Verify global sovereign harvest")
    r.log(f"n_countries={d.get('n_countries')} errors={d.get('n_errors')} global_avg_cds={d.get('global_avg_cds_bp')}bp")
    hi=d.get("highest_stress") or {}; lo=d.get("lowest_stress") or {}
    r.log(f"highest stress: {hi.get('country')} ({hi.get('stress_0_100')}, CDS {hi.get('cds_bp')}bp)")
    r.log(f"lowest stress: {lo.get('country')} ({lo.get('stress_0_100')}, CDS {lo.get('cds_bp')}bp)")
    r.log("regions:")
    for reg in d.get("regions", []):
        r.log(f"  {reg['region']}: avg stress {reg['avg_stress']} · avg CDS {reg['avg_cds_bp']}bp (n={reg['n']})")
    r.log("top-8 riskiest:")
    for c in d.get("countries", [])[:8]:
        r.log(f"  {c['country']}: stress {c['stress_0_100']} {c['regime']} · 10Y {c['yield_10y_pct']}% · CDS {c['cds_bp']} · {c['rating']}")

    if d.get("n_countries", 0) >= 40:
        r.ok(f"GLOBAL SOVEREIGN DESK LIVE — {d['n_countries']} sovereigns harvested.")
    else:
        r.fail(f"only {d.get('n_countries')} countries")
    with_cds = [c for c in d.get("countries",[]) if c.get("cds_bp") is not None]
    if len(with_cds) >= 30:
        r.ok(f"real CDS for {len(with_cds)} sovereigns.")
    else:
        r.log(f"⚠ CDS present for only {len(with_cds)}")
