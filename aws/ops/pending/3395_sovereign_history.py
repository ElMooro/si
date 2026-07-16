"""ops 3395 — historical snapshotting for the sovereign barometer. global-sovereign v1.4.0
appends each run's reading to data/global-sovereign-history.json (deduped latest-per-day,
~3yr retention) and computes percentile-of-own-history + 7d/30d trend + 90pt sparkline data.
Page gains percentile chip, trend arrows, sparkline. First run seeds history (n=1)."""
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
gc = json.loads(Path("aws/lambdas/justhodl-global-sovereign/config.json").read_text())

with report("3395_sovereign_history") as r:
    r.section("Deploy global-sovereign v1.4.0 (history snapshotting)")
    deploy_lambda(report=r, function_name="justhodl-global-sovereign",
                  source_dir=Path("aws/lambdas/justhodl-global-sovereign/source"),
                  env_vars=gc.get("env", {}),
                  eb_rule_name=gc["schedule"]["rule_name"], eb_schedule=gc["schedule"]["cron"],
                  timeout=gc["timeout"], memory=gc["memory"],
                  description=(gc.get("description") or "")[:256],
                  create_function_url=True, smoke=False)
    lam.invoke(FunctionName="justhodl-global-sovereign", InvocationType="Event", Payload=b"{}")
    r.log("harvesting + seeding history…")
    d=None
    for i in range(30):
        time.sleep(6)
        try:
            j=json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-sovereign.json")["Body"].read())
            if j.get("version")=="1.4.0" and j.get("eurodollar_hub_stress_0_100") is not None:
                d=j; break
        except Exception: continue
    if not d:
        r.fail("no v1.4.0 output"); raise SystemExit(0)
    r.ok(f"barometer live: stress {d.get('eurodollar_hub_stress_0_100')}, worst {(d.get('eurodollar_hub_worst') or {}).get('country')}")
    r.log(f"  history_n: {d.get('eurodollar_hub_history_n')} (seeds at 1, grows twice daily)")
    r.log(f"  percentile: {d.get('eurodollar_hub_percentile')} (needs ≥3 points to compute)")
    r.log(f"  chg_7d: {d.get('eurodollar_hub_chg_7d')} · chg_30d: {d.get('eurodollar_hub_chg_30d')} (build over time)")
    # verify the history file itself exists
    try:
        hist=json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-sovereign-history.json")["Body"].read())
        r.ok(f"history file written — {len(hist)} snapshot(s): {hist[-1] if hist else 'none'}")
    except Exception as e:
        r.fail(f"history file: {e}")
