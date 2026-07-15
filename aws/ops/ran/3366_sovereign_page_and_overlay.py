"""ops 3366 — (a) JSI v1.6.0: wire sovereign-stress europe_stress.score_0_100 as the 19th
overlay feed (Global Risk group). (b) dedicated sovereign-stress.html page ships via Pages.
VERIFY: JSI overlay now 19 feeds incl 'European Sovereign Stress' with real score."""
import json, time
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"
cfg = json.loads(Path("aws/lambdas/justhodl-stress-index/config.json").read_text())

with report("3366_sovereign_overlay") as r:
    r.section("Deploy JSI v1.6.0 (sovereign 19th feed)")
    deploy_lambda(report=r, function_name="justhodl-stress-index",
                  source_dir=Path("aws/lambdas/justhodl-stress-index/source"),
                  env_vars=cfg["env"],
                  eb_rule_name=cfg["schedule"]["rule_name"], eb_schedule=cfg["schedule"]["cron"],
                  timeout=cfg["timeout"], memory=cfg["memory"],
                  description=(cfg.get("description") or "")[:256],
                  create_function_url=True, smoke=False)
    lam = boto3.client("lambda", region_name="us-east-1", config=LONG)
    s3 = boto3.client("s3", region_name="us-east-1", config=LONG)
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
    oc = jsi.get("overlay_components") or []
    sov = next((c for c in oc if c.get("label")=="European Sovereign Stress"), None)
    r.log(f"JSI v{jsi.get('version')} overlay feeds={len(oc)} live={jsi.get('n_overlay_live')}")
    r.log(f"  European Sovereign Stress: {sov.get('stress') if sov else 'MISSING'} (raw {sov.get('raw') if sov else '?'})")
    if sov and sov.get("stress") is not None:
        r.ok(f"SOVEREIGN wired as 19th feed — stress {sov['stress']}, in Global Risk group.")
    else:
        r.fail("sovereign feed not live")
    if len(oc) == 19:
        r.ok("overlay now 19 feeds.")
    else:
        r.log(f"overlay has {len(oc)} feeds")
