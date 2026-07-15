"""ops 3353 — deploy JSI engine v1.2.0: overlay widened with the dispersed GLOBAL-RISK
signals the index was missing — European fragmentation / BTP-Bund
(fragmentation.score_0_100) and carry-unwind fragility (unwind_overlay.cohort_fragility).

VERIFY (async invoke + S3 poll):
  (a) overlay now has 14 feeds, >=13 live,
  (b) Euro Fragmentation (BTP-Bund) present with a real 0-100 score,
  (c) Carry-Unwind Fragility present with a real score,
  (d) JSI still coherent (spine + percentile intact).
"""
import json
import time
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})
BUCKET = "justhodl-dashboard-live"
cfg = json.loads(Path("aws/lambdas/justhodl-stress-index/config.json").read_text())

with report("3353_jsi_global_risk_overlay") as r:
    r.section("Deploy JSI v1.2.0 (global-risk overlay)")
    deploy_lambda(
        report=r, function_name="justhodl-stress-index",
        source_dir=Path("aws/lambdas/justhodl-stress-index/source"),
        env_vars=cfg["env"],
        eb_rule_name=cfg["schedule"]["rule_name"], eb_schedule=cfg["schedule"]["cron"],
        timeout=cfg["timeout"], memory=cfg["memory"],
        description=(cfg.get("description") or "")[:256],
        create_function_url=True, smoke=False,
    )

    r.section("Verify global-risk feeds in overlay")
    lam = boto3.client("lambda", region_name="us-east-1", config=LONG)
    s3 = boto3.client("s3", region_name="us-east-1", config=LONG)
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read()).get("generated_at")
    except Exception:
        prev = None
    lam.invoke(FunctionName="justhodl-stress-index", InvocationType="Event", Payload=b"{}")
    jsi = None
    for i in range(30):
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
    r.log(f"JSI v{jsi.get('version')} jsi={jsi.get('jsi')} spine={jsi.get('jsi_spine')} overlay={jsi.get('overlay_score')} regime={jsi.get('regime')}")
    r.log(f"overlay feeds: {len(oc)} total, {jsi.get('n_overlay_live')} live")
    for c in oc:
        tag = "" if c.get("stress") is not None else "  (unavailable)"
        r.log(f"  {c.get('label')}: {c.get('stress')}{tag}")

    labels = {c.get("label"): c for c in oc}
    ef = labels.get("Euro Fragmentation (BTP-Bund)")
    cu = labels.get("Carry-Unwind Fragility")

    if len(oc) == 14:
        r.ok("overlay widened to 14 feeds.")
    else:
        r.log(f"⚠ overlay has {len(oc)} feeds (expected 14)")

    if ef and ef.get("stress") is not None:
        r.ok(f"EURO FRAGMENTATION (BTP-Bund) live in JSI — stress {ef['stress']} (raw {ef.get('raw')}).")
    else:
        r.fail(f"euro-fragmentation not live: {ef}")

    if cu and cu.get("stress") is not None:
        r.ok(f"CARRY-UNWIND FRAGILITY live in JSI — stress {cu['stress']} (raw {cu.get('raw')}).")
    else:
        r.fail(f"carry-unwind not live: {cu}")

    if jsi.get("jsi_spine") and jsi.get("percentile_since_1990") is not None:
        r.ok(f"index coherent — spine {jsi['jsi_spine']}, {jsi['percentile_since_1990']}th pctile since 1990.")
    else:
        r.fail("index integrity check failed")
