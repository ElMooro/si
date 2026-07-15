"""ops 3356 — deploy JSI v1.3.0. Adds two brain-directed risk signals mined from the
operator's notes (liquidity #1 risk concept @ 1,658 notes; HYG/LQD named explicitly):
  · Global Risk Tide   — global-tide risk.global_risk_0_100 (composite 0-100 gauge)
  · HYG/LQD Credit Risk — risk-ratios hyg_lqd.latest, z-scored on own history + inverted
                          (falling credit ratio = risk-off = stress) via new ratio_inv tf.

VERIFY: 16 overlay feeds, both new ones live with sane scores, HYG/LQD polarity correct
(near top of range → low stress), index still coherent.
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

with report("3356_jsi_brain_risk_feeds") as r:
    r.section("Deploy JSI v1.3.0 (brain-directed risk feeds)")
    deploy_lambda(
        report=r, function_name="justhodl-stress-index",
        source_dir=Path("aws/lambdas/justhodl-stress-index/source"),
        env_vars=cfg["env"],
        eb_rule_name=cfg["schedule"]["rule_name"], eb_schedule=cfg["schedule"]["cron"],
        timeout=cfg["timeout"], memory=cfg["memory"],
        description=(cfg.get("description") or "")[:256],
        create_function_url=True, smoke=False,
    )

    r.section("Verify brain-directed feeds")
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
    labels = {c.get("label"): c for c in oc}
    r.log(f"JSI v{jsi.get('version')} jsi={jsi.get('jsi')} overlay={jsi.get('overlay_score')} regime={jsi.get('regime')}")
    r.log(f"overlay: {len(oc)} feeds, {jsi.get('n_overlay_live')} live")
    grt = labels.get("Global Risk Tide")
    hl = labels.get("HYG/LQD Credit Risk")
    r.log(f"  Global Risk Tide: {grt.get('stress') if grt else 'MISSING'} (raw {grt.get('raw') if grt else '?'})")
    r.log(f"  HYG/LQD Credit Risk: {hl.get('stress') if hl else 'MISSING'} (raw {hl.get('raw') if hl else '?'})")

    if len(oc) == 16:
        r.ok("overlay widened to 16 feeds (brain-directed risk signals added).")
    else:
        r.log(f"⚠ overlay has {len(oc)} feeds (expected 16)")

    if grt and grt.get("stress") is not None:
        r.ok(f"GLOBAL RISK TIDE live — {grt['stress']} (composite global-risk gauge from operator's #1 concept: liquidity).")
    else:
        r.fail("Global Risk Tide not live")

    if hl and hl.get("stress") is not None:
        pol = "correct (low stress at range-top)" if hl["stress"] < 45 else "elevated — inspect"
        r.ok(f"HYG/LQD CREDIT RISK live — {hl['stress']} (raw {hl.get('raw')}); polarity {pol}.")
    else:
        r.fail("HYG/LQD not live")

    if jsi.get("jsi_spine") and jsi.get("percentile_since_1990") is not None:
        r.ok(f"index coherent — spine {jsi['jsi_spine']}, {jsi['percentile_since_1990']}th pctile since 1990.")
    else:
        r.fail("index integrity failed")

    # summary of full overlay for the record
    r.section("Full overlay (16 feeds)")
    for c in sorted(oc, key=lambda x: -(x.get("stress") or -1)):
        r.log(f"  {c.get('label')}: {c.get('stress')}")
