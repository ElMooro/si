"""ops 3361 — deploy JSI v1.5.0. Completes the brain's funding-plumbing directive with two
FRED-computed overlay signals (regime-shift-aware transforms):
  · RRP Drain   — RRPONTSYD 63d change z-scored (rising = cash draining to Fed = risk-off).
                  Avoids the 2021-23 $2.5T policy-level trap by using change, not level.
  · SOFR Spike  — SOFR-IORB spread z-scored (Sept-2019-style positive spike = funding stress).

VERIFY: both plumbing signals live in overlay with sane scores; overlay feed count up;
index coherent.
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

with report("3361_jsi_plumbing_overlay") as r:
    r.section("Deploy JSI v1.5.0 (RRP drain + SOFR spike)")
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
    labels = {c.get("label"): c for c in oc}
    r.log(f"JSI v{jsi.get('version')} jsi={jsi.get('jsi')} overlay={jsi.get('overlay_score')} feeds={len(oc)} live={jsi.get('n_overlay_live')}")
    rrp = labels.get("RRP Drain (liquidity)")
    sofr = labels.get("SOFR Spike (funding)")
    r.log(f"  RRP Drain: {rrp.get('stress') if rrp else 'MISSING'} (63d chg {rrp.get('raw') if rrp else '?'} $bn)")
    r.log(f"  SOFR Spike: {sofr.get('stress') if sofr else 'MISSING'} (spread {sofr.get('raw') if sofr else '?'} bps)")

    if rrp and rrp.get("stress") is not None:
        r.ok(f"RRP DRAIN live — {rrp['stress']} (brain: 'reverse repo very very important').")
    else:
        r.fail("RRP Drain not live")
    if sofr and sofr.get("stress") is not None:
        r.ok(f"SOFR SPIKE live — {sofr['stress']} (brain: 'SOFR spikes = flashing red light').")
    else:
        r.fail("SOFR Spike not live")
    if jsi.get("percentile_since_1990") is not None:
        r.ok(f"index coherent — {jsi['percentile_since_1990']}th pctile since 1990, regime {jsi.get('regime')}.")

    r.section("Full overlay")
    for c in sorted(oc, key=lambda x: -(x.get("stress") or -1)):
        r.log(f"  {c.get('label')}: {c.get('stress')}")
