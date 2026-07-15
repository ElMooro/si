"""ops 3346 — carry-surface v1.4.0: remove crypto entirely (it dominated the ranking and
page via ×3×365 funding annualization) + page UX redo shipped separately in carry.html.

Crypto class, OKX helper, CRYPTO_UNIVERSE, compute_crypto_carry, methodology entry and the
page's crypto filter tab / regime tile all removed. Surface is now 4 classes: equity, FX,
fixed income, commodity.

VERIFY (async invoke + S3 poll — the reliable pattern for this heavier Lambda):
  (a) no crypto in any asset's asset_class,
  (b) 4 classes present with sane counts,
  (c) unwind overlay still regime-joined (no regression),
  (d) version 1.4.0.
"""
import json
import time
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

FN = "justhodl-carry-surface"
SRC = Path(f"aws/lambdas/{FN}/source")
CFG = json.loads(Path(f"aws/lambdas/{FN}/config.json").read_text())
ENV = CFG["env"]
BUCKET = ENV["S3_BUCKET"]
OUT_KEY = ENV["OUT_KEY"]
DESCRIPTION = (CFG.get("description") or "")[:256]
LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})

with report("3346_carry_remove_crypto") as r:
    r.section("Deploy carry-surface v1.4.0 (crypto removed)")
    deploy_lambda(
        report=r, function_name=FN, source_dir=SRC, env_vars=ENV,
        eb_rule_name=CFG["schedule"]["rule_name"], eb_schedule=CFG["schedule"]["cron"],
        timeout=CFG["timeout"], memory=CFG["memory"], description=DESCRIPTION,
        create_function_url=True, smoke=False,
    )

    r.section("Verify: async invoke + poll S3")
    lam = boto3.client("lambda", region_name=CFG["region"], config=LONG)
    s3 = boto3.client("s3", region_name=CFG["region"], config=LONG)
    try:
        prev_gen = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode()).get("generated_at")
    except Exception:
        prev_gen = None
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    r.log("async invoke fired; polling S3…")

    payload = None
    for attempt in range(24):
        time.sleep(5)
        try:
            obj = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode())
        except Exception:
            continue
        if obj.get("generated_at") and obj.get("generated_at") != prev_gen:
            payload = obj
            r.log(f"fresh write after ~{(attempt+1)*5}s")
            break
    if payload is None:
        r.fail("no fresh carry-surface.json within 2 min")
        raise SystemExit(0)

    assets = payload.get("all_assets", [])
    classes = {}
    for a in assets:
        if a.get("carry_pct") is not None:
            classes[a["asset_class"]] = classes.get(a["asset_class"], 0) + 1
    has_crypto = any(a.get("asset_class") == "crypto" for a in assets)
    u = payload.get("unwind_overlay") or {}
    rr = u.get("regime") or {}

    r.log(f"version={payload.get('version')} n_assets={payload.get('n_assets')} classes={classes}")
    r.log(f"unwind: cohort={u.get('cohort_fragility')} regime={rr.get('regime')} roro={rr.get('roro_score')}")

    if not has_crypto and "crypto" not in classes:
        r.ok(f"CRYPTO REMOVED — 4 classes only: {classes}.")
    else:
        r.fail("crypto still present in output.")

    if len(classes) == 4 and classes.get("equity", 0) > 50:
        r.ok(f"universe intact — {sum(classes.values())} assets across {len(classes)} classes.")
    else:
        r.fail(f"unexpected class shape: {classes}")

    if rr.get("available") and rr.get("regime"):
        r.ok(f"unwind overlay still regime-joined ({rr.get('regime')}).")
    else:
        r.fail("unwind regime join regressed.")

    if payload.get("version") == "1.4.0":
        r.ok("version 1.4.0 live")
    else:
        r.fail(f"version mismatch: {payload.get('version')}")
