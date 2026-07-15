"""ops 3350 — deploy + verify JustHodl Stress Index (JSI) engine.

New unified stress index: FRED 1990 spine (VIX/NFCI/KCFSI/FSI/curve/OAS, z-scored on own
history, logistic-mapped, blended) + 12 live stress-feed overlay. Publishes data/jsi.json
+ data/jsi-history.json with percentile-since-1990 and crisis markers.

VERIFY (async invoke + S3 poll):
  (a) history actually starts ~1990 (span start < 1992),
  (b) a real multi-thousand-point daily series exists,
  (c) percentile_since_1990 is populated,
  (d) overlay wired (>=6 live feeds),
  (e) historical max lands near a known crisis (2008/2020).
"""
import json
import time
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

FN = "justhodl-stress-index"
SRC = Path(f"aws/lambdas/{FN}/source")
CFG = json.loads(Path(f"aws/lambdas/{FN}/config.json").read_text())
ENV = CFG["env"]
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/jsi.json"
DESCRIPTION = (CFG.get("description") or "")[:256]
LONG = Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 0})

with report("3350_jsi_engine_deploy") as r:
    r.section("Deploy JSI engine")
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
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode()).get("generated_at")
    except Exception:
        prev = None
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    r.log("async invoke fired; polling (FRED pulls can take ~30-60s)…")

    payload = None
    for attempt in range(30):
        time.sleep(6)
        try:
            obj = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode())
        except Exception:
            continue
        if obj.get("generated_at") and obj.get("generated_at") != prev:
            payload = obj
            r.log(f"fresh write after ~{(attempt+1)*6}s")
            break
    if payload is None:
        r.fail("no fresh jsi.json within ~3 min")
        raise SystemExit(0)

    if not payload.get("ok"):
        r.fail(f"engine error: {payload.get('error')}")
        raise SystemExit(0)

    span = payload.get("history_span") or {}
    ext = payload.get("historical_extremes") or {}
    r.log(f"JSI now={payload.get('jsi')} spine={payload.get('jsi_spine')} overlay={payload.get('overlay_score')} regime={payload.get('regime')}")
    r.log(f"percentile_since_1990={payload.get('percentile_since_1990')}")
    r.log(f"history: {span.get('start')} → {span.get('end')} (n={span.get('n')})")
    r.log(f"extremes: max={ext.get('max')} min={ext.get('min')}")
    r.log(f"overlay live: {payload.get('n_overlay_live')}/12")
    sc = payload.get("spine_components") or {}
    r.log(f"spine components: {list(sc.keys())}")
    for sid, c in list(sc.items())[:7]:
        r.log(f"  {sid}: {c.get('label')} raw={c.get('raw')} stress={c.get('stress')} z={c.get('z')} since={c.get('inception')}")

    ok = True
    start = span.get("start", "9999")
    if start < "1992":
        r.ok(f"1990 SPINE CONFIRMED — history starts {start} (n={span.get('n')}).")
    else:
        r.fail(f"history does not reach 1990: starts {start}"); ok = False

    if span.get("n", 0) > 2000:
        r.ok(f"deep daily series — {span.get('n')} points.")
    else:
        r.log(f"⚠ series shorter than expected: {span.get('n')} points")

    if payload.get("percentile_since_1990") is not None:
        r.ok(f"percentile-in-history live: current JSI at {payload.get('percentile_since_1990')}th pctile since 1990.")
    else:
        r.fail("percentile missing"); ok = False

    if (payload.get("n_overlay_live") or 0) >= 6:
        r.ok(f"overlay wired — {payload.get('n_overlay_live')} live feeds.")
    else:
        r.log(f"⚠ few overlay feeds live: {payload.get('n_overlay_live')}")

    mx = (ext.get("max") or {}).get("date", "")
    if mx[:4] in ("2008", "2020", "2009"):
        r.ok(f"sanity: all-time-max stress at {mx} (matches a known crisis).")
    else:
        r.log(f"⚠ max-stress date {mx} — inspect (may be legit but verify).")
