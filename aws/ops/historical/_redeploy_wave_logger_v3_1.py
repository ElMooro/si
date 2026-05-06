"""Redeploy justhodl-wave-signal-logger with fixed divergence + cot translators, invoke, verify counts.

Bug fixed:
  - z_score / percentile in the source data were strings — now cast to float.
  - cot translator now uses 'extreme' flag ('high'/'low') as primary trigger.

Expected after fix (real data 2026-05-04):
  - divergence_extreme: 1 (nasdaq_long_rates QQQ z=2.16, RICH → DOWN)
  - cot_extreme:        1 (HG/Copper at 98.5%, extreme=high → DOWN via JJC proxy)
"""
import io
import json
import os
import time
import zipfile
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-wave-signal-logger"
SOURCE_DIR = "aws/lambdas/justhodl-wave-signal-logger/source"

lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


def make_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, SOURCE_DIR)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("redeploy_wave_logger_v3_1") as r:
        r.heading("1) Redeploy v3.1 with float casts in divergence + cot translators")
        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")
        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        # wait for active
        for _ in range(15):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed at {cfg.get('LastModified')}")
                break
            time.sleep(2)

        r.heading("2) Invoke")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:600]}")

        r.heading("3) Verify counts in DDB (last 5 min)")
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        tbl = ddb.Table("justhodl-signals")
        # scan recent
        items = []
        last_key = None
        pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("logged_at").gte(cutoff)}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            r2 = tbl.scan(**kw)
            items.extend(r2.get("Items", []))
            last_key = r2.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 8:
                break
        from collections import Counter
        types = Counter()
        for it in items:
            types[it.get("signal_type", "?")] += 1
        r.log(f"  total recent signals (5min): {len(items)}")
        for t, n in types.most_common():
            star = "★ " if t in ("divergence_extreme", "cot_extreme") else "  "
            r.log(f"  {star}{t:30s} n={n}")

        # Show actual divergence/cot records that were logged
        r.heading("4) Sample fixed-type signal records")
        for it in items:
            if it.get("signal_type") in ("divergence_extreme", "cot_extreme"):
                r.log(f"  {it.get('signal_type')}:")
                r.log(f"    value: {it.get('signal_value')}")
                r.log(f"    pred: {it.get('predicted_direction')}, conf: {it.get('confidence')}")
                r.log(f"    against: {it.get('measure_against')}, baseline: ${it.get('baseline_price')}")
                r.log(f"    rationale: {str(it.get('rationale', ''))[:120]}")


if __name__ == "__main__":
    main()
