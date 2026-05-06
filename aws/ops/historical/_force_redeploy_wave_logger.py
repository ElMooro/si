"""Force-redeploy justhodl-wave-signal-logger to pick up eurodollar_stress dispatch entry.

The auto-deploy workflow apparently didn't catch the source change in the earlier
multi-Lambda commit. This script directly packages and uploads.
"""
import io
import os
import time
import zipfile
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-wave-signal-logger/source"

lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


def make_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, source_dir)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("force_redeploy_wave_logger") as r:
        r.heading("1) Force-redeploy wave-signal-logger")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        lam.update_function_code(FunctionName="justhodl-wave-signal-logger", ZipFile=zb)
        for _ in range(15):
            cfg = lam.get_function(FunctionName="justhodl-wave-signal-logger")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        r.ok(f"  ✓ deployed at {cfg.get('LastModified')}")

        r.heading("2) Invoke and confirm eurodollar_stress in dispatch")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-wave-signal-logger", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body}")

        # Parse and check eurodollar_stress in by_type
        import json
        try:
            outer = json.loads(body)
            inner = json.loads(outer["body"])
            by_type = inner.get("by_type", {})
            r.log("")
            r.log(f"  total signal types in dispatch: {len(by_type)}")
            r.log(f"  eurodollar_stress in dispatch:  {'eurodollar_stress' in by_type}")
            if "eurodollar_stress" in by_type:
                r.ok(f"  ✓ eurodollar_stress dispatch entry present (n_signals={by_type['eurodollar_stress']})")
            else:
                r.log("  ✗ STILL MISSING — translator function or dispatch entry not deployed")
        except Exception as e:
            r.log(f"  ✗ parse: {e}")


if __name__ == "__main__":
    main()
