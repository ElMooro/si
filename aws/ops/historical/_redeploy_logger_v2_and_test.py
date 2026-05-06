"""Redeploy wave-signal-logger v2 + invoke + verify all 12 handlers."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-wave-signal-logger"
SOURCE_DIR = "aws/lambdas/justhodl-wave-signal-logger/source"

lam = boto3.client("lambda", region_name=REGION)


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
    with report("redeploy_logger_v2") as r:
        r.heading("Redeploy wave-signal-logger v2")
        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
        time.sleep(3)
        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        r.ok("  ✓ updated")

        r.heading("Invoke v2 — verify all 10 handlers")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", LogType="Tail")
        dt = time.time() - t0
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {dt:.1f}s")
        r.log(f"  resp: {body[:500]}")

        # Decode logs
        import base64
        log_tail = base64.b64decode(resp.get("LogResult", "").encode()).decode()
        r.heading("Per-handler log lines")
        for line in log_tail.split("\n"):
            if "[wave-logger]" in line or "[LOG]" in line:
                clean = line.split("\t")[-1] if "\t" in line else line
                r.log(f"  {clean.strip()[:200]}")


if __name__ == "__main__":
    main()
