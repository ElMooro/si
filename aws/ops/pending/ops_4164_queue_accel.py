"""ops_4164 — QUEUE ACCELERATOR: symbol-feed x2 back-to-back rotations."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=290,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"


def settle(rep, name, mark):
    src = (ROOT / "lambdas" / name / "source" /
           "lambda_function.py").read_text()
    assert mark in src
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
        if name == "justhodl-tradingview":
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
    for att in range(5):
        try:
            lam.update_function_code(FunctionName=name,
                                     ZipFile=buf.getvalue(), Publish=True)
            break
        except Exception:
            time.sleep(8)
    for i in range(35):
        try:
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=name)["Code"]
                    ["Location"], timeout=60).read())).read(
                    "lambda_function.py").decode()
                if mark in dep:
                    rep.ok(f"  {name} settled at loop {i}")
                    return True
        except Exception:
            pass
        time.sleep(9)
    rep.fail(f"  {name} never settled")
    return False




def main():
    with report("4164_queue_accel") as rep:
        rep.heading("ops 4164 — queue accelerator (feed x2)")
        base = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/symbol-feed.json")["Body"].read()
        ).get("resolved") or 0
        rep.kv(resolved_before=base)
        for k in (1, 2):
            r = lam.invoke(FunctionName="justhodl-symbol-feed",
                           InvocationType="RequestResponse", Payload=b"{}")
            out = r["Payload"].read().decode()[:100]
            sd = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/symbol-feed.json")["Body"].read())
            rep.kv(**{f"round{k}_out": out,
                      f"round{k}_resolved": sd.get("resolved")})
        final = sd.get("resolved") or 0
        rep.kv(resolved_after=final, delta=final - base)
        if final < base + 120:
            rep.fail(f"accelerator gained only {final - base}")
            sys.exit(1)
        rep.ok(f"ACCEL — resolved {base} -> {final} (+{final - base})")


if __name__ == "__main__":
    main()
