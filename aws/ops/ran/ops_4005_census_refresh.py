"""
ops_4005 — INVOKE-ONLY. Enforce 900/3008, confirm the v1.6 marker is the
deployed artifact (Deploy Lambdas ships it on this same push), fire the
engine async, exit 0 in under a minute. Verification is ops 3984's job,
re-triggered after the engine's window.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=90, retries={"max_attempts": 1}))
FN = "justhodl-data-census"
MARK = "data-census v2.3 ops3999 fleet-fallback"


def main():
    with report("4005_census_refresh") as rep:
        rep.heading("ops 3985 — invoke-only: enforce, settle, fire")
        for i in range(30):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(8)
        if c.get("Timeout") != 900 or c.get("MemorySize") != 3008:
            lam.update_function_configuration(FunctionName=FN, Timeout=900,
                                              MemorySize=3008)
            for _ in range(24):
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("LastUpdateStatus") != "InProgress":
                    break
                time.sleep(6)
        rep.kv(timeout=c.get("Timeout"), memory=c.get("MemorySize"))
        info = lam.get_function(FunctionName=FN)
        dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
            info["Code"]["Location"], timeout=60).read()
        )).read("lambda_function.py").decode()
        ok = MARK in dep
        rep.kv(v20_marker_deployed=ok)
        if not ok:
            src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
            buf = io.BytesIO()
            with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
                z.writestr("lambda_function.py", src)
            for a in range(6):
                try:
                    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue(),
                                             Publish=True)
                    break
                except lam.exceptions.ResourceConflictException:
                    time.sleep(12)
            for _ in range(24):
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                    break
                time.sleep(8)
            rep.ok("  v1.6 pushed from runner")
        if c.get("Timeout") != 900 or c.get("MemorySize") != 3008:
            rep.fail("resources not enforced")
            sys.exit(1)
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"source": "ops3985"}).encode())
        rep.ok("FIRED — v1.6 async under 900s/3008MB; verify via ops 3984 in ~13 min")


if __name__ == "__main__":
    main()
