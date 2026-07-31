"""ops_4210 — systematic fixes: NFS-lottery vault, root-admit feed, fire."""
import io
import json
import re
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

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=290,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, timeout=90):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:140]


def settle(rep, name, mark):
    src = (ROOT / "lambdas" / name / "source" /
           "lambda_function.py").read_text()
    if mark not in src:
        rep.fail(f"  {name}: marker missing in checkout")
        return False
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
    with report("4210_systematic_fixes") as rep:
        rep.heading("ops 4210 — lottery + root admission")
        checks = [("sf v1.8 settled",
                   settle(rep, "justhodl-symbol-feed",
                          "symbol-feed v1.8 ops4210 root-admit")),
                  ("vault v3.30.0 settled",
                   settle(rep, "justhodl-tradingview",
                          "tradingview-vault v3.30.0 ops4210 "
                          "nfs-lottery"))]
        r = lam.invoke(FunctionName="justhodl-symbol-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(sf=r["Payload"].read().decode()[:90])
        sd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/symbol-feed.json")["Body"].read())
        p = sd.get("prices") or {}
        for sy in ("DX1!", "DJIA1!", "CN1!"):
            rep.log(f"  root {sy}: {json.dumps(p.get(sy))[:90]}")
        rep.kv(sf_resolved=sd.get("resolved"))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired (lottery pass-1) — 4211 converts")
        failed = [l for l, k2 in checks if not k2]
        for l, k2 in checks:
            (rep.ok if k2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"SYSTEMATIC FIXES LIVE — sf={sd.get('resolved')}")


if __name__ == "__main__":
    main()
