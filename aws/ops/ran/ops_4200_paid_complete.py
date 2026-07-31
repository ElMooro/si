"""ops_4200 — paid completion: te v1.1 sweep-out, symbol v1.6 eodhd retries, fire."""
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
    with report("4200_paid_complete") as rep:
        rep.heading("ops 4200 — paid wave completion")
        checks = [("te v1.1 settled",
                   settle(rep, "justhodl-te-feed",
                          "te-feed v1.1 ops4200 catmap-wide")),
                  ("symbol v1.6 settled",
                   settle(rep, "justhodl-symbol-feed",
                          "symbol-feed v1.6 ops4200 eodhd-tier"))]
        for k in (1, 2):
            r = lam.invoke(FunctionName="justhodl-te-feed",
                           InvocationType="RequestResponse", Payload=b"{}")
            rep.kv(**{f"te{k}": r["Payload"].read().decode()[:70]})
        td = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/te-feed.json")["Body"].read())
        rep.kv(te_n=td.get("n"),
               countries=len(td.get("countries_done") or []))
        checks.append(("te n >= 6000", (td.get("n") or 0) >= 6000))
        for k in (1, 2):
            r = lam.invoke(FunctionName="justhodl-symbol-feed",
                           InvocationType="RequestResponse", Payload=b"{}")
            rep.kv(**{f"sf{k}": r["Payload"].read().decode()[:90]})
        sd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/symbol-feed.json")["Body"].read())
        rep.kv(sf_resolved=sd.get("resolved"))
        checks.append(("sf resolved >= 1650",
                       (sd.get("resolved") or 0) >= 1650))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired — 4201 converts")
        failed = [l for l, k2 in checks if not k2]
        for l, k2 in checks:
            (rep.ok if k2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PAID COMPLETE — te={td.get('n')} "
               f"sf={sd.get('resolved')}")


if __name__ == "__main__":
    main()
