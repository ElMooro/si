"""ops_4181 — GRES + COT old-crop + back-month labels: settle trio, invoke, fire."""
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
    with report("4181_trio") as rep:
        rep.heading("ops 4181 — GRES / old-crop / back-months")
        checks = [("feed v1.7 settled",
                   settle(rep, "justhodl-families-feed",
                          "families-feed v1.7 ops4181 gres")),
                  ("cot v1.2 settled",
                   settle(rep, "justhodl-cot-feed",
                          "cot-feed v1.2 ops4181 old-crop")),
                  ("vault v3.25.0 settled",
                   settle(rep, "justhodl-tradingview",
                          "tradingview-vault v3.25.0 ops4181 "
                          "gres-old-months"))]
        lam.invoke(FunctionName="justhodl-families-feed",
                   InvocationType="RequestResponse", Payload=b"{}")
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(GRES=c.get("GRES"))
        checks.append(("GRES >= 60", (c.get("GRES") or 0) >= 60))
        r = lam.invoke(FunctionName="justhodl-cot-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        cd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cot-feed.json")["Body"].read())
        rep.kv(cot_resolved=cd.get("resolved"), cot_wanted=cd.get("wanted"))
        checks.append(("cot resolved >= 240",
                       (cd.get("resolved") or 0) >= 240))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired — 4182 converts")
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"TRIO WIRED — GRES={c.get('GRES')} "
               f"cot={cd.get('resolved')}")


if __name__ == "__main__":
    main()
