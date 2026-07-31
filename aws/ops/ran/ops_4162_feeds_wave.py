"""ops_4162 — feeds wave: families v1.4 (six WB families) + symbol-feed v1.4 (US fallback)."""
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
    with report("4162_feeds_wave") as rep:
        rep.heading("ops 4162 — feeds wave")
        checks = [("families v1.4 settled",
                   settle(rep, "justhodl-families-feed",
                          "families-feed v1.4 ops4162 wb-wave")),
                  ("symbol-feed v1.4 settled",
                   settle(rep, "justhodl-symbol-feed",
                          "symbol-feed v1.4 ops4162 us-fallback"))]
        r = lam.invoke(FunctionName="justhodl-families-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(fam_err=r.get("FunctionError"))
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(**c)
        checks += [("GDG >= 100", (c.get("GDG") or 0) >= 100),
                   ("BOT >= 120", (c.get("BOT") or 0) >= 120),
                   ("DIR >= 90", (c.get("DIR") or 0) >= 90),
                   ("TOT >= 80", (c.get("TOT") or 0) >= 80)]
        r2 = lam.invoke(FunctionName="justhodl-symbol-feed",
                        InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(sf_err=r2.get("FunctionError"),
               sf_out=r2["Payload"].read().decode()[:110])
        sd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/symbol-feed.json")["Body"].read())
        rep.kv(targets=sd.get("targets"), resolved=sd.get("resolved"))
        checks.append(("resolved >= 1300 (US fallback wave)",
                       (sd.get("resolved") or 0) >= 1300))
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"FEEDS WAVE — families {c} | symfeed {sd.get('resolved')}")


if __name__ == "__main__":
    main()
