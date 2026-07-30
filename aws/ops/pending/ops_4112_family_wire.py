"""ops_4112 — settle vault v3.15.0, ASYNC invoke (4096 lesson), poll the
artifact, verify family flips + ground-truth spots."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=60, retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tradingview"
MARK = "tradingview-vault v3.15.0 ops4112 family-adapters"


def main():
    with report("4112_family_wire") as rep:
        rep.heading("ops 4112 — family adapters: settle, fire, verify")
        checks = []
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src and "_family_try" in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        ok = False
        for _ in range(30):
            try:
                lam.update_function_code(FunctionName=FN,
                                         ZipFile=buf.getvalue(), Publish=True)
            except Exception:
                pass
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("State") == "Active" and \
                        c.get("LastUpdateStatus") != "InProgress":
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                        lam.get_function(FunctionName=FN)["Code"]["Location"],
                        timeout=60).read())).read(
                        "lambda_function.py").decode()
                    if MARK in dep:
                        ok = True
                        break
            except Exception:
                pass
            time.sleep(8)
        checks.append(("v3.15.0 settled in deployed zip", ok))

        before = s3.head_object(Bucket=BUCKET,
                                Key="data/tradingview.json")["LastModified"]
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        rep.ok("  async invoke fired; polling artifact")
        moved = False
        v = None
        for _ in range(40):
            time.sleep(15)
            h = s3.head_object(Bucket=BUCKET, Key="data/tradingview.json")
            if h["LastModified"] > before:
                v = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
                if str(v.get("marker")) == MARK:
                    moved = True
                    break
        checks.append(("artifact moved with v3.15.0 marker", moved))

        fams = Counter()
        idx = {}
        live = 0
        for r in (v or {}).get("symbols") or []:
            idx[r.get("symbol")] = r
            if r.get("status") == "LIVE":
                live += 1
            ad = str(r.get("adapter") or "")
            if ad.startswith("family:"):
                fams[ad] += 1
        rep.kv(total_live=live, **{k: n for k, n in fams.most_common()})
        checks.append(("INTR family >=25 LIVE",
                       fams.get("family:INTR", 0) >= 25))
        checks.append(("FER family >=80 LIVE",
                       fams.get("family:FER", 0) >= 80))
        checks.append(("WB trio >=250 LIVE",
                       sum(fams.get("family:" + f, 0)
                           for f in ("GDPYY", "IRYY", "UR")) >= 250))
        for sym, want, tol in (("ECONOMICS:BRINTR", 14.25, 0.06),
                               ("ECONOMICS:PEINTR", 4.25, 0.06),
                               ("ECONOMICS:BRFER", 368899, 8000)):
            got = (idx.get(sym) or {}).get("value")
            okk = got is not None and abs(float(got) - want) <= tol
            rep.log(f"  spot {sym}: got={got} want~{want}")
            checks.append((f"spot {sym}", okk))

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — families live: {dict(fams)}; total LIVE {live}")


if __name__ == "__main__":
    main()
