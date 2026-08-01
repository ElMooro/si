"""ops_4226 — SOPR diagnosis + provider-label v3.30.1, fire, tally."""
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
    with report("4226_onchain_truth") as rep:
        rep.heading("ops 4226 — onchain truth")
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        idx = {str(r.get("symbol")): r for r in v.get("symbols") or []}
        for sy in ("BTC_SOPR", "GLASSNODE:BTC_SOPR", "BTC_HASHRATE",
                   "USDT_SUPPLY", "ERC20_WHALES"):
            r = idx.get(sy)
            rep.log(f"  {sy}: " + (json.dumps(
                {"status": r.get("status"),
                 "src": str(r.get("source"))[:28],
                 "note": str(r.get("resolution_note"))[:38]})
                if r else "ABSENT"))
        checks = [("vault v3.30.1 settled",
                   settle(rep, "justhodl-tradingview",
                          "tradingview-vault v3.30.1 ops4226 "
                          "onchain-label"))]
        t_op = time.time()
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        deadline = time.time() + 620
        fresh = False
        while time.time() < deadline:
            h = s3.head_object(Bucket=BUCKET,
                               Key="data/tradingview.json")
            if h["LastModified"].timestamp() > t_op + 5:
                fresh = True
                break
            time.sleep(20)
        checks.append(("fresh artifact", fresh))
        v2 = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        from collections import Counter
        st = Counter()
        prov = 0
        cqn = 0
        for r in v2.get("symbols") or []:
            st[r.get("status")] += 1
            if "provider-licensed" in str(r.get("resolution_note")):
                prov += 1
            if r.get("status") == "LIVE" and "cryptoquant" in str(
                    r.get("source")):
                cqn += 1
        rep.kv(live=st.get("LIVE"), nfs=st.get("NO_FREE_SOURCE"),
               provider_labeled=prov, cq_live=cqn)
        checks.append(("provider labels >= 120", prov >= 120))
        failed = [l for l, k2 in checks if not k2]
        for l, k2 in checks:
            (rep.ok if k2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"ONCHAIN TRUTH — provider={prov} cq_live={cqn} "
               f"LIVE={st.get('LIVE')}")


if __name__ == "__main__":
    main()
