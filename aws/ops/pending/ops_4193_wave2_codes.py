"""ops_4193 — MPRYY PUT (YOY_PCH_PT proven-first), cot v1.3 + vault
v3.26.2 settle, invoke chain, fire."""
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
    with report("4193_wave2_codes") as rep:
        rep.heading("ops 4193 — MPRYY config + cot codes-wide + wave-2")
        checks = []

        st, x = fetch("https://api.imf.org/external/sdmx/2.1/data/PPI/"
                      "..YOY_PCH_PT.M?lastNObservations=1", 90)
        nc = len(set(re.findall(r'COUNTRY="([A-Z]{3})"', x)))
        rep.kv(mpryy_countries=nc)
        if nc >= 40:
            d = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/family-defs.json")["Body"].read())
            fams = d.get("families") or {}
            fams["MPRYY"] = {"kind": "imf_mask", "flow": "PPI",
                             "mask": "..YOY_PCH_PT.M"}
            d["families"] = fams
            d["marker"] = "family-defs v3 ops4193"
            s3.put_object(Bucket=BUCKET, Key="data/family-defs.json",
                          Body=json.dumps(d).encode(),
                          ContentType="application/json",
                          CacheControl="max-age=300")
            rep.ok(f"  MPRYY def PUT ({nc} countries)")
        checks.append(("MPRYY proven >= 40", nc >= 40))

        checks.append(("cot v1.3 settled",
                       settle(rep, "justhodl-cot-feed",
                              "cot-feed v1.3 ops4193 codes-wide")))
        checks.append(("vault v3.26.2 settled",
                       settle(rep, "justhodl-tradingview",
                              "tradingview-vault v3.26.2 ops4193 "
                              "wave2-codes")))

        lam.invoke(FunctionName="justhodl-families-feed",
                   InvocationType="RequestResponse", Payload=b"{}")
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/families.json")["Body"].read())
        c2 = fd.get("counts") or {}
        rep.kv(MPRYY=c2.get("MPRYY"))
        checks.append(("MPRYY feed >= 40", (c2.get("MPRYY") or 0) >= 40))

        r = lam.invoke(FunctionName="justhodl-cot-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        cd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cot-feed.json")["Body"].read())
        rep.kv(cot_wanted=cd.get("wanted"), cot_resolved=cd.get("resolved"))
        checks.append(("cot wanted >= 330 (wide codes admitted)",
                       (cd.get("wanted") or 0) >= 330))
        checks.append(("cot resolved >= 260",
                       (cd.get("resolved") or 0) >= 260))

        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired — 4194 converts")

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"WAVE2+CODES — MPRYY={c2.get('MPRYY')} "
               f"cot={cd.get('resolved')}/{cd.get('wanted')}")


if __name__ == "__main__":
    main()
