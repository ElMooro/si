"""ops_4189 — v2.0 diagnosis + completion: invoke feed w/ LogResult tail, counts, vault leg."""
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


def settle(rep, name, mark, zb):
    for att in range(5):
        try:
            lam.update_function_code(FunctionName=name, ZipFile=zb,
                                     Publish=True)
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
    with report("4189_v2_diag") as rep:
        rep.heading("ops 4189 — feed v2.0 diagnosis + vault completion")
        import base64
        checks = []
        r = lam.invoke(FunctionName="justhodl-families-feed",
                       InvocationType="RequestResponse", Payload=b"{}",
                       LogType="Tail")
        rep.kv(feed_err=r.get("FunctionError"))
        tail = base64.b64decode(r.get("LogResult") or b"").decode(
            "utf-8", "ignore")
        for ln in tail.splitlines()[-10:]:
            rep.log("  LOG " + ln[:150])
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/families.json")["Body"].read())
        c = fd.get("counts") or {}
        rep.kv(marker=fd.get("marker"), CAG=c.get("CAG"),
               BCOI=c.get("BCOI"), IPRI=c.get("IPRI"),
               elapsed=fd.get("elapsed_s"))
        checks.append(("v2.0 marker live",
                       "v2.0" in str(fd.get("marker"))))
        checks.append(("new sum >= 120",
                       sum(c.get(k, 0) for k in
                           ("CAG", "BCOI", "IPRI")) >= 120))

        src = (ROOT / "lambdas" / "justhodl-tradingview" / "source" /
               "lambda_function.py").read_text()
        mark = "tradingview-vault v3.26.0 ops4187 config-driven"
        assert mark in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
        checks.append(("vault v3.26.0 settled",
                       settle(rep, "justhodl-tradingview", mark,
                              buf.getvalue())))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired — 4188 (pending) converts next")
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"V2 HEALTHY — CAG={c.get('CAG')} BCOI={c.get('BCOI')} "
               f"IPRI={c.get('IPRI')}")


if __name__ == "__main__":
    main()
