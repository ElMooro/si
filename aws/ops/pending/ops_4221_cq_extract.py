"""ops_4221 — CQ EXTRACTION: feed v2 (19 metrics + aliases), two crypto engines wired."""
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
    with report("4221_cq_extract") as rep:
        rep.heading("ops 4217 — wave-2 (recession, sentinel, dollar)")
        import base64
        checks = []
        src = (ROOT / "lambdas" / "justhodl-cq-feed" / "source" /
               "lambda_function.py").read_text()
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for att in range(5):
            try:
                lam.update_function_code(FunctionName="justhodl-cq-feed",
                                         ZipFile=buf.getvalue(),
                                         Publish=True)
                break
            except Exception:
                time.sleep(8)
        time.sleep(12)
        r0 = lam.invoke(FunctionName="justhodl-cq-feed",
                        InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(cqfeed=r0["Payload"].read().decode()[:60])
        cd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cq-feed.json")["Body"].read())
        rep.kv(cq_metrics=cd.get("n_metrics"),
               cq_aliases=len(cd.get("prices") or {}))
        checks.append(("cq metrics >= 17",
                       (cd.get("n_metrics") or 0) >= 17))
        TRIO = (("justhodl-crypto-exchange-flows", "cq_flows",
                 None),
                ("justhodl-crypto-cycle-risk", "cq_cycle",
                 None))
        for name, blk, _ in TRIO:
            src = (ROOT / "lambdas" / name / "source" /
                   "lambda_function.py").read_text()
            assert blk in src
            m2 = __import__("re").search(
                r'(OUT_KEY|KEY|S3_KEY)\s*=\s*"(data/[a-z0-9\-_]+\.json)"',
                src)
            outk = m2.group(2)
            buf = io.BytesIO()
            with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
                z.writestr("lambda_function.py", src)
                for sh in sorted((ROOT / "shared").glob("*.py")):
                    z.writestr(sh.name, sh.read_text())
            for att in range(5):
                try:
                    lam.update_function_code(FunctionName=name,
                                             ZipFile=buf.getvalue(),
                                             Publish=True)
                    break
                except Exception:
                    time.sleep(8)
            time.sleep(12)
            r = lam.invoke(FunctionName=name,
                           InvocationType="RequestResponse",
                           Payload=b"{}", LogType="Tail")
            tail = base64.b64decode(
                r.get("LogResult") or b"").decode("utf-8", "ignore")
            for ln in tail.splitlines():
                if blk in ln:
                    rep.log("  " + ln.strip()[:130])
            d = json.loads(s3.get_object(
                Bucket=BUCKET, Key=outk)["Body"].read())
            has = blk in d
            rep.kv(**{name.split("-", 1)[1] + "_block": has})
            checks.append((f"{name} {blk} emitted", has))
            if blk == "cq_cycle" and has:
                mv = d[blk].get("mvrv")
                checks.append(("mvrv plausible 0.5-5",
                               isinstance(mv, (int, float))
                               and 0.5 < mv < 5))

        led = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/bus-consumers.json")["Body"].read())
        led["wired"] += [
            {"engine": "justhodl-crypto-exchange-flows",
             "mode": "cq_flows (CQ primary)"},
            {"engine": "justhodl-crypto-cycle-risk",
             "mode": "cq_cycle (CQ primary)"}]
        led["marker"] = "bus-consumers v3 ops4221 +cq"
        s3.put_object(Bucket=BUCKET, Key="data/bus-consumers.json",
                      Body=json.dumps(led).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
        rep.ok(f"  ledger: wired={len(led['wired'])}")

        failed = [l for l, k2 in checks if not k2]
        for l, k2 in checks:
            (rep.ok if k2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok("CQ EXTRACTED — 19 metrics daily, aliases in bus, two engines "
               "on the paid rail")


if __name__ == "__main__":
    main()
