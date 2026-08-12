"""ops 4644 — dxy_predict canary on the physical board (trio
symmetry with blackswan_strip + liquidity): signal v2.1.5.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

PFN = "justhodl-physical-econ"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def http_get(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4644"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4644_dxy_canary") as r:
        r.heading("ops 4644 — dxy canary on the board")

        r.section("deploy (ops-side) + settle")
        import zipfile as zf2
        buf = io.BytesIO()
        with zf2.ZipFile(buf, "w", zf2.ZIP_DEFLATED) as z:
            z.write("aws/lambdas/justhodl-physical-econ/source/"
                    "lambda_function.py", "lambda_function.py")
        for att in range(10):
            try:
                st = lam.get_function_configuration(
                    FunctionName=PFN)
                if st.get("LastUpdateStatus") == "InProgress":
                    time.sleep(15)
                    continue
                lam.update_function_code(FunctionName=PFN,
                                         ZipFile=buf.getvalue())
                break
            except Exception as e:
                if "ResourceConflict" in str(e):
                    time.sleep(15)
                    continue
                raise
        settled = False
        for att in range(12):
            try:
                gf = lam.get_function(FunctionName=PFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zf2.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if "justhodl-physical-econ v2.1.5" in src:
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(20)
        misses += contract(r, "deploy", settled, "v2.1.5 live")
        if not settled:
            sys.exit(1)

        r.section("run + parity")
        lam.invoke(FunctionName=PFN,
                   InvocationType="RequestResponse")
        pe = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        dxp = json.loads(s3.get_object(
            Bucket=B, Key="data/dxy-predict.json")["Body"].read())
        dd = dxp.get("dxy") or {}
        cb = (pe.get("canaries") or {}).get("dxy_predict") or {}
        r.kv(canary=json.dumps(cb)[:220])
        misses += contract(r, "canary",
                           cb.get("trend_label")
                           == dd.get("trend_label")
                           and cb.get("reversal_label")
                           == dd.get("reversal_label"),
                           "board parity: %s / %s"
                           % (cb.get("trend_label"),
                              cb.get("reversal_label")))
        trio = [k for k in ("blackswan_strip", "dxy_predict")
                if k in (pe.get("canaries") or {})]
        misses += contract(r, "trio", len(trio) == 2,
                           "board carries %s" % trio)

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/"
                    "physical-economy.json?cb=%d" % time.time()))
                if ((jd.get("canaries") or {}).get("dxy_predict")
                        or {}).get("reversal_label"):
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge board carries the dxy dial")

        r.section("verdict")
        if misses:
            r.fail("dxy canary: %d red" % misses)
            sys.exit(1)
        r.ok("TRIO COMPLETE — dxy_predict on the physical board: "
             "%s · %s (state %s)"
             % (cb.get("trend_label"), cb.get("reversal_label"),
                cb.get("state")))


if __name__ == "__main__":
    main()
