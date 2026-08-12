"""ops 4642 — CRYPTOCAP forensic: deployed-branch reachability,
provider egress truth, warm-cache state, full row JSON. No hard
contracts — this run exists to make the failure name itself.
"""
import io
import json
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-liquidity-reversal"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
SYM = "CRYPTOCAP:USDT.D+CRYPTOCAP:USDC.D"


def http_get(url, timeout=25):
    req = urllib.request.Request(
        url, headers={"User-Agent": "ops-4642",
                      "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.getcode(), h.read()


def main():
    with report("4642_cc_forensic") as r:
        r.heading("ops 4642 — cryptocap forensic")

        r.section("deployed-source CC context")
        gf = lam.get_function(FunctionName=FN)
        code, zb = 200, None
        req = urllib.request.Request(gf["Code"]["Location"])
        with urllib.request.urlopen(req, timeout=60) as h:
            zb = h.read()
        src = zipfile.ZipFile(io.BytesIO(zb)).read(
            "lambda_function.py").decode("utf-8", "replace")
        r.log("marker: " + (src.splitlines()[0])[:80])
        lines = src.splitlines()
        for i, ln in enumerate(lines):
            if "CC_MAP" in ln and ("elif" in ln or "if sym" in ln
                                   or "leg in CC_MAP" in ln
                                   or "not in CC_MAP" in ln):
                for j in range(max(0, i - 2), min(len(lines),
                                                  i + 3)):
                    r.log("%5d| %s" % (j + 1, lines[j][:96]))
                r.log("     ---")

        r.section("provider egress from runner")
        for u in ("https://api.coingecko.com/api/v3/global",
                  "https://api.coinpaprika.com/v1/global",
                  "https://api.coincap.io/v2/assets"
                  "?ids=tether,usd-coin"):
            try:
                c, b = http_get(u)
                r.log("%s -> %s %s" % (u[8:40], c,
                                       b[:110].decode(
                                           "utf-8", "replace")))
            except Exception as e:
                r.log("%s -> ERR %s" % (u[8:40], str(e)[:90]))

        r.section("warm cc_* state")
        try:
            pg = s3.list_objects_v2(
                Bucket=B, Prefix="data/warm/blackswan/cc_")
            for ob in (pg.get("Contents") or [])[:8]:
                body = s3.get_object(
                    Bucket=B, Key=ob["Key"])["Body"].read()
                r.log("%s (%dB): %s"
                      % (ob["Key"].split("/")[-1], ob["Size"],
                         body[:140].decode("utf-8", "replace")))
            if not pg.get("Contents"):
                r.log("NO cc_ warm keys exist")
        except Exception as e:
            r.log("warm list: %s" % str(e)[:80])

        r.section("invoke + full row JSON")
        inv = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse")
        r.log("fn_error=%s" % inv.get("FunctionError"))
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/liquidity-reversal.json")["Body"].read())
        row = next((x for x in pl.get("rows") or []
                    if x.get("symbol") == SYM), None)
        r.log("row: " + json.dumps(row)[:400])
        r.ok("forensic complete — evidence above")


if __name__ == "__main__":
    main()
