"""ops_5558 — harvest paid ETF Global (flows+constituents+profiles) into data/etf-desk.json.

Invokes justhodl-etf-global-desk if already deployed; otherwise runs the
lambda source in-process so /etf.html has real creation/redemption $ today.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-etf-global-desk"
B = "justhodl-dashboard-live"


def _load_handler():
    root = Path(__file__).resolve().parents[3]
    src = root / "aws/lambdas/justhodl-etf-global-desk/source/lambda_function.py"
    spec = importlib.util.spec_from_file_location("etf_global_desk", src)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    with report("ops_5558_etf_global_desk") as R:
        R.heading("ops 5558 — ETF Global desk harvest (flows + constituents + profiles)")
        ssm = boto3.client("ssm", region_name=REGION)
        lam = boto3.client("lambda", region_name=REGION)
        s3 = boto3.client("s3", region_name=REGION)
        key = None
        for name in ("/justhodl/polygon/api-key", "/justhodl/massive/api-key",
                     "/justhodl/massive-api-key"):
            try:
                key = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
                R.ok("key from %s" % name)
                break
            except Exception as e:
                R.ok("no %s (%s)" % (name, str(e)[:80]))
        if not key:
            R.fail("no Massive/Polygon key in SSM")
            sys.exit(1)
        os.environ["POLYGON_KEY"] = key
        os.environ["S3_BUCKET"] = B

        invoked = False
        try:
            r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                           Payload=b"{}")
            raw = r["Payload"].read()
            body = json.loads(raw.decode("utf-8", "replace") or "{}")
            R.ok("lambda invoke statusCode=%s body=%s" % (
                r.get("StatusCode"), json.dumps(body)[:240]))
            invoked = True
        except Exception as e:
            R.ok("lambda not ready (%s) — running source in-process" % str(e)[:120])

        if not invoked:
            mod = _load_handler()
            t0 = time.time()
            out = mod.lambda_handler({})
            R.ok("in-process harvest %ss → %s" % (round(time.time() - t0, 1), out))

        desk = json.loads(s3.get_object(Bucket=B, Key="data/etf-desk.json")["Body"].read())
        n_ok = desk.get("n_ok") or {}
        R.ok("etf-desk status=%s n=%s n_ok=%s" % (desk.get("status"), desk.get("n"), n_ok))
        spy = (desk.get("by_etf") or {}).get("SPY") or {}
        R.ok("SPY flow_1d=%s aum=%s er=%s holdings=%s hhi=%s" % (
            spy.get("flow_1d"), spy.get("aum"), spy.get("er"),
            spy.get("holdings_n"), spy.get("hhi")))
        if desk.get("status") != "LIVE" or not n_ok.get("flows"):
            R.fail("desk harvest empty — check ETF Global entitlement on the key")
            sys.exit(1)
        R.ok("GREEN — ETF desk now serving Massive ETF Global, not $vol z")


if __name__ == "__main__":
    main()
