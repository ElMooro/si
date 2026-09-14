"""ops_5563 — re-harvest ETF Global with the constituents sort fix.

v1.0 used sort=constituent_rank.asc which 400s on every ticker (n_ok
constituents = 0, $99/mo sitting idle). v1.1 uses processed_date.desc,
paginates next_url, and corrects expense-ratio units.
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
    with report("ops_5563_etf_holdings_fix") as R:
        R.heading("ops 5563 — ETF Global constituents 400 fix + ER units")
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
                r.get("StatusCode"), json.dumps(body)[:280]))
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
        R.ok("etf-desk status=%s n=%s n_ok=%s ver=%s" % (
            desk.get("status"), desk.get("n"), n_ok, desk.get("version")))
        spy = (desk.get("by_etf") or {}).get("SPY") or {}
        R.ok("SPY flow_1d=%s aum=%s er=%s holdings=%s hhi=%s top_n=%s" % (
            spy.get("flow_1d"), spy.get("aum"), spy.get("er"),
            spy.get("holdings_n"), spy.get("hhi"), len(spy.get("top") or [])))
        if spy.get("er") is not None and spy.get("er") >= 1.5:
            R.fail("SPY expense ratio still looks like bps/percent mix (%s)" % spy.get("er"))
            sys.exit(1)
        if desk.get("status") != "LIVE" or not n_ok.get("flows"):
            R.fail("desk harvest empty — check ETF Global entitlement on the key")
            sys.exit(1)
        if not n_ok.get("constituents"):
            R.fail("constituents still 0 — ETF Global holdings add-on 400/empty")
            sys.exit(1)
        R.ok("GREEN — constituents live, ER in percent, ETF Global $297 earning its keep")


if __name__ == "__main__":
    main()
