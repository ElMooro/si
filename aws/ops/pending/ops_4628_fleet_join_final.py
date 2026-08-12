"""ops 4628 — fleet-join v1.3.1: columnar stores: the missing data from other
engines (Khalid's pointer). v1.3.0 resolvers: move-index history,
vix-curve-history columns (vix/vix3m/vxn), dollar-radar dxy, the
_ma200 closes buffer for plain exchange tickers, ECONOMICS->FRED
aliases (WEI/claims/TCU/permits/GDPQQ). Pre-dumps store shapes so a
miss becomes repair evidence.
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

BFN = "justhodl-blackswan-watch"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4627"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def s3j(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:
        return None


def shape(o, depth=0):
    if depth > 2:
        return type(o).__name__
    if isinstance(o, dict):
        return {k: shape(v, depth + 1)
                for k, v in list(o.items())[:5]}
    if isinstance(o, list):
        return ["len=%d" % len(o),
                shape(o[0], depth + 1) if o else "empty"]
    return (str(o)[:32] if isinstance(o, str) else o)


def main():
    misses = 0
    with report("4628_fleet_join_final") as r:
        r.heading("ops 4628 — fleet-join v1.3.1: columnar stores resolution")

        r.section("pre-dump store shapes")
        for k in ("data/_ma200/closes.json",
                  "data/vix-curve-history.json",
                  "data/dollar-radar-history.json"):
            r.log("%s: %s" % (k, json.dumps(shape(s3j(k)))[:260]))

        r.section("deploy-settle")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=BFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "justhodl-blackswan-watch v1.3.1" in src:
                    settled = True
                    break
            except Exception as e:
                r.log("settle %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(30)
        misses += contract(r, "deploy", settled, "v1.3.1 live")
        if not settled:
            sys.exit(1)

        r.section("run + fleet-join truth")
        lam.invoke(FunctionName=BFN, InvocationType="RequestResponse")
        pl = s3j("data/blackswan-watch.json") or {}
        rows = {x["symbol"]: x for x in pl.get("rows") or []}
        strip = pl.get("strip") or {}
        r.kv(resolved=pl.get("n_resolved"),
             with_history=pl.get("n_with_history"),
             alarm=strip.get("alarm"), red=strip.get("n_red"),
             amber=strip.get("n_amber"),
             extremes=strip.get("n_range_extreme"))
        for sym in ("TVC:MOVE", "CBOE:VIX3M", "CBOE:VXN", "TVC:DXY",
                    "NASDAQ:TLT", "AMEX:HYG", "NASDAQ:SMH",
                    "ECONOMICS:USWEI"):
            x = rows.get(sym) or {}
            r.log("%-16s %-9s z=%-5s n=%-4s %s"
                  % (sym, x.get("move_state", "?"), x.get("move_z"),
                     x.get("n_obs", "-"),
                     str(x.get("chg_str") or "")[:30]))
        zsyms = [sym for sym in ("TVC:MOVE", "CBOE:VIX3M",
                                 "NASDAQ:TLT", "AMEX:HYG")
                 if (rows.get(sym) or {}).get("move_z") is not None]
        misses += contract(r, "fleet-z", len(zsyms) >= 3,
                           "z-basis via fleet joins: %s" % zsyms)
        misses += contract(r, "wei",
                           (rows.get("ECONOMICS:USWEI")
                            or {}).get("resolved") is True,
                           "ECONOMICS:USWEI via FRED alias")
        misses += contract(r, "resolution",
                           (pl.get("n_resolved") or 0) >= 430,
                           "%s/500 resolved" % pl.get("n_resolved"))
        misses += contract(r, "history-depth",
                           (pl.get("n_with_history") or 0) >= 320,
                           "%s rows on statistical basis"
                           % pl.get("n_with_history"))
        misses += contract(r, "alarm-valid",
                           strip.get("alarm") in ("CALM", "AMBER",
                                                  "RED"),
                           "alarm %s" % strip.get("alarm"))

        r.section("canary + edge")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pe = s3j("data/physical-economy.json") or {}
        cb = (pe.get("canaries") or {}).get("blackswan_strip") or {}
        misses += contract(r, "canary",
                           cb.get("state") == strip.get("alarm"),
                           "board parity: %s" % json.dumps(cb)[:110])
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/blackswan-watch.json"
                    "?cb=%d" % time.time()))
                rr = {x["symbol"]: x for x in jd.get("rows") or []}
                if (rr.get("NASDAQ:TLT") or {}).get(
                        "move_z") is not None:
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge serves fleet-joined TLT z-basis")

        r.section("verdict")
        if misses:
            r.fail("fleet-join: %d red (shapes above are the "
                   "repair evidence)" % misses)
            sys.exit(1)
        r.ok("FLEET-JOINED — %s/500 resolved, %s on statistical "
             "basis, alarm %s; MOVE/VIX-curve/DXY/ETF histories now "
             "sourced from the fleet's own engines"
             % (pl.get("n_resolved"), pl.get("n_with_history"),
                strip.get("alarm")))


if __name__ == "__main__":
    main()
