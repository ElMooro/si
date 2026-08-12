"""ops 4619 — ragged-edge fix + grid structural-norm calibration.

Board audit of the live page found two legs misreporting in opposite
directions: port tonnage pinned at 0 (trailing days with partial port
coverage undercount global tonnage -> phantom collapse -> transform
saturation) and grid buildout at 16.9 (raw executed-IA ratio scored
as support when ~15% is the structural norm for queues). Fixes:
port-cargo v1.3.0 coverage-trims the ragged edge before any window
math and reports complete_through / true_latest / trimmed-days;
signal v2.0.3 scores grid as deviation from the 15% norm.
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

FN = "justhodl-port-cargo"
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4619"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def settle(r, fn, marker):
    for att in range(16):
        try:
            gf = lam.get_function(FunctionName=fn)
            zb = http_get(gf["Code"]["Location"], 60)
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if marker in src:
                r.log("%s carries %s" % (fn, marker))
                return True
        except Exception as e:
            r.log("%s attempt %d: %s" % (fn, att + 1, str(e)[:80]))
        time.sleep(30)
    return False


def main():
    misses = 0
    with report("4619_ragged_edge") as r:
        r.heading("ops 4619 — ragged edge + grid norm")

        r.section("deploy-settle")
        ok_a = settle(r, FN, '"1.3.0"')
        ok_b = settle(r, PFN, "v2.0.3")
        misses += contract(r, "deploy", ok_a and ok_b,
                           "port-cargo v1.3.0 + signal v2.0.3")
        if not (ok_a and ok_b):
            sys.exit(1)

        r.section("port-cargo full run + ragged-edge truth")
        lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        pc = json.loads(s3.get_object(
            Bucket=B, Key="data/port-cargo.json")["Body"].read())
        seas = pc.get("seasonal_baseline") or {}
        gp = pc.get("global_pulse") or {}
        r.kv(fetch_status=pc.get("fetch_status"),
             true_latest=pc.get("true_latest_date"),
             complete_through=pc.get("complete_through"),
             trimmed=pc.get("ragged_days_trimmed"),
             coverage=json.dumps(pc.get("coverage") or {}),
             seasonal_chg=seas.get("seasonal_chg_pct"),
             total_chg=gp.get("total_chg_pct"))
        misses += contract(r, "fetch",
                           pc.get("fetch_status") == "OK",
                           "fetch OK")
        misses += contract(r, "ragged-fields",
                           "complete_through" in pc
                           and "ragged_days_trimmed" in pc,
                           "complete-window fields present "
                           "(trimmed %s day(s), through %s)"
                           % (pc.get("ragged_days_trimmed"),
                              pc.get("complete_through")))
        misses += contract(r, "fresh",
                           isinstance(pc.get("data_age_days"),
                                      (int, float))
                           and pc.get("data_age_days") <= 12,
                           "complete-window age %s d"
                           % pc.get("data_age_days"))

        r.section("signal recompute + calibrated legs")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pl = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        rows = {x["leg_id"]: x for x in pl.get("legs") or []}
        pt = rows.get("port_tonnage") or {}
        gb = rows.get("grid_buildout") or {}
        misses += contract(r, "port-leg",
                           pt.get("expansion_0_100") is not None,
                           "port leg %s · %s"
                           % (pt.get("expansion_0_100"),
                              str(pt.get("detail"))[:80]))
        if pt.get("expansion_0_100") == 0:
            r.warn("port still at floor AFTER trim — if seasonal_chg "
                   "above is genuinely <= -20%% on complete data, "
                   "that is a REAL contraction reading, not a bug")
        misses += contract(r, "grid-leg",
                           gb.get("expansion_0_100") is not None
                           and 35 <= gb["expansion_0_100"] <= 85,
                           "grid %s · %s"
                           % (gb.get("expansion_0_100"),
                              str(gb.get("detail"))[:80]))
        subs = pl.get("sub_pillars") or {}
        r.kv(composite=pl.get("composite_score"),
             label=pl.get("composite_label"),
             subs=json.dumps({k: (v or {}).get("score")
                              for k, v in subs.items()}))
        misses += contract(r, "coverage",
                           (pl.get("n_live_legs") or 0) >= 23,
                           "%s live legs" % pl.get("n_live_legs"))

        r.section("edge")
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/physical-economy.json"
                    "?cb=%d" % time.time()))
                rr = {x["leg_id"]: x for x in jd.get("legs") or []}
                gg = rr.get("grid_buildout") or {}
                if "structural norm" in str(gg.get("detail")):
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge serves the recalibrated legs")

        r.section("verdict")
        if misses:
            r.fail("ragged edge: %d red" % misses)
            sys.exit(1)
        r.ok("BOARD CORRECTED — port on complete-window basis "
             "(trimmed %s ragged day(s): seasonal %s%%, leg %s), "
             "grid norm-calibrated (%s), composite %s (%s)"
             % (pc.get("ragged_days_trimmed"),
                seas.get("seasonal_chg_pct"),
                pt.get("expansion_0_100"), gb.get("expansion_0_100"),
                pl.get("composite_score"), pl.get("composite_label")))


if __name__ == "__main__":
    main()
