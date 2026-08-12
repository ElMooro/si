"""ops 4626 — blackswan basis audit fixes (from Khalid's page paste).

Four defects owned: (1) per-observation changes mislabeled DoD ->
native cadence labels; (2) pct-change on sign-crossing/near-zero
series (RRP/NFCI/CFNAI/SLOOS class) -> difference basis with diff-z;
(3) pct-basis rows (no sigma) capped at AMBER — only z-basis rows can
mint RED (EPU-class daily indexes swing 40% normally); (4) composite
formula evaluator for FRED-mappable legs (SOFR-FF, OBFR-SOFR, CP-FF,
30s10s, IG-vs-10Y...) with full z+range basis. Budget 90.
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-4626"})
    with urllib.request.urlopen(req, timeout=timeout) as h:
        return h.read()


def main():
    misses = 0
    with report("4626_basis_audit") as r:
        r.heading("ops 4626 — blackswan basis fixes")

        r.section("deploy-settle")
        settled = False
        for att in range(16):
            try:
                gf = lam.get_function(FunctionName=BFN)
                zb = http_get(gf["Code"]["Location"], 60)
                src = zipfile.ZipFile(io.BytesIO(zb)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "justhodl-blackswan-watch v1.2.0" in src:
                    settled = True
                    break
            except Exception as e:
                r.log("settle %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(30)
        misses += contract(r, "deploy", settled, "v1.2.0 live")
        if not settled:
            sys.exit(1)

        r.section("run + basis truth")
        lam.invoke(FunctionName=BFN, InvocationType="RequestResponse")
        pl = json.loads(s3.get_object(
            Bucket=B, Key="data/blackswan-watch.json")["Body"].read())
        rows = {x["symbol"]: x for x in pl.get("rows") or []}
        strip = pl.get("strip") or {}
        r.kv(resolved=pl.get("n_resolved"),
             with_history=pl.get("n_with_history"),
             alarm=strip.get("alarm"),
             red=strip.get("n_red"), amber=strip.get("n_amber"),
             extremes=strip.get("n_range_extreme"),
             top=json.dumps(strip.get("top_movers") or [])[:180])
        for sym in ("FRED:POILBREUSDM", "FRED:NFCILEVERAGE",
                    "FRED:RECPROUSM156N", "FRED:SOFR-FRED:FEDFUNDS",
                    "TVC:US30Y-TVC:US10Y",
                    "FRED:DCPF3M-FRED:FEDFUNDS",
                    "FRED:BAMLC4A0C710YEY-TVC:US10Y"):
            x = rows.get(sym) or {}
            r.log("%-34s %-9s %-7s z=%-5s %s"
                  % (sym[:34], x.get("move_state", "?"),
                     x.get("basis", "-"), x.get("move_z"),
                     str(x.get("chg_str") or x.get("dod_pct"))[:32]))
        br = rows.get("FRED:POILBREUSDM") or {}
        misses += contract(r, "cadence",
                           "MoM" in str(br.get("chg_str")),
                           "Brent labeled MoM: %s"
                           % br.get("chg_str"))
        nf = rows.get("FRED:NFCILEVERAGE") or {}
        misses += contract(r, "diff-basis",
                           nf.get("basis") == "diff-z",
                           "NFCI-leverage on diff basis: %s"
                           % nf.get("chg_str"))
        pct_reds = [x["symbol"] for x in pl.get("rows") or []
                    if x.get("move_state") == "RED"
                    and x.get("move_z") is None]
        misses += contract(r, "no-pct-red", not pct_reds,
                           "no sigma-less row carries RED "
                           "(violations: %s)" % (pct_reds[:4]
                                                 or "none"))
        comps = [x for x in pl.get("rows") or []
                 if any(op in x["symbol"] for op in "-/+")
                 and ":" in x["symbol"] and x.get("move_z")
                 is not None]
        misses += contract(r, "composites", len(comps) >= 6,
                           "%d formula composites on z-basis (e.g. "
                           "%s)" % (len(comps),
                                    [c["symbol"][:28]
                                     for c in comps[:3]]))
        misses += contract(r, "resolution",
                           (pl.get("n_resolved") or 0) >= 355,
                           "%s/500 resolved"
                           % pl.get("n_resolved"))
        misses += contract(r, "alarm-valid",
                           strip.get("alarm") in ("CALM", "AMBER",
                                                  "RED"),
                           "recomputed alarm %s" % strip.get("alarm"))

        r.section("canary + edge")
        time.sleep(3)
        lam.invoke(FunctionName=PFN, InvocationType="RequestResponse")
        pe = json.loads(s3.get_object(
            Bucket=B,
            Key="data/physical-economy.json")["Body"].read())
        cb = (pe.get("canaries") or {}).get("blackswan_strip") or {}
        misses += contract(r, "canary",
                           cb.get("state") == strip.get("alarm"),
                           "physical board carries %s"
                           % json.dumps(cb)[:120])
        fresh = False
        for att in range(8):
            try:
                jd = json.loads(http_get(
                    "https://justhodl.ai/data/blackswan-watch.json"
                    "?cb=%d" % time.time()))
                rr = {x["symbol"]: x for x in jd.get("rows") or []}
                if (rr.get("FRED:NFCILEVERAGE") or {}).get(
                        "basis") == "diff-z":
                    fresh = True
                    break
            except Exception as e:
                r.log("edge %d: %s" % (att + 1, str(e)[:70]))
            time.sleep(20)
        misses += contract(r, "edge", fresh,
                           "edge serves the basis-audited strip")

        r.section("verdict")
        if misses:
            r.fail("basis audit: %d red" % misses)
            sys.exit(1)
        r.ok("STRIP AUDITED — alarm %s (red=%s amber=%s) on honest "
             "bases: cadence-labeled, diff-z for sign-crossers, RED "
             "reserved for sigma, %d plumbing composites live"
             % (strip.get("alarm"), strip.get("n_red"),
                strip.get("n_amber"), len(comps)))


if __name__ == "__main__":
    main()
