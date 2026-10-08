#!/usr/bin/env python3
"""ops 3052 -- SMART-MONEY CONFLUENCE upgrade (Khalid: improve the
accumulation radar + whales pages). Radar v1.3.0 joins 4 independent
lenses per name (dark-pool prints, 13F whale $, Wyckoff dated phase,
insider clusters) -> confirm chips + SMART-MONEY CONFIRMED boards.
Whales v1.2 adds sector rollup, whale-breadth boards, per-whale net
stance, DP/Wyckoff/radar cross-chips + two SVG visuals (diverging flow
bars, sector net bars). Verify: wait both fns fresh (this push touches
both), invoke both, poll both feeds, assert new fields + coverage."""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FNS = {"justhodl-accumulation-radar": "data/accumulation-radar.json",
       "justhodl-whales": "data/whales.json"}


def s3j(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"]
                      .read())


def wait_fresh(fn, rep):
    t0 = datetime.now(timezone.utc)
    diff = subprocess.run(["git", "diff", "--name-only", "HEAD^",
                           "HEAD"], capture_output=True, text=True,
                          timeout=20, cwd=str(AWS_DIR.parent)).stdout
    need = ("aws/lambdas/%s/" % fn) in diff
    for _ in range(30):
        c = LAM.get_function_configuration(FunctionName=fn)
        lm = datetime.fromisoformat(
            c["LastModified"].replace("+0000", "+00:00"))
        ok = (c.get("LastUpdateStatus") in (None, "Successful")
              and c.get("State") in (None, "Active"))
        if ok and ((not need) or lm >= t0 - timedelta(seconds=90)):
            rep.kv(**{fn.split("-")[-1] + "_deploy": c["LastModified"]})
            time.sleep(6)
            return True
        time.sleep(20)
    return False


def main():
    fails, warns = [], []
    with report("3052_confluence_upgrade") as rep:
        rep.section("1. Wait both deploys, invoke both")
        prev = {}
        for fn, key in FNS.items():
            if not wait_fresh(fn, rep):
                fails.append("%s never fresh" % fn)
                _fin(rep, fails, warns)
                sys.exit(1)
            try:
                prev[key] = s3j(key).get("generated_at", "")
            except Exception:
                prev[key] = ""
            LAM.invoke(FunctionName=fn, InvocationType="Event",
                       Payload=b"{}")
        docs = {}
        for _ in range(42):                     # up to ~14 min
            time.sleep(20)
            for key in FNS.values():
                if key in docs:
                    continue
                try:
                    c = s3j(key)
                    if c.get("generated_at", "") > prev[key]:
                        docs[key] = c
                except Exception:
                    pass
            if len(docs) == 2:
                break
        for key in FNS.values():
            if key not in docs:
                fails.append("no fresh %s" % key)
        if fails:
            _fin(rep, fails, warns)
            sys.exit(1)

        rep.section("2. Radar v1.3.0 asserts")
        r = docs["data/accumulation-radar.json"]
        jc = r.get("join_coverage") or {}
        cb, ct = (r.get("confirmed_bottoms") or [],
                  r.get("confirmed_tops") or [])
        rep.kv(radar_version=r.get("version"),
               join_coverage=json.dumps(jc),
               confirmed_bottoms=len(cb), confirmed_tops=len(ct),
               sample_confirm=json.dumps(
                   [{"t": x["ticker"], "n": x["confirm_n"],
                     "c": x["confirms"]} for x in (cb + ct)[:4]]))
        if r.get("version") != "1.3.0":
            fails.append("radar version=%s" % r.get("version"))
        if (jc.get("dark_pool") or 0) < 30:
            fails.append("dark_pool join=%s" % jc.get("dark_pool"))
        if (jc.get("whales") or 0) < 1000:
            fails.append("whales join=%s" % jc.get("whales"))
        if (jc.get("wyckoff") or 0) < 100:
            fails.append("wyckoff join=%s" % jc.get("wyckoff"))
        if len(cb) + len(ct) < 3:
            fails.append("confirmed boards too thin: %d+%d"
                         % (len(cb), len(ct)))
        if not cb:
            warns.append("no confirmed bottoms today")
        if not ct:
            warns.append("no confirmed tops today")
        for x in cb + ct:
            if x.get("confirm_n", 0) < 2:
                fails.append("%s on confirmed board with confirm_n=%s"
                             % (x["ticker"], x.get("confirm_n")))
                break

        rep.section("3. Whales v1.2 asserts")
        w = docs["data/whales.json"]
        sf = w.get("sector_flows") or []
        bb = w.get("breadth_buying") or []
        cc = w.get("cross_coverage") or {}
        stance = sum(1 for c in (w.get("whales") or [])
                     if c.get("net_flow_usd") is not None)
        rep.kv(whales_schema=w.get("schema"), sector_flows=len(sf),
               top_sectors=json.dumps(
                   [{"s": a["sector"], "net": a["net_usd"]}
                    for a in sf[:4]]),
               breadth_buying=len(bb),
               breadth_top=json.dumps(bb[:3]),
               whale_stance_n=stance,
               cross_coverage=json.dumps(cc))
        if w.get("schema") != "1.2":
            fails.append("whales schema=%s" % w.get("schema"))
        if len(sf) < 6:
            fails.append("sector_flows=%d (<6)" % len(sf))
        if len(bb) < 5:
            fails.append("breadth_buying=%d (<5)" % len(bb))
        if stance < 10:
            fails.append("whale stance on %d cards (<10)" % stance)
        if (cc.get("dark_pool") or 0) < 30:
            fails.append("whales dark_pool cross=%s"
                         % cc.get("dark_pool"))

        rep.section("4. Pages (warn-level, CDN)")
        import urllib.request
        for url, marker in (
                ("https://justhodl.ai/accumulation.html",
                 "SMART-MONEY CONFIRMED"),
                ("https://justhodl.ai/whales.html", "renderFlowViz")):
            try:
                req = urllib.request.Request(
                    url + "?cb=%d" % time.time(),
                    headers={"User-Agent": "Mozilla/5.0 ops-3052"})
                pg = urllib.request.urlopen(req, timeout=25).read(
                ).decode("utf-8", "replace")
                if marker not in pg:
                    warns.append("%s not propagated" % url.split("/")[-1])
            except Exception as e:
                warns.append("page %s" % str(e)[:70])

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- confluence live on both pages")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3052.json").write_text(json.dumps(
        {"ops": 3052, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
