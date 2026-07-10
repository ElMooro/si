#!/usr/bin/env python3
"""ops 3053 -- confluence retry, SEQUENTIAL. 3052 findings: (a) whales
published only a top-400 stocks map (v1.0 cap) -> radar whale-join
capped at 400; now 1500. (b) sector map read the WRONG docs -- the
fleet's sector source is screener/data.json (+opportunities/
dislocations/capital-flow), not data/data.json; sectors were 0. Order
matters: whales must write BEFORE radar reads it, so run whales ->
poll -> radar -> poll -> assert."""
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
            time.sleep(6)
            return True
        time.sleep(20)
    return False


def run_and_wait(fn, key, rep, polls=36):
    prev = ""
    try:
        prev = s3j(key).get("generated_at", "")
    except Exception:
        pass
    LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
    for _ in range(polls):
        time.sleep(20)
        try:
            c = s3j(key)
            if c.get("generated_at", "") > prev:
                return c
        except Exception:
            pass
    return None


def main():
    fails, warns = [], []
    with report("3053_confluence_seq") as rep:
        rep.section("1. Whales v1.2 first (radar depends on it)")
        if not wait_fresh("justhodl-whales", rep):
            fails.append("whales never fresh")
            _fin(rep, fails, warns)
            sys.exit(1)
        w = run_and_wait("justhodl-whales", "data/whales.json", rep)
        if not w:
            fails.append("no fresh whales.json")
            _fin(rep, fails, warns)
            sys.exit(1)
        sf = w.get("sector_flows") or []
        bb = w.get("breadth_buying") or []
        stance = sum(1 for c in (w.get("whales") or [])
                     if c.get("net_flow_usd") is not None)
        n_map = len(w.get("stocks") or {})
        rep.kv(whales_schema=w.get("schema"), stocks_map=n_map,
               sector_flows=len(sf),
               top_sectors=json.dumps([{"s": a["sector"],
                                        "net": a["net_usd"]}
                                       for a in sf[:5]]),
               breadth_buying=len(bb), whale_stance_n=stance)
        if n_map < 1200:
            fails.append("stocks map=%d (<1200)" % n_map)
        if len(sf) < 6:
            fails.append("sector_flows=%d" % len(sf))
        if len(bb) < 5:
            fails.append("breadth=%d" % len(bb))
        if stance < 10:
            fails.append("stance=%d" % stance)

        rep.section("2. Radar v1.3.0 (reads the fresh whale map)")
        if not wait_fresh("justhodl-accumulation-radar", rep):
            fails.append("radar never fresh")
            _fin(rep, fails, warns)
            sys.exit(1)
        r = run_and_wait("justhodl-accumulation-radar",
                         "data/accumulation-radar.json", rep)
        if not r:
            fails.append("no fresh radar json")
            _fin(rep, fails, warns)
            sys.exit(1)
        jc = r.get("join_coverage") or {}
        cb, ct = (r.get("confirmed_bottoms") or [],
                  r.get("confirmed_tops") or [])
        rep.kv(radar_version=r.get("version"),
               join_coverage=json.dumps(jc),
               confirmed_bottoms=len(cb), confirmed_tops=len(ct),
               sample=json.dumps([{"t": x["ticker"],
                                   "n": x["confirm_n"],
                                   "c": x["confirms"]}
                                  for x in (cb + ct)[:5]]))
        if r.get("version") != "1.3.0":
            fails.append("radar v=%s" % r.get("version"))
        if (jc.get("whales") or 0) < 1200:
            fails.append("whale join=%s" % jc.get("whales"))
        if (jc.get("dark_pool") or 0) < 30:
            fails.append("dp join=%s" % jc.get("dark_pool"))
        if len(cb) + len(ct) < 3:
            fails.append("confirmed thin %d+%d" % (len(cb), len(ct)))
        if not cb:
            warns.append("no confirmed bottoms today (honest)")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3053.json").write_text(json.dumps(
        {"ops": 3053, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
