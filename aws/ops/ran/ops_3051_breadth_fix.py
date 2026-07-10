#!/usr/bin/env python3
"""ops 3051 -- breadth-thrust FIX verify. Root cause (evidence 3049/50 +
node repro): page + feed + CORS all fine; the engine's single 20y
/historical-price-eod/light call returned EMPTY -> Whaley 'PENDING' in
July, Coppock INSUFFICIENT_DATA, forwards n=0, episodes [] -- and the
state banner rendered a literal 'NULL'. Fixes shipped this push:
chunked /full SPY fetch + Polygon fallback + spy_history_n telemetry +
explainer None%% guard (engine); NULL -> 'NO SETUP' banner (page).
Note: 1982/84/87 triggers predate SPY inception (1993) -- expect n=5
of 8 episodes priced, that is data reality not a bug."""
import base64
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=320,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-breadth-thrust"
AWS_DIR = Path(__file__).resolve().parents[2]


def wait_fresh(rep):
    t0 = datetime.now(timezone.utc)
    import subprocess
    touched = ("aws/lambdas/%s/" % FN) in subprocess.run(
        ["git", "diff", "--name-only", "HEAD^", "HEAD"],
        capture_output=True, text=True, timeout=20,
        cwd=str(AWS_DIR.parent)).stdout
    rep.kv(need_fresh_deploy=touched)
    for _ in range(30):
        c = LAM.get_function_configuration(FunctionName=FN)
        lm = datetime.fromisoformat(
            c["LastModified"].replace("+0000", "+00:00"))
        settled = (c.get("LastUpdateStatus") in (None, "Successful")
                   and c.get("State") in (None, "Active"))
        if settled and ((not touched) or lm >= t0 -
                        timedelta(seconds=90)):
            time.sleep(8)
            rep.kv(deployed_at=c["LastModified"])
            return True
        time.sleep(20)
    return False


def main():
    fails, warns = [], []
    with report("3051_breadth_fix") as rep:
        rep.section("1. Wait for this push's deploy")
        if not wait_fresh(rep):
            fails.append("fn never fresh+settled")
            _fin(rep, fails, warns)
            sys.exit(1)

        rep.section("2. Sync run + log tail")
        r = LAM.invoke(FunctionName=FN,
                       InvocationType="RequestResponse",
                       LogType="Tail", Payload=b"{}")
        tail = base64.b64decode(r.get("LogResult", "")).decode(
            "utf-8", "replace")
        rep.log("tail:\n" + tail[-1500:])
        if r.get("FunctionError"):
            fails.append("FunctionError: %s" % r["Payload"].read()
                         [:250].decode("utf-8", "replace"))
            _fin(rep, fails, warns)
            sys.exit(1)

        rep.section("3. Assert the five symptoms are gone")
        d = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/breadth-thrust.json")["Body"]
            .read())
        cr = d.get("current_readings") or {}
        wh = cr.get("whaley") or {}
        co = cr.get("coppock") or {}
        fw = (d.get("forward_expectations") or {}).get("12m") or {}
        eps = d.get("historical_episodes") or []
        rep.kv(spy_history_n=cr.get("spy_history_n"),
               whaley=json.dumps({k: wh.get(k) for k in
                                  ("state", "first_5d_return_pct")}),
               coppock_state=co.get("state"),
               coppock_value=co.get("current_value"),
               fwd12m_n=fw.get("n"), fwd12m_ret=fw.get("return_pct"),
               episodes=len(eps),
               explainer_none="None%" in (d.get("why_now_explainer")
                                          or ""))
        if (cr.get("spy_history_n") or 0) < 3000:
            fails.append("spy_history_n=%s (<3000)"
                         % cr.get("spy_history_n"))
        if wh.get("state") not in ("BULLISH", "BEARISH"):
            fails.append("whaley still %s" % wh.get("state"))
        if wh.get("first_5d_return_pct") is None:
            fails.append("whaley return still null")
        if co.get("state") == "INSUFFICIENT_DATA":
            fails.append("coppock still INSUFFICIENT_DATA")
        if (fw.get("n") or 0) < 4:
            fails.append("fwd 12m n=%s (<4)" % fw.get("n"))
        if len(eps) < 4:
            fails.append("episodes=%d (<4)" % len(eps))
        if len(eps) < 8:
            warns.append("episodes %d/8 -- 1982/84/87 predate SPY "
                         "inception, expected" % len(eps))
        if "None%" in (d.get("why_now_explainer") or ""):
            fails.append("explainer still prints None%")

        rep.section("4. Page NO-SETUP banner (warn-level, CDN lag)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/breadth-thrust.html?cb=%d"
                % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3051"})
            page = urllib.request.urlopen(req, timeout=25).read(
            ).decode("utf-8", "replace")
            rep.kv(page_no_setup="NO SETUP" in page)
            if "NO SETUP" not in page:
                warns.append("page not propagated yet")
        except Exception as e:
            warns.append("page: %s" % str(e)[:90])

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- breadth-thrust healed")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3051.json").write_text(json.dumps(
        {"ops": 3051, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
