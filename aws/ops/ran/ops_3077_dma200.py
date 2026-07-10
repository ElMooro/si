#!/usr/bin/env python3
"""ops 3077 (page-gate fix) -- 200DMA BREAKS added to the reversal boards (Khalid): fresh break ABOVE the 200DMA = dated bottom trigger (Weinstein Stage 2), fresh break BELOW = dated top trigger (Stage 4); both feed the >=1.5x volume CONFIRMED gate; pct-vs-200DMA on every card. Engine v1.4.1. Base arc
(Khalid): two clearly separated boards -- BOTTOMED->turning up vs
TOPPED->dump starting -- each requiring a prior-trend context gate +
a DATED trigger within 12-15 sessions (50DMA reclaim/loss, 3-month
breakout/breakdown, golden/death cross), with VOLUME as the CONFIRMED
gate (>=1.5x 50d avg on the trigger, O'Neil/IBD), up/down volume
tape, OBV divergences (Granville), distribution-day clusters,
capitulation detection, and an on-page methodology + volume-reading
guide with citations (Wyckoff, Weinstein, O'Neil/IBD, Granville).
Engine v1.4.0 (MAXDAYS 200->235 for the cross scan). Sequential:
invoke engine fresh -> assert schema + separation -> page live."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-accumulation-radar"
AWS_DIR = Path(__file__).resolve().parents[2]
T_START = datetime.now(timezone.utc)
UA = {"User-Agent": "Mozilla/5.0 ops-3077",
      "Cache-Control": "no-cache"}


def wait_fresh(rep):
    import subprocess
    diff = subprocess.run(
        ["git", "diff", "--name-only", "HEAD^", "HEAD"],
        capture_output=True, text=True, cwd=str(AWS_DIR.parent)
    ).stdout
    need = ("aws/lambdas/%s/" % FN) in diff
    rep.kv(need_fresh=need)
    for _ in range(30):
        try:
            cfg = L.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            settled = cfg.get("LastUpdateStatus") in ("Successful",
                                                      None)
            fresh = lm >= T_START - __import__(
                "datetime").timedelta(seconds=90)
            if settled and (fresh or not need):
                return True
        except Exception:
            pass
        time.sleep(20)
    return False


def main():
    fails, warns = [], []
    with report("3077_dma200") as rep:
        rep.section("1. Deploy gate + invoke")
        if not wait_fresh(rep):
            fails.append("engine never fresh/settled")
            _fin(rep, fails, warns)
            sys.exit(1)
        L.invoke(FunctionName=FN, InvocationType="Event",
                 Payload=b"{}")
        d = None
        for _ in range(50):
            time.sleep(20)
            try:
                o = S3.get_object(Bucket=BUCKET,
                                  Key="data/accumulation-radar.json")
                if (datetime.now(timezone.utc)
                        - o["LastModified"]).total_seconds() < 1500:
                    d = json.loads(o["Body"].read())
                    if d.get("version") == "1.4.1":
                        break
                    d = None
            except Exception:
                pass
        if not d:
            fails.append("no fresh v1.4.1 output")
            _fin(rep, fails, warns)
            sys.exit(1)

        rep.section("2. Reversals schema + separation")
        rv = d.get("reversals") or {}
        bt, tp = rv.get("bottoms") or [], rv.get("tops") or []
        overlap = set(x["ticker"] for x in bt) & set(
            x["ticker"] for x in tp)
        rep.kv(n_bottoms=len(bt), n_tops=len(tp),
               overlap=json.dumps(sorted(overlap)),
               buffer_days=d.get("buffer_days"),
               bottom_sample=json.dumps(bt[0] if bt else None)[:400],
               top_sample=json.dumps(tp[0] if tp else None)[:400])
        if overlap:
            fails.append("boards not separated: %s"
                         % sorted(overlap))
        if not rv.get("method", {}).get("volume_guide"):
            fails.append("volume guide missing")
        if len(rv.get("method", {}).get("citations") or []) < 4:
            fails.append("citations missing")
        n200 = sum(1 for x in bt if x.get("broke_200dma_up")) + \
            sum(1 for x in tp if x.get("broke_200dma_down"))
        rep.kv(n_200dma_breaks=n200,
               pct200_present=all("pct_vs_200dma" in x
                                  for x in bt + tp))
        if (bt or tp) and not all("pct_vs_200dma" in x
                                  for x in bt + tp):
            fails.append("pct_vs_200dma missing on some rows")
        for x in bt + tp:
            for k in ("score", "tier", "evidence",
                      "vol_ratio_today"):
                if x.get(k) in (None, []):
                    fails.append("%s missing %s" % (x["ticker"], k))
                    break
        if not bt and not tp:
            warns.append("zero reversals today (honest -- gates are "
                         "strict); schema verified structurally")
        conf = [x for x in bt + tp if x.get("tier") == "CONFIRMED"]
        rep.kv(n_confirmed=len(conf))
        if (bt or tp) and not conf:
            warns.append("no CONFIRMED tier today (no >=1.5x volume "
                         "trigger)")
        if (d.get("buffer_days") or 0) < 210:
            warns.append("buffer_days=%s (<210) -- cross-scan window "
                         "thin until backfill completes"
                         % d.get("buffer_days"))

        rep.section("3. Page live")
        ok = False
        for i in range(24):
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/accumulation.html?cb=%d"
                    % time.time(), headers=UA)
                pg = urllib.request.urlopen(
                    req, timeout=25).read().decode("utf-8", "replace")
                if '200DMA \u2191' in pg:  # this-push marker (3076: old-marker gate grabbed the stale page)
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(20)
        if not ok:
            fails.append("page sections not live after 8min")
        else:
            for m in ('id="rev-tops"', 'id="rev-method"',
                      "what the volume means", "GOLDEN",
                      "DEATH", "BREAKOUT", "BREAKDOWN",
                      "200DMA \u2191", "200DMA \u2193",
                      "200DMA upside break",
                      "distribution days"):
                if m not in pg:
                    fails.append("page marker missing: %s" % m)

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3077.json").write_text(json.dumps(
        {"ops": 3077, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
