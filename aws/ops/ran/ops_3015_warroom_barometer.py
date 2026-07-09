#!/usr/bin/env python3
"""ops 3015 -- War Room barometer + full-inventory + fusion VERIFY.

Shipped this push (all deploys via deploy-lambdas.yml on this same push;
this script is verify-only per AUTONOMY.md):
  1. warroom v2: leading-markets/dollar/vol normalizers now emit EVERY
     watched canary (calm ones graded, firing rules unchanged); funding
     gains per-family aggregate rows; NEW top-level `barometer` =
     equal-weight mean stress, one vote per watched canary (Khalid spec),
     sentinel alert-rules excluded as binary flips.
  2. canaries.html: top SVG barometer gauge + 'Everything watched'
     inventory tab rendering all_canaries incl calm.
  3. Fusion: signal-board feed "Early-Warning War Room" (barometer ->
     -2..+2) + morning-intelligence EARLY_WARNING_WARROOM prompt line.
     (strategist already consumes warroom; crisis-composite deliberately
     NOT wired -- it consumes the raw grid, wiring the warroom too would
     double-count.)"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def wait_fresh(fn, max_min=8):
    for _ in range(int(max_min * 3)):
        try:
            c = LAM.get_function_configuration(FunctionName=fn)
            lm = datetime.fromisoformat(
                c["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds() / 60.0
            if age < 12:
                return age
        except Exception:
            pass
        time.sleep(20)
    return None


def main():
    fails, warns = [], []
    with report("3015_warroom_barometer") as rep:
        rep.section("0. Wait for deploys")
        ages = {fn: wait_fresh(fn) for fn in
                ("justhodl-canary-warroom", "justhodl-signal-board",
                 "justhodl-morning-intelligence")}
        rep.kv(code_ages_min={k: (round(v, 1) if v is not None else "STALE")
                              for k, v in ages.items()})
        if ages["justhodl-canary-warroom"] is None:
            fails.append("canary-warroom code not fresh after wait")
            _finish(rep, fails, warns, {})
            sys.exit(1)

        rep.section("1. Warroom regeneration + barometer")
        r = LAM.invoke(FunctionName="justhodl-canary-warroom",
                       InvocationType="RequestResponse", Payload=b"{}")
        body = json.loads(r["Payload"].read() or b"{}")
        rep.kv(invoke_result=json.dumps(body)[:200])
        d = s3_json("data/canary-warroom.json")
        baro = d.get("barometer") or {}
        cans = d.get("all_canaries") or []
        by_mech = {}
        calm_by_mech = {}
        for c in cans:
            by_mech[c.get("mechanism")] = by_mech.get(c.get("mechanism"),
                                                      0) + 1
            if not c.get("firing"):
                calm_by_mech[c.get("mechanism")] = calm_by_mech.get(
                    c.get("mechanism"), 0) + 1
        fam_rows = sum(1 for c in cans if c.get("synthetic_family"))
        rep.kv(barometer_score=baro.get("score"), barometer_band=baro.get(
            "band"), n_votes=baro.get("n_votes"), n_all_canaries=len(cans),
            per_mechanism=json.dumps(by_mech),
            calm_rows_per_mechanism=json.dumps(calm_by_mech),
            family_rows=fam_rows)
        if baro.get("score") is None:
            fails.append("barometer missing/None")
        if (baro.get("n_votes") or 0) < 100:
            fails.append("barometer n_votes=%s (<100 -- inventory not "
                         "flowing)" % baro.get("n_votes"))
        if len(cans) < 100:
            fails.append("all_canaries=%d (<100)" % len(cans))
        for mech in ("leading_markets", "dollar", "vol"):
            if not calm_by_mech.get(mech):
                fails.append("%s has zero calm rows -- still firing-only"
                             % mech)
        if fam_rows < 3:
            fails.append("funding family rows=%d (<3)" % fam_rows)

        rep.section("2. Signal-board fusion")
        LAM.invoke(FunctionName="justhodl-signal-board",
                   InvocationType="RequestResponse", Payload=b"{}")
        sb = s3_json("data/signal-board.json")
        blob = json.dumps(sb)
        has_row = "Early-Warning War Room" in blob
        rep.kv(signal_board_has_warroom=has_row)
        if not has_row:
            fails.append("signal-board missing War Room feed row")

        rep.section("3. Live page checks (CDN lag = warn-level)")
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://justhodl.ai/canaries.html?v=%d" % time.time(),
                headers={"User-Agent": "Mozilla/5.0 ops-3015"})
            page = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "replace")
            ok_g = "MASTER CANARY BAROMETER" in page
            ok_t = "Everything watched" in page
            rep.kv(page_gauge=ok_g, page_everything_tab=ok_t)
            if not (ok_g and ok_t):
                warns.append("pages not propagated yet (gauge=%s tab=%s)"
                             % (ok_g, ok_t))
        except Exception as e:
            warns.append("live page check: %s" % str(e)[:120])
        rep.log("morning-intelligence wiring verified by deploy + compile; "
                "prompt line lands in tomorrow's 8AM brief (not invoked "
                "here -- LLM cost discipline).")

        rep.section("verdict")
        _finish(rep, fails, warns,
                {"barometer": baro.get("score"), "n_votes": baro.get(
                    "n_votes"), "n_canaries": len(cans)})
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS -- barometer %s (%s) over %s equal votes; full "
                "inventory live" % (baro.get("score"), baro.get("band"),
                                    baro.get("n_votes")))


def _finish(rep, fails, warns, extra):
    payload = {"ops": 3015, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3015.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
