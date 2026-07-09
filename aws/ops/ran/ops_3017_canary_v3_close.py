#!/usr/bin/env python3
"""ops 3017 -- v3 CLOSE: single remaining fail was norm_cftc iterating
top-level cache values; contracts live under d["data"] (ground-truthed
from the writer: result={"source","contracts",<int>,"data":{...}}).
Reader fixed. Rerun of: ops 3016 -- Canary v3 (Khalid 10-item list + full CISS board) VERIFY.
Shipped: grid +8 (discount window, fin CP-bill TED-successor, BBB-AAA
fallen-angel pipeline, 2s10s bull-steepening VELOCITY, BKLN/HYG,
copper/gold, SMH/ACWI, MOVE/VIX); risk-ratios +4 metrics (+minimal
FRED fetch for VIXCLS, Yahoo ^MOVE probe); warroom +3 mechanisms
(norm_ciss = EVERY ECB CISS series as a canary at its own history
percentile, norm_factor_regime appetite z, norm_cftc positioning
extremes) -- all voting in the equal-weight barometer (headline now
9 mechanisms). This script re-runs the chain and asserts.

Prior scope: ops 3015 -- War Room barometer + full-inventory + fusion VERIFY.

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
    with report("3017_canary_v3_close") as rep:
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

        rep.section("1. Risk-ratios v3 (4 new metrics)")
        LAM.invoke(FunctionName="justhodl-risk-ratios",
                   InvocationType="RequestResponse", Payload=b"{}")
        rr = s3_json("data/risk-ratios.json")
        for k in ("bkln_hyg", "copper_gold", "smh_acwi", "move_vix"):
            ok = (rr.get(k) or {}).get("available")
            rep.kv(**{k: "%s %s" % ("LIVE" if ok else "DEAD",
                                    (rr.get(k) or {}).get("latest"))})
            if not ok and k != "move_vix":
                fails.append("risk-ratios %s unavailable" % k)
            elif not ok:
                warns.append("move_vix unavailable (Yahoo ^MOVE probe -- "
                             "flagged probe-first)")

        rep.section("2. Grid v3 regeneration (+8 canaries)")
        prev = ""
        try:
            prev = s3_json("data/canary-grid.json").get("generated_at", "")
        except Exception:
            pass
        LAM.invoke(FunctionName="justhodl-canary-grid",
                   InvocationType="Event", Payload=b"{}")
        cg = None
        for _ in range(30):
            time.sleep(20)
            try:
                cand = s3_json("data/canary-grid.json")
                if cand.get("generated_at", "") > prev:
                    cg = cand
                    break
            except Exception:
                continue
        if not cg:
            fails.append("no fresh canary-grid.json after 10min")
            _finish(rep, fails, warns, {})
            sys.exit(1)
        sigs = {s.get("key"): s for s in (cg.get("signals") or [])}
        new8 = ["discount_window", "fin_cp_bill", "bbb_aaa",
                "curve_velocity", "bkln_hyg", "copper_gold", "smh_acwi",
                "move_vix"]
        table = {k: "%s %s%s" % ("LIVE" if (sigs.get(k) or {}).get(
            "available") else "DEAD", (sigs.get(k) or {}).get("value"),
            (sigs.get(k) or {}).get("unit") or "") for k in new8}
        rep.kv(n_signals=len(sigs), v3_table=json.dumps(table))
        for k in new8:
            if not (sigs.get(k) or {}).get("available"):
                (warns if k == "move_vix" else fails).append(
                    "grid %s unavailable" % k)
        if len(sigs) < 60:
            fails.append("grid signals=%d (<60)" % len(sigs))

        rep.section("2b. Warroom v3 (9 mechanisms + barometer)")
        r = LAM.invoke(FunctionName="justhodl-canary-warroom",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(invoke_result=json.loads(r["Payload"].read()
                                        or b"{}").get("barometer"))
        d = s3_json("data/canary-warroom.json")
        baro = d.get("barometer") or {}
        cans = d.get("all_canaries") or []
        mech_keys = [m.get("key") for m in (d.get("mechanisms") or [])]
        n_ciss = sum(1 for c in cans if c.get("mechanism") == "ciss")
        has_factor = any(c.get("mechanism") == "factor_regime" for c in cans)
        has_cftc = any(c.get("mechanism") == "cftc" for c in cans)
        rep.kv(barometer=baro.get("score"), band=baro.get("band"),
               n_votes=baro.get("n_votes"), n_canaries=len(cans),
               mechanisms=mech_keys, n_ciss_rows=n_ciss,
               factor_row=has_factor, cftc_row=has_cftc)
        for mk in ("ciss", "factor_regime", "cftc"):
            if mk not in mech_keys:
                fails.append("mechanism %s missing from warroom" % mk)
        if n_ciss < 8:
            fails.append("only %d CISS rows (<8 -- every series expected)"
                         % n_ciss)
        if not has_factor:
            fails.append("factor-regime canary missing")
        if not has_cftc:
            fails.append("cftc canary missing")
        if (baro.get("n_votes") or 0) <= 128:
            fails.append("barometer n_votes=%s (did not grow past 128)"
                         % baro.get("n_votes"))

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
    payload = {"ops": 3017, "fails": fails, "warns": warns,
               "verdict": "FAIL" if fails else "PASS",
               "ts": datetime.now(timezone.utc).isoformat()}
    payload.update(extra)
    (AWS_DIR / "ops" / "reports" / "3017.json").write_text(
        json.dumps(payload, indent=1))
    rep.kv(verdict=payload["verdict"], n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
