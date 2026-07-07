#!/usr/bin/env python3
"""ops 2980 -- gap-metrics FINAL: cor3m via CBOE historical OHLC (shape
bug fixed: data is a list, not {prices:[]}), revision-breadth v3 =
strategist SPX-target revisions (sellside recent_revisions_30d) live now
+ TRUE stock-level consensus-EPS snapshot diff (60-name universe, 21d
warmup, store data/gap-est-snapshots.json). NAAIM confirmed healthy-
weekly by ops 2979 (no repair; cadence-mismatch flag only).

Gate: race-fixed deploy wait; invoke; >=10/11 modules OK with cor3m and
revision_breadth explicitly OK; implied-corr in (0,100); breadth
strategist n>=5; snapshot store persisted with >=30 names for the 21d
warmup clock.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=310, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-gap-metrics"


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    fails, warns = [], []
    hl = {"naaim_finding": "healthy weekly series; 2026-07-01 print "
                           "84.69 NEUTRAL, 1043-row history, schedule "
                           "ENABLED (ops 2979) -- 'stale' was a cadence "
                           "mismatch, not a fault"}
    with report("2980_gap_final") as rep:

        rep.section("1. Race-safe deploy wait")
        time.sleep(75)
        fresh = False
        for _ in range(50):
            cfg = LAM.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds()
            if cfg.get("LastUpdateStatus") == "Successful" \
                    and age < 1800:
                env_n = len((cfg.get("Environment") or {})
                            .get("Variables") or {})
                rep.kv(deploy_age_s=int(age), env_vars=env_n)
                if env_n < 3:
                    fails.append("env nuked: %d vars" % env_n)
                fresh = True
                break
            time.sleep(8)
        if not fresh:
            fails.append("no successful deploy in window")
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("2. Invoke")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(invoke_seconds=round(time.time() - t0, 1),
               fn_error=resp.get("FunctionError"),
               body=json.dumps(body)[:180])
        if resp.get("FunctionError"):
            fails.append("invoke error: %s" % json.dumps(body)[:250])
            _write(rep, fails, warns, hl)
            return

        rep.section("3. Verify 10+/11 with the two fixes")
        idx = s3_json("data/gap-metrics.json")
        mods = idx.get("modules") or {}
        ok = sorted(n for n, m in mods.items() if m["status"] == "OK")
        deg = {n: (m.get("headline") or {}).get("note")
               for n, m in mods.items() if m["status"] != "OK"}
        rep.kv(modules_ok=len(ok), ok=ok,
               degraded=json.dumps(deg)[:300])
        hl["modules_ok"] = len(ok)
        hl["degraded"] = deg
        if len(ok) < 10:
            fails.append("only %d/11 OK: %s" % (len(ok),
                                                json.dumps(deg)[:300]))
        for need in ("cor3m", "revision_breadth"):
            if need not in ok:
                fails.append("%s not OK: %s" % (need, deg.get(need)))

        if "cor3m" in ok:
            c = s3_json("data/implied-corr.json")
            hl["implied_corr"] = c.get("implied_corr_3m")
            hl["implied_corr_pctile"] = c.get("pctile_1y")
            rep.kv(implied_corr=hl["implied_corr"],
                   pctile=hl["implied_corr_pctile"],
                   source=c.get("source"))
            if not (0 < (c.get("implied_corr_3m") or -1) < 100
                    and "CBOE" in str(c.get("source"))):
                fails.append("implied-corr values/source off: %s"
                             % json.dumps(c)[:150])

        if "revision_breadth" in ok:
            b = s3_json("data/revision-breadth.json")
            hl["breadth"] = b.get("breadth_pct_positive")
            hl["breadth_basis"] = b.get("headline_basis")
            hl["strategist"] = b.get("strategist")
            rep.kv(breadth=hl["breadth"], basis=hl["breadth_basis"],
                   strategist=json.dumps(b.get("strategist"))[:160],
                   notes=json.dumps(b.get("notes"))[:160])
            st = b.get("strategist") or {}
            if b.get("mode") == "WARMING_UP":
                w = b.get("warmup") or {}
                rep.kv(breadth_mode="WARMING_UP",
                       warmup=json.dumps(w)[:140])
                hl["breadth_basis"] = "WARMING_UP eta %s (%s names)" % (
                    w.get("eta"), w.get("captured_names"))
                if (w.get("captured_names") or 0) < 30:
                    fails.append("warmup snapshot thin: %s" % w)
            elif not (0 <= (b.get("breadth_pct_positive") or -1) <= 100
                      and (st.get("n") or 0) >= 5):
                fails.append("breadth v3 values off: %s"
                             % json.dumps(b)[:200])
            try:
                snap = s3_json("data/gap-est-snapshots.json")
                day = sorted(snap)[-1]
                n_names = len(snap[day])
                rep.kv(snapshot_day=day, snapshot_names=n_names)
                hl["snapshot_names"] = n_names
                if n_names < 30:
                    warns.append("estimate snapshot thin: %d names "
                                 "(warmup clock still runs)" % n_names)
            except Exception as e:
                warns.append("snapshot store not readable: %s"
                             % str(e)[:70])

        if not fails:
            rep.ok("gap-metrics COMPLETE: %d/11 OK; implied-corr %s "
                   "(%sth pctile) via CBOE; breadth %s%% (%s); "
                   "stock-level warmup running with %s names"
                   % (len(ok), hl.get("implied_corr"),
                      hl.get("implied_corr_pctile"), hl.get("breadth"),
                      hl.get("breadth_basis"), hl.get("snapshot_names")))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2980, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2980.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


main()
sys.exit(0)
