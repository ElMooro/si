#!/usr/bin/env python3
"""ops 2995 -- industry-rotation FINAL FIX: the fleet schedules via
EventBridge RULES through the shared helper (ensure_eb_rule: put_rule +
put_targets + add_permission), NOT the Scheduler API with an invoke
role -- 2993/2994 crashed on RoleArn=None ParamValidation because the
assumed scheduler role does not exist. Sequence: settle; idempotent
donor-env apply (confluence-meta keys + S3_BUCKET); wait Successful
env>=3; ensure_eb_rule industry-rotation-daily cron(35 21 * * ? *);
invoke; complete 2992 verification battery. Crash guard: any uncaught
exception still writes report + traceback."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import ensure_eb_rule

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=310, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-industry-rotation"
DONOR = "justhodl-confluence-meta"
KEYS = ["FRED_KEY", "FRED_API_KEY", "POLYGON_API_KEY", "POLYGON_KEY",
        "FMP_API_KEY", "FMP_KEY"]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2995",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8", "replace")


def wait_status(want_env=None, tries=50):
    cfg, env_n = {}, 0
    for _ in range(tries):
        cfg = LAM.get_function_configuration(FunctionName=FN)
        env_n = len((cfg.get("Environment") or {}).get("Variables")
                    or {})
        if cfg.get("LastUpdateStatus") == "Successful" and \
                (want_env is None or env_n >= want_env):
            return cfg, env_n
        time.sleep(6)
    return cfg, env_n


def main():
    fails, warns = [], []
    out = {"ops": 2995, "ts": datetime.now(timezone.utc).isoformat()}
    with report("2995_ir_final") as rep:

        rep.section("1. Settle + idempotent env + EB rule")
        time.sleep(60)
        wait_status()
        donor = (LAM.get_function_configuration(FunctionName=DONOR)
                 .get("Environment") or {}).get("Variables") or {}
        env = {k: v for k, v in donor.items() if k in KEYS}
        env["S3_BUCKET"] = BUCKET
        rep.kv(env_keys=sorted(env))
        if len(env) < 3:
            fails.append("donor env thin: %s" % sorted(env))
            _w(rep, out, fails, warns)
            return
        try:
            LAM.update_function_configuration(
                FunctionName=FN, Environment={"Variables": env},
                Timeout=240, MemorySize=512)
        except Exception as e:
            if "ResourceConflict" not in str(e):
                raise
            time.sleep(20)
            LAM.update_function_configuration(
                FunctionName=FN, Environment={"Variables": env})
        cfg, env_n = wait_status(want_env=3)
        rep.kv(env_vars=env_n, update=cfg.get("LastUpdateStatus"))
        out["env_vars"] = env_n
        if env_n < 3:
            fails.append("env still thin: %d" % env_n)
            _w(rep, out, fails, warns)
            return
        try:
            ensure_eb_rule(report=rep,
                           rule_name="industry-rotation-daily",
                           schedule="cron(35 21 * * ? *)",
                           function_name=FN)
        except Exception as e:
            fails.append("eb rule: %s" % str(e)[:140])

        rep.section("2. Invoke")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(secs=round(time.time() - t0, 1),
               err=resp.get("FunctionError"),
               body=json.dumps(body)[:200])
        if resp.get("FunctionError"):
            fails.append("invoke: %s" % json.dumps(body)[:300])
            _w(rep, out, fails, warns)
            return

        rep.section("3. Doc verify")
        d = s3_json("data/industry-rotation.json")
        mr = d.get("market_regime") or {}
        rows = d.get("ladder") or []
        out["regime"] = mr
        out["ladder_n"] = len(rows)
        out["top5"] = [(r["etf"], r["leadership_score"], r.get("tag"))
                       for r in rows[:5]]
        out["bottom3"] = [(r["etf"], r["leadership_score"])
                          for r in rows[-3:]]
        out["absorption"] = d.get("absorption_watch")
        out["breakdown"] = d.get("breakdown_watch")
        out["rank_note"] = d.get("rank_note")
        out["doc_warns"] = d.get("warns")
        rep.kv(regime=json.dumps(mr)[:200], ladder_n=len(rows),
               top5=json.dumps(out["top5"]),
               absorption=json.dumps(out["absorption"]))
        st = mr.get("state")
        coher = {"STRONG": mr.get("spy_above_sma20")
                 and mr.get("spy_above_sma50"),
                 "WEAK": not mr.get("spy_above_sma20")
                 and not mr.get("spy_above_sma50")}.get(st, True)
        if st not in ("STRONG", "WEAK", "NEUTRAL") or not coher:
            fails.append("regime incoherent: %s" % json.dumps(mr)[:150])
        if len(rows) < 28:
            fails.append("ladder thin: %d" % len(rows))
        scores = [r["leadership_score"] for r in rows]
        if scores and (max(scores) - min(scores) < 20):
            fails.append("no score dispersion: %s..%s"
                         % (min(scores), max(scores)))
        if any(not (0 <= s <= 100) for s in scores):
            fails.append("score out of bounds")
        for r in rows:
            if r["tag"] == "ABSORPTION" and st != "WEAK":
                fails.append("ABSORPTION tag outside WEAK regime")
                break
        hist = d.get("score_history") or {}
        if len(hist) < 1:
            fails.append("history not seeded")
        if len(hist) < 21 and not d.get("rank_note"):
            fails.append("no honest WARMING rank note")
        leaders = d.get("leaders") or []
        if len(leaders) != 5:
            fails.append("leaders != 5: %d" % len(leaders))
        else:
            with_sold = sum(1 for l in leaders
                            if l.get("holdings_top")
                            or l.get("resilient_names"))
            out["leaders_with_soldiers"] = with_sold
            if with_sold == 0 and not d.get("warns"):
                fails.append("no soldiers anywhere and no warns")
        bs = d.get("by_sector_name") or {}
        out["sector_map_n"] = len(bs)
        if len(bs) < 10:
            fails.append("by_sector_name thin: %d" % len(bs))

        rep.section("4. Pages live")
        page_ok = False
        for _ in range(9):
            try:
                stc, html = get("https://justhodl.ai/"
                                "industry-rotation.html?v=%d"
                                % int(time.time()))
                page_ok = (stc == 200 and "Leadership Ladder" in html
                           and "Absorption" in html
                           and "data/industry-rotation.json" in html)
                if page_ok:
                    break
            except Exception:
                pass
            time.sleep(10)
        wires_ok = 0
        for pg in ("sectors.html", "signal-board.html"):
            try:
                stc, html = get("https://justhodl.ai/%s?v=%d"
                                % (pg, int(time.time())))
                wires_ok += int("data/industry-rotation.json" in html)
            except Exception:
                pass
        rep.kv(page_ok=page_ok, wires_ok=wires_ok)
        out["page_ok"], out["wires_ok"] = page_ok, wires_ok
        if not page_ok:
            fails.append("industry-rotation.html not live/complete")
        if wires_ok < 2:
            warns.append("wire pages at %d/2 (CDN TTL)" % wires_ok)

        if not fails:
            rep.ok("INDUSTRY ROTATION LIVE: regime %s | ladder %d | "
                   "top %s | absorption %s"
                   % (st, len(rows), json.dumps(out["top5"][:3]),
                      json.dumps(out["absorption"])))
        _w(rep, out, fails, warns)


def _w(rep, out, fails, warns):
    out["fails"], out["warns"] = fails, warns
    out["verdict"] = "PASS" if not fails else "FAIL"
    (AWS_DIR / "ops" / "reports" / "2995.json").write_text(
        json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


try:
    main()
except SystemExit:
    raise
except Exception as e:
    import traceback
    (AWS_DIR / "ops" / "reports" / "2995.json").write_text(json.dumps(
        {"ops": 2995, "verdict": "FAIL",
         "fails": ["CRASH: %s" % str(e)[:200]],
         "trace": traceback.format_exc()[-1500:],
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    sys.exit(1)
sys.exit(0)
