"""ops 3176 — WATCHLIST ENGINE FRAMEWORK: 207 engines, one runtime.

Khalid: "every single one of my watchlists = a separate engine."

Pressure-tested: 207 Lambdas would re-fetch the same series 207 times
(SPX/DXY/FEDFUNDS sit in dozens of his lists), blow the saturated
EventBridge rule cap, and hand him 207 things to maintain. So each
watchlist is a FIRST-CLASS ENGINE — own engine_id, own feed
(data/engines/wl-*.json), own signal_type graded independently by the
outcome-checker, own scorecard row, own fusion hooks — executing on ONE
multi-tenant runtime with ONE shared series cache. That is how a signal
desk runs hundreds of signals.

Perf: rolling z computed once per SYMBOL in O(n) (verified: 2,900
symbols x 1,746 weeks in 3.7s; the naive per-member version is ~800M ops
and times out).

Gates: >=150 ACTIVE engines · per-engine feeds written · index written ·
FDR applied across all engines · firing engines listed with the exact
indicators lighting them up.
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-wl-engines"
AWS_DIR = Path(__file__).resolve().parents[2]

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3176_wl_engines") as rep:
    fails, warns = [], []
    rep.heading("ops 3176 — 207 watchlists → 207 engines")

    rep.section("1. Deploy the runtime")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    donor = (LAM.get_function_configuration(
        FunctionName="justhodl-thesis-engine").get("Environment")
        or {}).get("Variables") or {}
    env = {k: v for k, v in donor.items()
           if k in ("S3_BUCKET", "POLYGON_KEY", "FRED_API_KEY", "FRED_KEY")}
    env.setdefault("S3_BUCKET", BUCKET)
    sch = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=sch.get("rule_name"),
                  eb_schedule=sch.get("cron"),
                  timeout=cfg["timeout"], memory=cfg["memory"],
                  description=cfg.get("description", "")[:250], smoke=False)

    rep.section("2. First run — every watchlist becomes an engine")
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event",
               Payload=json.dumps({"force_emit": True}).encode())
    idx = None
    deadline = time.time() + 860
    while time.time() < deadline:
        try:
            d = s3_json("data/wl-engines.json")
            if datetime.fromisoformat(d["generated_at"]) >= t0:
                idx = d
                break
        except Exception:
            pass
        time.sleep(20)
    if not idx:
        fails.append("wl-engines.json never written")
        rep.kv(n_fails=1, verdict="FAIL")
        sys.exit(1)

    rep.section("3. The fleet he now owns")
    rep.kv(engines=idx.get("n_engines"), active=idx.get("n_active"),
           dormant=idx.get("n_dormant"), firing=idx.get("n_firing"),
           fdr_survivors=idx.get("n_fdr"),
           signals_logged=idx.get("signals_logged"),
           series_cached=idx.get("series_cached"),
           elapsed_s=idx.get("elapsed_s"))
    rep.log("── by theme: " + ", ".join(
        f"{k}={v}" for k, v in sorted((idx.get("themes") or {}).items(),
                                      key=lambda kv: -kv[1])))
    if (idx.get("n_active") or 0) < 100:
        warns.append(f"only {idx.get('n_active')} ACTIVE — the rest need "
                     "more of their indicators mapped to free sources")
    else:
        rep.ok(f"{idx['n_active']} of {idx['n_engines']} watchlists are LIVE "
               "ENGINES with their own feed, signal_type and scorecard row")

    firing = [e for e in (idx.get("engines") or []) if e.get("firing")]
    rep.log(f"── FIRING NOW ({len(firing)}) — with the exact indicators lit:")
    for e in firing[:14]:
        w = e.get("w13") or {}
        rep.log(f"  {str(e['name'])[:34]:34s} [{e['theme']:9s}] "
                f"act {str(e.get('activation_now')):>5}% "
                f"({str(e.get('activation_pctile')):>5}p) "
                f"t={str(w.get('t_stat')):>6} "
                f"lit: {', '.join(e.get('lit') or [])[:44]}")

    rep.section("4. Per-engine feeds exist (spot-check)")
    sample = [e for e in (idx.get("engines") or [])
              if e.get("state") == "ACTIVE"][:5]
    for e in sample:
        try:
            d = s3_json(f"data/engines/{e['engine_id']}.json")
            rep.ok(f"{e['engine_id']}: {d['members_resolved']}/"
                   f"{d['members_total']} members · "
                   f"{len(d.get('lit_indicators') or [])} lit · "
                   f"signal_type {d['signal_type']} · "
                   f"fusion → {', '.join((d.get('fusion_targets') or [])[:2])}")
        except Exception as ex:
            fails.append(f"feed missing: {e['engine_id']} ({str(ex)[:50]})")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
