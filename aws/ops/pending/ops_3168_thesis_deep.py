"""ops 3168 — Thesis Engine v2 on 36 YEARS (1990-2026).

v1's honest null (0/56 survive FDR) was a POWER problem: ~2 years of
history gave n_eff ~5 per thesis. ops 3167 proved free deep sources
(FRED 1990+, Yahoo/Stooq market chain) and mapped 2,895 symbols.

v2 rebuilds the study on a weekly grid back to 1990 (~1,900 weeks),
3y rolling z-scores, forward SPY at 4/13/26 weeks, overlap-corrected t,
BH-FDR across all theses. THIS run's numbers carry weight.

Deploy (3008MB/900s; series_source.py rides the aws/shared bundle),
invoke with force_emit, wait, then report:
  · weeks of history actually achieved
  · FDR survivors — the theses that genuinely lead
  · which are firing today
"""

import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-thesis-engine"
AWS_DIR = Path(__file__).resolve().parents[1]

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3168_thesis_deep") as rep:
    fails, warns = [], []
    rep.heading("ops 3168 — Thesis Engine v2 (1990-2026)")

    rep.section("1. Deploy v2")
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
    env = (LAM.get_function_configuration(FunctionName=FN)
           .get("Environment") or {}).get("Variables") or {}
    sched = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env,
                  eb_rule_name=sched.get("rule_name"),
                  eb_schedule=sched.get("cron"),
                  timeout=cfg.get("timeout", 900),
                  memory=cfg.get("memory", 3008),
                  description=cfg.get("description", "")[:250],
                  smoke=False)

    rep.section("2. Deep run (first pass backfills 1990→today)")
    doc, last = None, None
    for attempt in (1, 2):          # 2nd invoke continues from the cache
        t0 = datetime.now(timezone.utc)
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"force_emit": True}).encode())
        rep.log(f"invoke {attempt} fired")
        deadline = time.time() + 880
        while time.time() < deadline:
            try:
                d = s3_json("data/thesis-engine.json")
                if datetime.fromisoformat(d["generated_at"]) >= t0:
                    last = d
                    break
            except Exception:
                pass
            time.sleep(20)
        if last is None:
            fails.append(f"invoke {attempt}: doc never freshened")
            break
        rep.kv(**{f"pass{attempt}_status": last.get("status"),
                  f"pass{attempt}_weeks": last.get("n_weeks"),
                  f"pass{attempt}_series": last.get("series_cached"),
                  f"pass{attempt}_theses": last.get("n_theses")})
        if last.get("status") == "LIVE" and (last.get("n_theses") or 0) > 0:
            doc = last
            break
        rep.log(f"  status={last.get('status')} — running a second pass to "
                "finish the backfill from cache")

    if not doc:
        fails.append(f"engine never reached LIVE with theses "
                     f"(last status: {(last or {}).get('status')})")
        rep.kv(n_fails=len(fails), verdict="FAIL")
        sys.exit(1)

    rep.section("3. Results — 36 years of evidence")
    rows = doc.get("theses") or []
    surv = [r for r in rows if r.get("fdr_pass")]
    firing = [r for r in rows if r.get("firing")]
    rep.kv(history_start=doc.get("history_start"), weeks=doc.get("n_weeks"),
           series_cached=doc.get("series_cached"), theses=len(rows),
           fdr_survivors=len(surv), firing_now=len(firing),
           signals_logged=doc.get("signals_logged"),
           elapsed_s=doc.get("elapsed_s"))
    b = doc.get("spy_base_rates_pct") or {}
    rep.log(f"── SPY base rates since 1990: 4w {b.get('w4')}% · "
            f"13w {b.get('w13')}% · 26w {b.get('w26')}%")

    rep.log("── ranked by overlap-corrected t on 13w forward SPY:")
    for r in rows[:16]:
        e = (r.get("event_study") or {}).get("w13") or {}
        rep.log(f"  {str(r['name'])[:36]:36s} "
                f"hist {str(r.get('history_from')):>8} "
                f"act {str(r.get('activation_now')):>5}% "
                f"| excess {str(e.get('excess_vs_base_pct')):>6}% "
                f"hit-edge {str(e.get('hit_edge_pp')):>5}pp "
                f"t={str(e.get('t_stat')):>6} n_eff={str(e.get('n_effective')):>5}"
                f"{'  FDR' if r.get('fdr_pass') else ''}"
                f"{' FIRING' if r.get('firing') else ''}")

    if surv:
        rep.ok(f"{len(surv)} theses SURVIVE FDR on 36 years — these are "
               "real leads, not noise")
        rep.log("── THE THESES THAT ACTUALLY PREDICT:")
        for r in surv[:10]:
            e = r["event_study"]["w13"]
            tag = "risk-OFF" if e["excess_vs_base_pct"] < 0 else "risk-ON"
            rep.log(f"  · {r['name']} [{tag}] — when firing, SPY 13w "
                    f"{e['excess_vs_base_pct']:+.2f}% vs base "
                    f"(hit edge {e['hit_edge_pp']:+.1f}pp), t={e['t_stat']}, "
                    f"n_eff={e['n_effective']}, since {r['history_from']}"
                    f"{'   ← FIRING NOW' if r.get('firing') else ''}")
    else:
        warns.append("still zero FDR survivors even on 36y — the honest "
                     "read: these panels describe regimes, they do not time "
                     "SPY. Keep them as context, not as timing signals.")

    rep.section("4. Page")
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"https://justhodl.ai/theses.html?t={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 ops"}), timeout=15)
        if "Thesis Engine" in r.read().decode("utf-8", "replace"):
            rep.ok("theses.html live")
    except Exception as e:
        warns.append(f"page: {str(e)[:50]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    sys.exit(1 if fails else 0)
