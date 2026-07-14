"""ops 3290 — FIX-FORWARD for 3286+3288. Sentinel: Yahoo ^TNX close is
already percent — /10 produced level 0.461; now a unit-ambiguity
resolver picks the candidate nearest FRED's last DGS10 print (self-
correcting either convention). Comeback: FinViz export 52-Week Low/High
are PERCENT distances, not prices — price math zeroed the funnel; now
a 400-row probe auto-detects semantics and percent-mode uses the
columns directly, with a parsed→tradeable→comeback→confirm funnel
logged into warns. Redeploys both, re-runs, full truth bands."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


with report("3290_fixforward") as rep:
    fails = []
    envm = (LAM.get_function_configuration(
        FunctionName="justhodl-confluence-meta")
        .get("Environment") or {}).get("Variables") or {}

    for fn, t, m in (("justhodl-us10y-sentinel", 300, 512),
                     ("justhodl-comeback-screener", 840, 1024)):
        env = (LAM.get_function_configuration(FunctionName=fn)
               .get("Environment") or {}).get("Variables") or envm
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=AWS_DIR / "lambdas" / fn / "source",
                      env_vars=env, eb_rule_name=None, eb_schedule=None,
                      timeout=t, memory=m,
                      description="fix-forward ops 3290", smoke=False)

    rep.section("2. Sentinel re-run + truth bands")
    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-us10y-sentinel",
               InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(40):
        time.sleep(10)
        d = s3_json("data/us10y-sentinel.json")
        if d and d.get("as_of", "") >= mark:
            break
    if not d or d.get("as_of", "") < mark:
        fails.append("sentinel never freshened")
    else:
        lvl = d.get("level")
        rep.kv(level=lvl, src=d.get("level_source"),
               tier=d.get("tier"),
               dist_bps=d.get("distance_to_5pct_bps"),
               d60=(d.get("velocity") or {}).get("d60_bps"),
               real10=d.get("real_10y"),
               pct_rank=d.get("pct_rank_since_1990"))
        if not (lvl and 3.0 <= lvl <= 6.5):
            fails.append("sentinel level still off: %s" % lvl)
        eps = d.get("episode_study") or {}
        n_ok = sum(1 for v in eps.values() if (v or {}).get("n", 0) >= 1)
        rep.kv(episode_buckets=n_ok)
        if n_ok < 2:
            fails.append("episode study thin")
        if len(d.get("history_260d") or []) < 200:
            fails.append("history_260d short")

    rep.section("3. Comeback re-run + truth bands")
    mark2 = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-comeback-screener",
               InvocationType="Event", Payload=b"{}")
    c = None
    for i in range(60):
        time.sleep(14)
        c = s3_json("data/comeback-screener.json")
        if c and c.get("as_of", "") >= mark2:
            break
    if not c or c.get("as_of", "") < mark2:
        fails.append("comeback never freshened")
    else:
        rep.kv(universe=c.get("universe_n"),
               candidates=c.get("candidates_n"),
               warns=c.get("warns"))
        if (c.get("universe_n") or 0) < 3000:
            fails.append("universe thin")
        if (c.get("candidates_n") or 0) < 5:
            fails.append("candidates still thin: %s"
                         % c.get("candidates_n"))
        b = c.get("boards") or {}
        rep.kv(boards={k: len(v or []) for k, v in b.items()})
        for k in ("confirmed", "early_turn", "moonshots",
                  "dilution_traps"):
            if k not in b:
                fails.append("board missing: %s" % k)
        pool = (b.get("confirmed") or []) + (b.get("early_turn") or [])
        if pool:
            t0 = pool[0]
            rep.kv(sample=dict(t=t0.get("ticker"),
                               off_low=t0.get("off_low_pct"),
                               below_high=t0.get("below_high_pct"),
                               sh1y=t0.get("sh_1y_cagr_pct"),
                               score=t0.get("comeback_score")))
            if not (t0.get("off_low_pct", 0) >= 75
                    and t0.get("below_high_pct", 0) <= -50):
                fails.append("sample row violates comeback definition")
        else:
            fails.append("no rows on recovery boards")

    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3290 PASS — sentinel reads the true 10Y; comeback "
            "funnel flowing.")
sys.exit(0)
