"""ops 3148 — kill-thesis pipeline, actually revived.

3147 decoded fully:
  • theses key on SYMBOL — 3146's counter, compass _index_kill, and
    master-ranker's 3145 overlay were all ticker-blind (best-setups'
    older join was already symbol-tolerant — the model to copy).
  • best-ideas "0 rows" was another field-blind counter — real key is
    `stack` (n_total).
  • the 15-second premortem "run" writing symbol-only skeleton rows =
    llm_router import failing in an OLD zip that predates shared
    bundling. Env-arming alone can't fix a missing module.

THIS OP:
  1. Redeploy premortem via patched helpers (aws/shared bundled →
     llm_router lands), env preserved (+Anthropic fallback already
     armed) → async invoke (600s) → poll ≤660s.
  2. Gate: ≥5 theses WITH kill_conditions; on failure print row-level
     errors verbatim.
  3. Redeploy compass + master-ranker with symbol-tolerant joins;
     invoke both; kv kill-hit counts + universe overlaps (0 overlap can
     be legitimate — the counts prove which).
  4. Correct diagnostics: best-ideas.stack n_total.
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
HERE = Path(__file__).resolve().parent
AWS_DIR = HERE.parents[1]

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def dep(rep, fn, smoke=True, invoke_async=False):
    live = LAM.get_function_configuration(FunctionName=fn)
    env = (live.get("Environment") or {}).get("Variables") or {}
    cp = AWS_DIR / "lambdas" / fn / "config.json"
    cfg = json.loads(cp.read_text()) if cp.exists() else {
        "timeout": live.get("Timeout", 300),
        "memory": live.get("MemorySize", 512),
        "description": live.get("Description", "")}
    sched = cfg.get("schedule") or {}
    deploy_lambda(report=rep, function_name=fn,
                  source_dir=AWS_DIR / "lambdas" / fn / "source",
                  env_vars=env,
                  eb_rule_name=(sched.get("name") or sched.get("rule_name")),
                  eb_schedule=(sched.get("expression") or sched.get("cron")),
                  timeout=cfg.get("timeout", 300),
                  memory=cfg.get("memory", 512),
                  description=(cfg.get("description") or "")[:250],
                  smoke=smoke)
    if invoke_async:
        LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        rep.log(f"  {fn}: async invoke fired")


def wait_fresh(key, t0, secs):
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            d = s3_json(key)
            ts = d.get("generated_at") or d.get("as_of")
            if ts and datetime.fromisoformat(ts) >= t0:
                return d
        except Exception:
            pass
        time.sleep(15)
    return None


with report("3148_kill_pipeline_revive") as rep:
    fails, warns = [], []
    rep.heading("ops 3148 — kill-thesis pipeline true revive")

    rep.section("0. Corrected input diagnostics")
    bi = s3_json("data/best-ideas.json")
    n_stack = bi.get("n_total") or len(bi.get("stack") or [])
    rep.kv(best_ideas_stack=n_stack,
           best_ideas_generated=bi.get("generated_at"))
    if not n_stack:
        warns.append("best-ideas stack truly empty — nobrainers fallback")
    else:
        rep.ok(f"best-ideas healthy: {n_stack} in stack "
               "(3147's 0 was a field-blind counter)")

    rep.section("1. Premortem redeploy (shared bundled) + invoke")
    t1 = datetime.now(timezone.utc)
    try:
        dep(rep, "justhodl-premortem-engine", smoke=False, invoke_async=True)
    except Exception as e:
        fails.append(f"premortem deploy: {str(e)[:160]}")
    kt = wait_fresh("data/kill-theses.json", t1, 660) \
        if not fails else None
    if kt is None:
        fails.append("kill-theses never freshened post-redeploy")
    else:
        th = [t for t in (kt.get("theses") or []) if isinstance(t, dict)]
        rich = [t for t in th if t.get("kill_conditions")]
        errs = [t for t in th if t.get("error")]
        rep.kv(theses=len(th), with_kill_conditions=len(rich),
               row_errors=len(errs))
        if errs:
            rep.log(f"error sample: {json.dumps(errs[0])[:240]}")
        if len(rich) >= 5:
            rep.ok(f"PIPELINE LIVE: {len(rich)} theses with real "
                   "kill_conditions")
            for t in rich[:3]:
                kc = (t.get("kill_conditions") or [{}])[0]
                rep.log(f"  · {t.get('symbol')}: "
                        f"{str(kc.get('risk') or kc.get('condition') or kc)[:130]}")
        else:
            fails.append(f"only {len(rich)} rich theses "
                         f"({len(errs)} row errors) — see sample above")

    rep.section("2. Consumers redeployed with symbol-tolerant joins")
    t2 = datetime.now(timezone.utc)
    for fn in ("justhodl-alpha-compass", "justhodl-master-ranker"):
        try:
            dep(rep, fn, smoke=True)
        except Exception as e:
            fails.append(f"{fn} deploy: {str(e)[:140]}")

    rep.section("3. Kill-hit accounting (0 overlap can be legitimate)")
    kt_syms = set()
    if kt:
        kt_syms = {str(t.get("symbol") or t.get("ticker") or "").upper()
                   for t in (kt.get("theses") or [])
                   if isinstance(t, dict) and not t.get("error")}
    mr = wait_fresh("data/master-ranker.json", t2, 120) or \
        s3_json("data/master-ranker.json")
    tops = (mr.get("top_tickers") or mr.get("ranked")
            or mr.get("leaderboard") or [])
    mr_syms = {str(t.get("ticker") or "").upper() for t in tops}
    n_kill_hits = sum(1 for t in tops if t.get("kill_risk"))
    rep.kv(kill_symbols=len(kt_syms),
           mr_overlap=len(kt_syms & mr_syms),
           mr_kill_hits=n_kill_hits)
    if (kt_syms & mr_syms) and n_kill_hits == 0:
        fails.append("overlap exists but master-ranker kill_risk still 0 "
                     "— join still blind")
    cp = wait_fresh("data/alpha-compass.json", t2, 120) or \
        s3_json("data/alpha-compass.json")
    n_cp = sum(1 for c in (cp.get("top_calls") or []) +
               (cp.get("watchlist") or [])
               for n in ((c.get("express") or {}).get("names") or [])
               if n.get("kill_risk"))
    exp_syms = {str(n.get("ticker") or "").upper()
                for c in (cp.get("top_calls") or []) + (cp.get("watchlist") or [])
                for n in ((c.get("express") or {}).get("names") or [])}
    rep.kv(compass_express_names=len(exp_syms),
           compass_overlap=len(kt_syms & exp_syms),
           compass_kill_hits=n_cp)
    if (kt_syms & exp_syms) and n_cp == 0:
        fails.append("compass overlap exists but kill_risk 0 — join blind")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
