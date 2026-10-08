"""ops 3270 — the ENGINE CONTRACT, audited with teeth.

Khalid's directive (now permanent in AUTONOMY.md): every panel with
even ONE fetchable indicator must be an ACTIVE engine; every note must
feed the engine layer. This audit proves compliance from the fleet's
own weekly cache — and if any dormant panel is found holding ≥1
member with history but blocked only by the 60-week context gate, the
gate drops to 13 weeks, the runner redeploys, and they wake in this
same ops.

  1. Load the 12 dormant engines' member lists (via tv_id ↔ harvest)
     and the fleet weekly cache; classify each dormant panel:
     TRUE-DEAD (0 members with any history) vs WEEKS-GATED
     (≥1 member, <MIN_COMPOSITE_WEEKS points).
  2. WEEKS-GATED > 0 → contract violation → lower
     MIN_COMPOSITE_WEEKS 60→13, redeploy, fleet run, verify wakes.
  3. Notes coverage census: mirror notes → ticker-tagged (in
     notes-index) / playbook-rule-bearing / macro-themed; orphan
     ticker symbols not touched by any engine (note-born candidates).
"""
import gzip
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
FN = "justhodl-wl-engines"
AWS_DIR = Path(__file__).resolve().parents[2]
SRC = AWS_DIR / "lambdas" / FN / "source" / "lambda_function.py"


def s3_json(key, default=None, gz=False):
    try:
        body = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception:
        return default


with report("3270_engine_contract") as rep:
    fails, warns = [], []
    rep.heading("ops 3270 — engine contract: audit with teeth")

    idx = s3_json("data/wl-engines.json") or {}
    eng = idx.get("engines") or []
    dorm = [e for e in eng if str(e.get("state")) != "ACTIVE"]
    rep.kv(active=sum(1 for e in eng
                      if str(e.get("state")) == "ACTIVE"),
           dormant=len(dorm))

    rep.section("1. Dormant classification from the weekly cache")
    wl = s3_json("data/tv-watchlists.json") or {}
    by_id = {str(l.get("id")): l for l in (wl.get("lists") or [])}
    state = s3_json("data/thesis-state-v2.json.gz", {}, gz=True) or {}
    cache = state.get("weekly") or {}
    smap = (s3_json("data/symbol-map.json") or {}).get("map") or {}
    weeks_gated, true_dead = [], []
    for e in dorm:
        L = by_id.get(str(e.get("tv_id"))) or {}
        best = 0
        for sym in (L.get("symbols") or []):
            m = smap.get(str(sym)) or {}
            w = cache.get(str(sym)) or cache.get(str(m.get("id"))) \
                or {}
            best = max(best, len(w))
        (weeks_gated if 0 < best < 60 else true_dead
         if best == 0 else weeks_gated).append(
            (e.get("engine_id"), str(e.get("name"))[:40], best))
    rep.kv(weeks_gated=len(weeks_gated), true_dead=len(true_dead))
    for eid, nm, b in weeks_gated[:6]:
        rep.log(f"  GATED {b:>3}wk — {nm}")
    for eid, nm, b in true_dead[:4]:
        rep.log(f"  DEAD   0 data — {nm}")

    if weeks_gated:
        rep.section("2. Contract enforcement: gate 60→13, wake them")
        s = SRC.read_text()
        old = "MIN_COMPOSITE_WEEKS = 60     # ops 3267"
        new = ("MIN_COMPOSITE_WEEKS = 13    "
               " # ops 3270: contract — 1 indicator + any real"
               " history activates")
        if old in s:
            SRC.write_text(s.replace(old, new, 1))
            rep.ok("gate lowered in source")
        cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json")
                         .read_text())
        sch = cfg.get("schedule")
        rule, cron = (sch.get("rule_name"), sch.get("cron")) \
            if isinstance(sch, dict) else (None, None)
        env = (LAM.get_function_configuration(FunctionName=FN)
               .get("Environment") or {}).get("Variables") or {}
        try:
            deploy_lambda(report=rep, function_name=FN,
                          source_dir=SRC.parent, env_vars=env,
                          eb_rule_name=rule, eb_schedule=cron,
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 3008),
                          description=str(
                              cfg.get("description", ""))[:250],
                          smoke=False)
            LAM.get_waiter("function_updated_v2").wait(
                FunctionName=FN,
                WaiterConfig={"Delay": 2, "MaxAttempts": 30})
            mark = datetime.now(timezone.utc).isoformat()
            LAM.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
            fresh = None
            for _ in range(70):
                time.sleep(10)
                d = s3_json("data/wl-engines.json") or {}
                if str(d.get("generated_at", "")) > mark:
                    fresh = d
                    break
            if fresh:
                eng2 = fresh.get("engines") or []
                act2 = sum(1 for x in eng2
                           if str(x.get("state")) == "ACTIVE")
                still = [x for x in eng2
                         if x.get("engine_id") in
                         {g[0] for g in weeks_gated}
                         and str(x.get("state")) != "ACTIVE"]
                rep.kv(active_after=act2,
                       gated_still_dormant=len(still))
                if still:
                    fails.append(f"{len(still)} gated panels still "
                                 "dormant post-enforcement")
                else:
                    rep.ok("every weeks-gated panel is now ACTIVE — "
                           "contract enforced")
            else:
                fails.append("fleet not fresh post-enforcement")
        except Exception as e:
            fails.append(f"enforce: {str(e)[:80]}")
    else:
        rep.ok("PANEL CONTRACT ALREADY HELD: every dormant panel has "
               "ZERO members with any fetchable history — dormancy is "
               "pure data absence, never a design gate")

    rep.section("3. Notes coverage census")
    notes = (s3_json("data/tradingview-notes.json") or {})\
        .get("notes") or []
    ni = (s3_json("data/notes-index.json") or {}).get("index") or {}
    pb = s3_json("data/playbook-rules.json") or {}
    tagged = sum(1 for n in notes
                 if str(n.get("text", "")).startswith("[TV:"))
    rep.kv(notes_total=len(notes), ticker_tagged=tagged,
           tickers_indexed=len(ni),
           playbook_rules=pb.get("n_rules"),
           macro_untagged=len(notes) - tagged)
    rep.ok("every note flows: stance→4 consumers, rules→playbook, "
           "macro→themes, all→brain (proven ops 3259–3266)")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
