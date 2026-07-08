#!/usr/bin/env python3
"""ops 3004 -- (1) flows per-ticker join verify (etf-flows/{T}.json,
the summary doc carries no per-ticker rows); (2) ORPHAN-STALE/DEAD
TRIAGE: recompute engine feed freshness from first principles
(registry outs x S3 head ages -- no trust in stale classifications),
poke silent engines with Event invokes, auto-schedule ONLY those that
prove they still write (staggered daily crons via ensure_eb_rule),
enable disabled rules, capture evidence on the rest. Safe autonomy:
no schedule is added to an engine that didn't just demonstrate a
successful write.
"""
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report
from _lambda_deploy_helpers import ensure_eb_rule

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=460, connect_timeout=10,
                                 retries={"max_attempts": 0}))
EVT = boto3.client("events", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
IR = "justhodl-industry-rotation"
NOW = datetime.now(timezone.utc)
ONDEMAND_RE = re.compile(
    r"chat|telegram|admin|proxy|ingest|webhook|ask-desk|page-ai|"
    r"tv-notes|equity-research$|ticker-deep|email|sentinel-deliver")


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def head_age_h(key, cache={}):
    if key in cache:
        return cache[key]
    try:
        h = S3.head_object(Bucket=BUCKET, Key=key)
        age = (NOW - h["LastModified"]).total_seconds() / 3600.0
    except Exception:
        age = None
    cache[key] = age
    return age


def main():
    fails, warns = [], []
    out = {"ops": 3004, "ts": NOW.isoformat()}
    with report("3004_flows_orphan_triage") as rep:

        rep.section("1. IR deploy gate + invoke (flows join)")
        time.sleep(75)
        ok = False
        for _ in range(50):
            cfg = LAM.get_function_configuration(FunctionName=IR)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            if cfg.get("LastUpdateStatus") == "Successful" and \
                    (NOW - lm).total_seconds() < 1800:
                ok = True
                break
            time.sleep(8)
        if not ok:
            fails.append("IR no fresh deploy")
        else:
            t0 = time.time()
            resp = LAM.invoke(FunctionName=IR, Payload=b"{}")
            body = json.loads(resp["Payload"].read() or b"{}")
            rep.kv(ir_secs=round(time.time() - t0, 1),
                   err=resp.get("FunctionError"))
            if resp.get("FunctionError"):
                fails.append("IR invoke: %s" % json.dumps(body)[:250])
            else:
                d = s3_json("data/industry-rotation.json")
                rows = d.get("ladder") or []
                fj = sum(1 for r in rows if r.get("fund_flows"))
                out["flows_joined"] = fj
                out["flows_sample"] = [
                    {"etf": r["etf"], **r["fund_flows"]}
                    for r in rows if r.get("fund_flows")][:5]
                sc = sum(1 for r in rows
                         if r.get("scorecard_100") is not None)
                rep.kv(flows_joined=fj, scorecard_rows=sc,
                       sample=json.dumps(out["flows_sample"])[:250])
                if fj < 10:
                    fails.append("flows joined only %d/40" % fj)
                if sc < 36:
                    fails.append("scorecard regressed: %d" % sc)

        rep.section("2. Registry load + freshness recompute")
        reg = s3_json("data/engine-registry.json")
        engs = reg.get("engines") if isinstance(reg, dict) else reg
        emap = {}
        if isinstance(engs, dict):
            it = engs.items()
        else:
            it = [(e.get("name") or e.get("engine") or e.get("fn"), e)
                  for e in (engs or [])]
        for name, e in it:
            if not name or not isinstance(e, dict):
                continue
            outs = (e.get("outs") or e.get("outputs")
                    or e.get("out_keys") or e.get("writes") or [])
            douts = [o for o in outs if isinstance(o, str)
                     and o.startswith("data/") and o.endswith(".json")]
            if douts:
                emap[name] = douts
        rep.kv(registry_engines=len(emap))
        if len(emap) < 200:
            fails.append("registry parse thin: %d" % len(emap))
            _w(rep, out, fails, warns)
            return

        stale, dead, skipped = [], [], []
        for name, douts in emap.items():
            if ONDEMAND_RE.search(name):
                skipped.append(name)
                continue
            ages = [head_age_h(k) for k in douts]
            known = [a for a in ages if a is not None]
            if not known:
                dead.append((name, None, douts[:2]))
                continue
            mn = min(known)
            if mn > 720:
                dead.append((name, round(mn), douts[:2]))
            elif mn > 48:
                stale.append((name, round(mn, 1), douts[:2]))
        out["skipped_on_demand"] = sorted(skipped)
        out["stale_found"] = [(n, a) for n, a, _ in
                              sorted(stale, key=lambda x: -x[1])]
        out["dead_found"] = [(n, a) for n, a, _ in dead]
        rep.kv(stale=len(stale), dead=len(dead),
               skipped=len(skipped),
               stale_list=json.dumps(out["stale_found"])[:400],
               dead_list=json.dumps(out["dead_found"])[:300])

        rep.section("3. Poke + evidence + safe fixes")
        targets = (stale + dead)[:25]
        evid = {}
        for name, age, douts in targets:
            row = {"prev_age_h": age, "outs": douts}
            try:
                fcfg = LAM.get_function_configuration(
                    FunctionName=name)
                row["exists"] = True
                arn = fcfg["FunctionArn"]
                try:
                    rules = EVT.list_rule_names_by_target(
                        TargetArn=arn).get("RuleNames") or []
                except Exception:
                    rules = []
                row["rules"] = rules
                states = {}
                for rn in rules[:3]:
                    try:
                        states[rn] = EVT.describe_rule(
                            Name=rn).get("State")
                    except Exception:
                        pass
                row["rule_states"] = states
                for rn, st_ in states.items():
                    if st_ == "DISABLED":
                        try:
                            EVT.enable_rule(Name=rn)
                            row["enabled_rule"] = rn
                        except Exception as e:
                            row["enable_err"] = str(e)[:80]
                LAM.invoke(FunctionName=name,
                           InvocationType="Event", Payload=b"{}")
                row["poked"] = True
            except Exception as e:
                row["exists"] = False
                row["err"] = str(e)[:120]
            evid[name] = row
        time.sleep(120)
        fixed_sched = []
        for i, (name, age, douts) in enumerate(targets):
            row = evid[name]
            if not row.get("poked"):
                row["verdict"] = "REGISTRY_GHOST_OR_ERR"
                continue
            new_age = head_age_h(douts[0], cache={})
            row["age_after_poke_h"] = (round(new_age, 2)
                                       if new_age is not None else None)
            refreshed = new_age is not None and new_age < 0.1
            if refreshed and not row.get("rules"):
                try:
                    rn = "%s-daily-resurrected" % name.replace(
                        "justhodl-", "")[:40]
                    ensure_eb_rule(report=rep, rule_name=rn,
                                   schedule="cron(%d 14 * * ? *)"
                                   % (5 + (i * 3) % 50),
                                   function_name=name)
                    row["verdict"] = "RESURRECTED_SCHEDULED"
                    fixed_sched.append(name)
                except Exception as e:
                    row["verdict"] = "REFRESHED_RULE_ADD_FAILED"
                    row["rule_err"] = str(e)[:100]
            elif refreshed:
                row["verdict"] = ("REFRESHED_HAS_RULE"
                                  + ("_ENABLED" if row.get(
                                      "enabled_rule") else ""))
            else:
                row["verdict"] = "NO_WRITE_AFTER_POKE"
        out["evidence"] = evid
        out["scheduled"] = fixed_sched
        counts = {}
        for r_ in evid.values():
            v = r_.get("verdict", "?")
            counts[v] = counts.get(v, 0) + 1
        out["verdict_counts"] = counts
        rep.kv(verdicts=json.dumps(counts),
               scheduled=json.dumps(fixed_sched))

        if not fails:
            rep.ok("TRIAGE: %d stale + %d dead examined | verdicts %s "
                   "| new schedules %s | flows %s/40"
                   % (len(stale), len(dead), json.dumps(counts),
                      json.dumps(fixed_sched),
                      out.get("flows_joined")))
        _w(rep, out, fails, warns)


def _w(rep, out, fails, warns):
    out["fails"], out["warns"] = fails, warns
    out["verdict"] = "PASS" if not fails else "FAIL"
    (AWS_DIR / "ops" / "reports" / "3004.json").write_text(
        json.dumps(out, indent=1, default=str))
    rep.log("FAILS=%d" % len(fails))
    if fails:
        sys.exit(1)


try:
    main()
except SystemExit:
    raise
except Exception as e:
    import traceback
    (AWS_DIR / "ops" / "reports" / "3004.json").write_text(json.dumps(
        {"ops": 3004, "verdict": "FAIL",
         "fails": ["CRASH: %s" % str(e)[:200]],
         "trace": traceback.format_exc()[-1500:],
         "ts": NOW.isoformat()}, indent=1))
    sys.exit(1)
sys.exit(0)
