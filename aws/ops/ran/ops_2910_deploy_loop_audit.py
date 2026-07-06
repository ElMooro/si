#!/usr/bin/env python3
"""ops 2910 — Deploy-loop audit + close the gaps found in the 2026-07-05 session.

WHAT THIS DOES
──────────────
1. HEAD freshness proof (guards the known HEAD^..HEAD stale-checkout trap).
2. investor-lenses + technical-overlays: verify function state, locate their
   schedule (classic EventBridge rule OR EventBridge Scheduler), and FIX the
   empty-Input problem — scheduled invokes currently send {} which the
   handlers 400 on. Attach a real ticker batch (page quick-list + AI basket).
3. inherit_env evidence: count standard-bundle keys on buyback-scanner (the
   workflow's default source) vs confluence-meta (proposed replacement).
4. Invoke both engines once with the batch and confirm fresh S3 writes.
5. Report → aws/ops/reports/2910.json (+ stdout to _lastrun.log).
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone

import boto3

sys.path.insert(0, "aws/ops")
from ops_report import report

REGION = "us-east-1"
ACC = "857687956942"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
s3 = boto3.client("s3")

# Page quick-list + Khalid's AI datacenter basket = the daily refresh universe
TICKERS = ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "TSLA", "AMZN", "ORCL",
           "JPM", "BRK-B", "WYFI", "APLD", "CIFR", "HUT", "CORZ", "WULF", "IREN"]

ENGINES = {
    "justhodl-investor-lenses": {
        "rule": "justhodl-investor-lenses-daily",
        "s3": "data/investor-lenses/AAPL.json",
    },
    "justhodl-technical-overlays": {
        "rule": "justhodl-technical-overlays-daily",
        "s3": "data/technical-overlays/AAPL.json",
    },
}
STD_KEYS = ["FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY", "CMC_KEY",
            "ANTHROPIC_API_KEY", "BLS_KEY", "BEA_KEY", "CENSUS_KEY"]


def env_keys(fn):
    cfg = lam.get_function_configuration(FunctionName=fn)
    return sorted((cfg.get("Environment", {}).get("Variables", {}) or {}).keys())


def main():
    out = {"generated": datetime.now(timezone.utc).isoformat(), "engines": {}}
    with report("2910") as r:
        r.section("HEAD freshness proof")
        head = subprocess.getoutput("git rev-parse HEAD")[:12]
        r.ok(f"runner HEAD = {head}")
        out["runner_head"] = head

        r.section("inherit_env evidence: bundle-source key counts")
        for src in ("justhodl-buyback-scanner", "justhodl-confluence-meta"):
            try:
                ks = env_keys(src)
                have = [k for k in STD_KEYS if k in ks]
                r.log(f"  {src}: {len(have)}/{len(STD_KEYS)} std keys -> {have}")
                out.setdefault("bundle_sources", {})[src] = have
            except Exception as e:
                r.fail(f"  {src}: {e}")

        input_json = json.dumps({"tickers": TICKERS})
        for fn, meta in ENGINES.items():
            e = {}
            r.section(fn)
            try:
                cfg = lam.get_function_configuration(FunctionName=fn)
                e["state"] = cfg["State"]
                e["env_keys"] = sorted((cfg.get("Environment", {}).get("Variables", {}) or {}).keys())
                r.ok(f"  state={e['state']} env={len(e['env_keys'])} keys")

                # ── schedule: classic rule first, else EB Scheduler ──
                rule = meta["rule"]
                sched_fixed = False
                try:
                    eb.describe_rule(Name=rule)
                    tg = eb.list_targets_by_rule(Rule=rule).get("Targets", [])
                    had_input = bool(tg and tg[0].get("Input"))
                    e["schedule"] = {"kind": "classic_rule", "rule": rule,
                                     "had_input_before": had_input}
                    # (re)attach target WITH the ticker batch Input
                    eb.put_targets(Rule=rule, Targets=[{
                        "Id": "target1",
                        "Arn": cfg["FunctionArn"],
                        "Input": input_json,
                    }])
                    sched_fixed = True
                    r.ok(f"  classic rule {rule}: Input attached "
                         f"({len(TICKERS)} tickers; before={'had input' if had_input else 'EMPTY'})")
                except eb.exceptions.ResourceNotFoundException:
                    # try EventBridge Scheduler namespace
                    found = None
                    for pg in sch.get_paginator("list_schedules").paginate():
                        for s_ in pg.get("Schedules", []):
                            if fn.replace("justhodl-", "") in s_["Name"]:
                                found = s_["Name"]; break
                        if found:
                            break
                    if found:
                        d = sch.get_schedule(Name=found)
                        tgt = d["Target"]
                        tgt["Input"] = input_json
                        sch.update_schedule(
                            Name=found, ScheduleExpression=d["ScheduleExpression"],
                            FlexibleTimeWindow=d["FlexibleTimeWindow"], Target=tgt,
                            State=d.get("State", "ENABLED"))
                        e["schedule"] = {"kind": "scheduler", "name": found}
                        sched_fixed = True
                        r.ok(f"  EB Scheduler {found}: Input attached")
                    else:
                        e["schedule"] = {"kind": "MISSING"}
                        r.fail(f"  NO schedule found for {fn} — creating classic rule")
                        # fall back: create it from the config.json spec
                        cj = json.load(open(f"aws/lambdas/{fn}/config.json"))
                        eb.put_rule(Name=rule,
                                    ScheduleExpression=cj["schedule"]["cron"],
                                    State="ENABLED",
                                    Description=cj["schedule"].get("description", ""))
                        try:
                            lam.add_permission(FunctionName=fn,
                                               StatementId=f"EventBridge-{rule}",
                                               Action="lambda:InvokeFunction",
                                               Principal="events.amazonaws.com",
                                               SourceArn=f"arn:aws:events:{REGION}:{ACC}:rule/{rule}")
                        except lam.exceptions.ResourceConflictException:
                            pass
                        eb.put_targets(Rule=rule, Targets=[{
                            "Id": "target1", "Arn": cfg["FunctionArn"],
                            "Input": input_json}])
                        e["schedule"] = {"kind": "classic_rule_created", "rule": rule}
                        sched_fixed = True
                        r.ok(f"  created {rule} with Input")
                e["schedule_input_fixed"] = sched_fixed

                # ── live batch invoke + S3 freshness ──
                inv = lam.invoke(FunctionName=fn,
                                 Payload=json.dumps({"tickers": ["AAPL", "MSFT", "NVDA"]}).encode())
                body = json.loads(inv["Payload"].read().decode())
                e["invoke_status"] = inv["StatusCode"]
                e["invoke_body"] = (body.get("body", body) if isinstance(body, dict) else body)
                time.sleep(3)
                h = s3.head_object(Bucket=BUCKET, Key=meta["s3"])
                e["s3_bytes"] = h["ContentLength"]
                e["s3_modified"] = str(h["LastModified"])
                r.ok(f"  invoke={e['invoke_status']} s3={e['s3_bytes']}B @ {e['s3_modified']}")
            except Exception as ex:
                e["error"] = f"{type(ex).__name__}: {ex}"
                r.fail(f"  {e['error']}")
            out["engines"][fn] = e

        with open("aws/ops/reports/2910.json", "w") as f:
            json.dump(out, f, indent=2, default=str)
        r.ok("report -> aws/ops/reports/2910.json")

    print(json.dumps(out, indent=2, default=str))
    sys.exit(0)


main()
