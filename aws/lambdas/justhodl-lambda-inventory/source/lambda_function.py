"""justhodl-lambda-inventory — SPEC D1+D2+D3 (ops 4429).

A living map of the 785-lambda fleet. Daily 06:00 UTC it walks the AWS
Lambda/EventBridge APIs and writes:

  data/audit/lambda-inventory.json      D1  memory, timeout, runtime, schedules,
                                            env-var KEY NAMES (never values),
                                            last-modified, code size
  data/audit/lambda-config-issues.json  D2  flags: <1024MB memory, no schedule,
                                            hardcoded model IDs in env, empty
                                            env values, very short timeouts
  data/audit/lambda-health.json         D3  DEAD detection — any SCHEDULED
                                            function with zero log events in
                                            24h+ is flagged, with its last
                                            seen timestamp

D3 is what stops the class of failure Khalid already hit: the mechanical bot
tried to restart a ghost lambda and crashed with ResourceNotFoundException.
The inventory is the source of truth the restart guard (D5) checks against.
"""
import json
import os
import re
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60, retries={"max_attempts": 2}))
ev = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

MODEL_ID_RE = re.compile(r"(claude|gpt|gemini|glm|sonar)[-\w.]*", re.I)
SECRET_KEYS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL")


def _now():
    return datetime.now(timezone.utc)


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")


def lambda_handler(event, context):
    # ── D1: inventory ──
    fns, tok = [], None
    while True:
        kw = {"MaxItems": 50}
        if tok:
            kw["Marker"] = tok
        r = lam.list_functions(**kw)
        fns += r.get("Functions", [])
        tok = r.get("NextMarker")
        if not tok:
            break

    # schedules: one pass over rules, mapping target arn -> rule
    sched = {}
    try:
        rtok = None
        while True:
            kw = {"Limit": 100}
            if rtok:
                kw["NextToken"] = rtok
            rr = ev.list_rules(**kw)
            for rule in rr.get("Rules", []):
                try:
                    tg = ev.list_targets_by_rule(Rule=rule["Name"],
                                                 Limit=20).get("Targets", [])
                except Exception:
                    continue
                for t in tg:
                    arn = t.get("Arn", "")
                    if ":function:" in arn:
                        fn = arn.split(":function:")[-1].split(":")[0]
                        sched.setdefault(fn, []).append({
                            "rule": rule["Name"],
                            "schedule": rule.get("ScheduleExpression"),
                            "state": rule.get("State")})
            rtok = rr.get("NextToken")
            if not rtok:
                break
    except Exception as e:
        print("rule scan partial:", str(e)[:120])

    inventory, issues = {}, []
    for f in fns:
        name = f["FunctionName"]
        envv = (f.get("Environment", {}) or {}).get("Variables", {}) or {}
        env_keys = sorted(envv.keys())          # names only, never values
        rules = sched.get(name, [])
        rec = {
            "name": name, "runtime": f.get("Runtime"),
            "memory_mb": f.get("MemorySize"), "timeout_s": f.get("Timeout"),
            "code_size_kb": round(f.get("CodeSize", 0) / 1024, 1),
            "last_modified": f.get("LastModified"),
            "handler": f.get("Handler"), "state": f.get("State"),
            "schedules": rules, "scheduled": bool(rules),
            "env_key_count": len(env_keys),
            "env_keys": [k for k in env_keys
                         if not any(s in k.upper() for s in SECRET_KEYS)],
            "secret_env_keys": [k for k in env_keys
                                if any(s in k.upper() for s in SECRET_KEYS)],
        }
        inventory[name] = rec

        # ── D2: config completeness ──
        probs = []
        if (f.get("MemorySize") or 0) < 1024:
            probs.append(f"memory {f.get('MemorySize')}MB < 1024")
        if not rules and name.startswith("justhodl"):
            probs.append("no schedule bound")
        if (f.get("Timeout") or 0) < 30:
            probs.append(f"timeout {f.get('Timeout')}s is very short")
        for k, v in envv.items():
            if v == "":
                probs.append(f"empty env value: {k}")
            elif MODEL_ID_RE.fullmatch(str(v).strip()):
                probs.append(f"hardcoded model id in env {k}={v}")
            elif str(v).rstrip().endswith("."):
                probs.append(f"trailing-dot value in {k}={v}")
        if probs:
            issues.append({"name": name, "issues": probs})

    # ── D3: DEAD detection ──
    since = int((_now() - timedelta(hours=26)).timestamp() * 1000)
    dead, alive, unknown = [], 0, 0
    for name, rec in inventory.items():
        if not rec["scheduled"]:
            continue
        try:
            e = logs.filter_log_events(logGroupName=f"/aws/lambda/{name}",
                                       startTime=since, limit=1)
            if e.get("events"):
                alive += 1
            else:
                dead.append({"name": name, "schedules": rec["schedules"],
                             "last_modified": rec["last_modified"],
                             "verdict": "DEAD — scheduled but no logs in 26h"})
        except logs.exceptions.ResourceNotFoundException:
            dead.append({"name": name, "schedules": rec["schedules"],
                         "verdict": "DEAD — no log group (never ran)"})
        except Exception:
            unknown += 1

    ts = _now().isoformat(timespec="seconds")
    _put("data/audit/lambda-inventory.json",
         {"generated_at": ts, "n_functions": len(inventory),
          "n_scheduled": sum(1 for r in inventory.values() if r["scheduled"]),
          "functions": inventory,
          "note": "SPEC D1 — env VALUES are never recorded, only key names."})
    _put("data/audit/lambda-config-issues.json",
         {"generated_at": ts, "n_flagged": len(issues), "issues": issues,
          "checks": ["memory<1024MB", "no schedule", "timeout<30s",
                     "empty env value", "hardcoded model id",
                     "trailing-dot value"]})
    _put("data/audit/lambda-health.json",
         {"generated_at": ts, "alive": alive, "dead": len(dead),
          "unknown": unknown, "dead_functions": dead,
          "note": "SPEC D3 — a scheduled function with no logs in 26h is "
                  "DEAD. This is the source of truth the restart guard (D5) "
                  "checks so the mechanical bot cannot crash on ghosts."})

    res = {"ok": True, "n_functions": len(inventory),
           "n_scheduled": sum(1 for r in inventory.values() if r["scheduled"]),
           "config_issues": len(issues), "dead": len(dead), "alive": alive}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
