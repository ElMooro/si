"""
justhodl-engine-registry — the fleet's live engine directory.

WHY THIS EXISTS
data/engine-registry.json had no writer since 2026-07-07. The home page's
page-data contract flagged it as "no source-bound writer", the engine
directory baked a 667-engine July snapshot next to a 900+ function fleet,
and nothing in the account could answer "what is this engine, how often does
it run, what does it publish" from live state. This function is that writer.

WHAT IT PUBLISHES (same top-level shape the July document had, so existing
readers keep working: generated_at, n_engines, count, engines{name -> record})
  engines[name] = {
    doc            Lambda description (what the July 'doc' field carried)
    outs           artifact keys this engine writes (from config/artifact-producers.json)
    reads          artifact keys it reads (same source)
    runtime, memory_mb, timeout_s, last_modified, package_type
    schedules      [{kind, name, state, expr}] — classic rules and Scheduler
    scheduled      True if any binding is ENABLED
  }

READ-ONLY toward everything except its own output key. Never invokes anything.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

VERSION = "1.0.0"
MARKER = "engine-registry v1.0.0 live fleet directory"

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/engine-registry.json"
PRODUCERS_KEY = "config/artifact-producers.json"

CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=60)
s3 = boto3.client("s3", config=CFG)
lam = boto3.client("lambda", config=CFG)
evb = boto3.client("events", config=CFG)
sch = boto3.client("scheduler", config=CFG)


def now():
    return datetime.now(timezone.utc)


def function_name(arn):
    if not isinstance(arn, str) or ":function:" not in arn:
        return None
    return arn.split(":function:")[1].split(":")[0]


def list_functions():
    out = {}
    for page in lam.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            out[f["FunctionName"]] = {
                "doc": f.get("Description") or "",
                "runtime": f.get("Runtime") or "image",
                "memory_mb": f.get("MemorySize"),
                "timeout_s": f.get("Timeout"),
                "last_modified": f.get("LastModified"),
                "package_type": f.get("PackageType", "Zip"),
                "schedules": [],
            }
    return out


def attach_schedules(functions):
    for page in evb.get_paginator("list_rules").paginate():
        for r in page["Rules"]:
            if not r.get("ScheduleExpression"):
                continue
            try:
                targets = evb.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
            except Exception as e:  # one unreadable rule must not sink the directory
                print("[registry] list_targets_by_rule %s: %s" % (r["Name"], str(e)[:80]))
                continue
            for t in targets:
                fn = function_name(t.get("Arn"))
                if fn in functions:
                    functions[fn]["schedules"].append(
                        {"kind": "events", "name": r["Name"], "state": r.get("State"),
                         "expr": r.get("ScheduleExpression")})
    for page in sch.get_paginator("list_schedules").paginate():
        for s in page["Schedules"]:
            fn = function_name((s.get("Target") or {}).get("Arn"))
            if fn in functions:
                functions[fn]["schedules"].append(
                    {"kind": "scheduler", "name": s["Name"], "group": s.get("GroupName"),
                     "state": s.get("State"), "expr": None})
    for f in functions.values():
        f["scheduled"] = any(x.get("state") == "ENABLED" for x in f["schedules"])


def attach_outputs(functions):
    try:
        prod = json.loads(s3.get_object(Bucket=BUCKET, Key=PRODUCERS_KEY)["Body"].read())
    except Exception as e:
        print("[registry] producers map unavailable: %s" % str(e)[:80])
        return None
    outs, reads = {}, {}
    for key, rec in (prod.get("producers") or {}).items():
        for w in rec.get("writers") or []:
            outs.setdefault(w, []).append(key)
        for r in rec.get("readers") or []:
            reads.setdefault(r, []).append(key)
    for name, f in functions.items():
        f["outs"] = sorted(outs.get(name, []))
        f["reads"] = sorted(reads.get(name, []))
    return prod.get("generated_at")


def build():
    functions = list_functions()
    attach_schedules(functions)
    producers_asof = attach_outputs(functions)
    doc = {
        "version": VERSION, "marker": MARKER,
        "generated_at": now().isoformat(),
        "source": "justhodl-engine-registry: live Lambda inventory + EventBridge/Scheduler bindings + "
                  "config/artifact-producers.json",
        "producers_generated_at": producers_asof,
        "n_engines": len(functions), "count": len(functions),
        "n_scheduled": sum(1 for f in functions.values() if f["scheduled"]),
        "runtimes": {},
        "engines": dict(sorted(functions.items())),
    }
    for f in functions.values():
        doc["runtimes"][f["runtime"]] = doc["runtimes"].get(f["runtime"], 0) + 1
    return doc


def lambda_handler(event=None, context=None):
    t0 = time.time()
    doc = build()
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(doc, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    out = {"ok": True, "n_engines": doc["n_engines"], "n_scheduled": doc["n_scheduled"],
           "runtimes": doc["runtimes"], "elapsed_s": round(time.time() - t0, 1)}
    print("[registry] %s" % json.dumps(out))
    return out
