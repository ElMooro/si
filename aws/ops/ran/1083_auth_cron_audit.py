#!/usr/bin/env python3
"""1083 — scan ALL Lambdas for same auth-gate-on-cron silent failure.

Pattern: Lambda has both:
  1. authorize(event, allowed_origins=...) as first step
  2. An EventBridge schedule (cron/rate)

These will silently 401 on every scheduled invocation, leaving stale
outputs. The S3 file's last_modified will trail by ~ the date the auth
gate was added (2026-05-06).
"""
import io, json, os, pathlib, urllib.request, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1083_auth_cron_audit.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    print("[1083] phase 1: find Lambdas with authorize() in code…")
    paginator = lam.get_paginator("list_functions")
    auth_users = []
    n_scanned = 0
    for page in paginator.paginate():
        for f in page["Functions"]:
            n_scanned += 1
            name = f["FunctionName"]
            try:
                info = lam.get_function(FunctionName=name)
                url = info["Code"]["Location"]
                with urllib.request.urlopen(url, timeout=30) as r:
                    zip_bytes = r.read()
                with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                    for fname in zf.namelist():
                        if not fname.endswith(".py"):
                            continue
                        try:
                            content = zf.read(fname).decode("utf-8", errors="replace")
                        except Exception:
                            continue
                        # Look for: authorize(event AND allowed_origins
                        if "authorize(event" in content and "allowed_origins" in content:
                            # Check if it has the ops/1082 bypass already
                            has_bypass = "Internal invocation bypass (ops/1082)" in content
                            auth_users.append({
                                "name":         name,
                                "file":         fname,
                                "has_bypass":   has_bypass,
                                "last_modified": info["Configuration"].get("LastModified"),
                            })
                            break
            except Exception:
                continue
    
    out["n_scanned"]    = n_scanned
    out["auth_lambdas"] = auth_users
    
    # Phase 2: for each, check if it has an EventBridge schedule
    print(f"[1083] phase 2: check schedules for {len(auth_users)} auth-using Lambdas…")
    for entry in auth_users:
        name = entry["name"]
        try:
            target_arn = f"arn:aws:lambda:{REGION}:857687956942:function:{name}"
            result = events.list_rule_names_by_target(TargetArn=target_arn)
            rule_names = result.get("RuleNames", [])
            schedules = []
            for rn in rule_names:
                r = events.describe_rule(Name=rn)
                schedules.append({
                    "name": rn,
                    "expr": r.get("ScheduleExpression"),
                    "state": r.get("State"),
                })
            entry["schedules"] = schedules
        except Exception as e:
            entry["schedule_err"] = str(e)[:100]
    
    # Phase 3: classify
    affected = []        # has schedule + no bypass = SILENTLY BROKEN
    fixed = []           # has schedule + bypass = OK
    no_schedule = []     # no schedule = HTTP-only, not affected
    
    for entry in auth_users:
        has_sched = bool(entry.get("schedules"))
        has_bypass = entry.get("has_bypass")
        if has_sched and not has_bypass:
            affected.append(entry)
        elif has_sched and has_bypass:
            fixed.append(entry)
        else:
            no_schedule.append(entry)
    
    out["summary"] = {
        "total_auth_lambdas":  len(auth_users),
        "silently_broken":     len(affected),
        "fixed_already":       len(fixed),
        "no_schedule":         len(no_schedule),
    }
    out["affected"]    = affected
    out["fixed"]       = fixed
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1083] DONE — scanned {n_scanned}, found {len(auth_users)} with auth, "
            f"{len(affected)} silently broken")


if __name__ == "__main__":
    main()
