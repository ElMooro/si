"""ops 1120 — Phase D: migrate the 72 unmapped EB rules (granular daily-XX-utc + every_Nh) to tick rules.

Extends the canonical-tick taxonomy with 12 more buckets:
  daily-06utc..daily-19utc (8 specific UTC hours)  +  every_2h/3h/6h/12h (4 multi-hour)

Same safety pattern as 1107: per-cadence atomic — update manifest → create tick rule → sync-verify
scheduler can invoke → delete old individual rules → next cadence.
"""
import json, os, re, time, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"; ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
BUCKET = "justhodl-dashboard-live"
MANIFEST_KEY = "config/schedule-manifest.json"
SCHEDULER_ARN = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:justhodl-scheduler"

events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# Phase D extends the canonical map with granular daily hours + multi-hour intervals
TICK_CRON = {
    # daily-XX-utc — for cron(0 H * * ? *) patterns
    "daily-06utc": "cron(0 6 * * ? *)",
    "daily-07utc": "cron(0 7 * * ? *)",
    "daily-08utc": "cron(0 8 * * ? *)",
    "daily-15utc": "cron(0 15 * * ? *)",
    "daily-16utc": "cron(0 16 * * ? *)",
    "daily-17utc": "cron(0 17 * * ? *)",
    "daily-18utc": "cron(0 18 * * ? *)",
    "daily-19utc": "cron(0 19 * * ? *)",
    # multi-hour intervals
    "every_2h":  "cron(0 0/2 * * ? *)",
    "every_3h":  "cron(0 0/3 * * ? *)",
    "every_6h":  "cron(0 0/6 * * ? *)",
    "every_12h": "cron(0 0/12 * * ? *)",
}


def classify_cadence(expr):
    s = (expr or "").strip().lower()
    m = re.match(r"cron\(([^)]+)\)", s)
    if not m: return "unmapped"
    parts = m.group(1).split()
    if len(parts) < 6: return "unmapped"
    mn, hr, dom, mon, dow, _ = parts[:6]
    # multi-hour step
    if "/" in hr:
        step = int(hr.split("/")[1])
        if step in (2, 3, 6, 12): return f"every_{step}h"
    # specific hour daily
    if mn.isdigit() and hr.isdigit():
        h = int(hr)
        if dow.upper() != "?" and dow.upper() != "*": return "unmapped"  # weekly
        if dom != "?" and dom != "*": return "unmapped"  # monthly
        if 6 <= h <= 19:
            return f"daily-{h:02d}utc"
    return "unmapped"


def load_manifest():
    return json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())


def save_manifest(m):
    m["generated_at"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                  Body=json.dumps(m, indent=2).encode("utf-8"),
                  ContentType="application/json")


def ensure_tick_rule(tick):
    name = f"jhk-tick-{tick}"
    events.put_rule(Name=name, ScheduleExpression=TICK_CRON[tick], State="ENABLED",
                    Description=f"JustHodl scheduler fanout - {tick} (Phase D)")
    events.put_targets(Rule=name, Targets=[{
        "Id": "1", "Arn": SCHEDULER_ARN,
        "Input": json.dumps({"tick": tick})
    }])
    return name


def verify_scheduler(tick, expected):
    r = lam.invoke(FunctionName="justhodl-scheduler",
                   InvocationType="RequestResponse",
                   LogType="Tail",
                   Payload=json.dumps({"tick": tick}).encode())
    body = json.loads(r["Payload"].read())
    parsed = {}
    try:
        if isinstance(body, dict) and "body" in body:
            parsed = json.loads(body["body"])
    except Exception: pass
    ok = parsed.get("invoked_ok", 0); err = parsed.get("invoked_err", 0)
    return {"ok": ok, "err": err, "expected": expected,
            "verified": ok >= max(1, expected - 2) and err == 0}


def main():
    manifest = load_manifest()
    rpt = {"started": datetime.now(timezone.utc).isoformat(), "cadences": [], "summary": {}}

    # Inventory
    all_rules = []
    p = events.get_paginator("list_rules")
    for pg in p.paginate():
        all_rules.extend(pg.get("Rules", []))

    rule_by_target = {}
    cadence_targets = {}
    for r in all_rules:
        if r["Name"].startswith("jhk-tick-"): continue
        if not r.get("ScheduleExpression"): continue
        if r.get("State") == "DISABLED": continue
        cadence = classify_cadence(r["ScheduleExpression"])
        if cadence == "unmapped": continue
        try:
            tg = events.list_targets_by_rule(Rule=r["Name"])
            for t in tg.get("Targets", []):
                arn = t.get("Arn", "")
                if ":function:" in arn:
                    fn = arn.split(":function:")[-1]
                    rule_by_target.setdefault(fn, []).append(r["Name"])
                    cadence_targets.setdefault(cadence, [])
                    if fn not in cadence_targets[cadence]:
                        cadence_targets[cadence].append(fn)
        except ClientError: pass

    initial_rule_count = len(all_rules)
    deleted_total = 0
    ticks_created = 0

    # Order: biggest savings first
    order = sorted([t for t in cadence_targets if t in TICK_CRON],
                   key=lambda t: -len(cadence_targets[t]))

    for tick in order:
        targets = cadence_targets.get(tick) or []
        if not targets:
            rpt["cadences"].append({"tick": tick, "skipped": "empty"}); continue

        cr = {"tick": tick, "n_targets": len(targets)}

        manifest["ticks"][tick] = sorted(set(targets))
        save_manifest(manifest)
        cr["manifest"] = "UPDATED"

        try:
            ensure_tick_rule(tick); cr["rule"] = "CREATED_OR_UPDATED"; ticks_created += 1
        except ClientError as e:
            cr["rule_err"] = str(e)[:200]
            if "LimitExceeded" in str(e):
                cr["abort"] = "AT_CAP"; rpt["cadences"].append(cr); break
            rpt["cadences"].append(cr); continue

        try:
            lam.add_permission(FunctionName="justhodl-scheduler",
                               StatementId=f"EB-jhk-tick-{tick}",
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/jhk-tick-{tick}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceConflictException":
                cr["perm_err"] = str(e)[:120]

        time.sleep(2)
        v = verify_scheduler(tick, len(targets))
        cr["verify"] = v

        if v["verified"]:
            to_delete = set()
            for fn in targets:
                for rname in rule_by_target.get(fn, []):
                    to_delete.add(rname)
            to_delete = {r for r in to_delete if not r.startswith("jhk-tick-")}
            deleted = []
            for rname in sorted(to_delete):
                try:
                    tg = events.list_targets_by_rule(Rule=rname).get("Targets", [])
                    if tg: events.remove_targets(Rule=rname, Ids=[t["Id"] for t in tg])
                    events.delete_rule(Name=rname)
                    deleted.append(rname)
                except ClientError as e:
                    cr.setdefault("delete_errs", []).append({"rule": rname, "err": str(e)[:100]})
            cr["deleted_count"] = len(deleted); deleted_total += len(deleted)
        else:
            cr["deleted_count"] = 0; cr["note"] = "verify failed - rules retained"

        rpt["cadences"].append(cr)

    final_count = sum(len(p.get("Rules", [])) for p in events.get_paginator("list_rules").paginate())
    rpt["summary"] = {
        "initial_rule_count": initial_rule_count,
        "final_rule_count": final_count,
        "ticks_created": ticks_created,
        "individual_rules_deleted": deleted_total,
        "net_slots_freed": initial_rule_count - final_count,
    }
    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1120.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(rpt, open(out, "w"), indent=2, default=str)
    print(json.dumps(rpt["summary"], indent=2))


if __name__ == "__main__":
    main()
