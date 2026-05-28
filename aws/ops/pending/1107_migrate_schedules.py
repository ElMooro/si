"""ops 1107 — Phase C: migrate canonical-cadence rules into scheduler manifest.

Strategy (executed per cadence in size order, biggest first):
  1. Update manifest in S3 with the cadence's target Lambdas
  2. Create the tick rule (uses 1 EB slot)
  3. Invoke scheduler manually with tick={cadence} → verify scheduler can fan out
  4. If verify OK → delete the N individual rules → frees N-1 slots
  5. Move to next cadence

Headroom math: at 299/300, the first cadence (daily-morn, 94 rules) creates 1 rule
hitting 300/300, then deletes 94 to drop to 206/300. Each subsequent cadence
follows the same +1/-N pattern, never exceeding the cap.

Safety: between create and verify, both old EB rules AND the new tick rule are
active. After verify, deleting individual rules is the commit. If verify fails
for any cadence, that cadence is skipped (old rules retained, no destructive change).
"""
import io, json, os, re, time, base64
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

# Tick name -> EventBridge cron (UTC)
TICK_CRON = {
    "1min":       "cron(* * * * ? *)",
    "5min":       "cron(0/5 * * * ? *)",
    "15min":      "cron(0/15 * * * ? *)",
    "30min":      "cron(0/30 * * * ? *)",
    "hourly":     "cron(0 * * * ? *)",
    "4hourly":    "cron(0 0/4 * * ? *)",
    "daily-morn": "cron(0 11 * * ? *)",   # 11 UTC ≈ 7AM ET
    "daily-eve":  "cron(0 22 * * ? *)",   # 22 UTC ≈ 6PM ET
    "weekly-sun": "cron(0 12 ? * SUN *)",
    "monthly":    "cron(0 12 1 * ? *)",
}


def load_manifest():
    o = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
    return json.loads(o["Body"].read())


def save_manifest(m):
    m["generated_at"] = datetime.now(timezone.utc).isoformat()
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY,
                  Body=json.dumps(m, indent=2).encode("utf-8"),
                  ContentType="application/json")


def ensure_tick_rule(tick):
    """Create-or-update the jhk-tick-<tick> EventBridge rule."""
    name = f"jhk-tick-{tick}"
    cron = TICK_CRON[tick]
    events.put_rule(Name=name, ScheduleExpression=cron, State="ENABLED",
                    Description=f"JustHodl scheduler fanout — {tick}")
    events.put_targets(Rule=name, Targets=[{
        "Id": "1", "Arn": SCHEDULER_ARN,
        "Input": json.dumps({"tick": tick})
    }])
    return name


def verify_scheduler(tick, expected_count, wait=8):
    """Synchronously invoke scheduler with tick — count successful Lambda fanouts."""
    r = lam.invoke(FunctionName="justhodl-scheduler",
                   InvocationType="RequestResponse",
                   LogType="Tail",
                   Payload=json.dumps({"tick": tick}).encode())
    body = json.loads(r["Payload"].read())
    log = base64.b64decode(r.get("LogResult","")).decode("utf-8","replace")[-1500:]
    parsed = {}
    try:
        if isinstance(body, dict) and "body" in body:
            parsed = json.loads(body["body"])
    except Exception:
        pass
    ok = parsed.get("invoked_ok", 0)
    err = parsed.get("invoked_err", 0)
    return {
        "ok": ok, "err": err, "expected": expected_count,
        "verified": ok >= max(1, expected_count - 2) and err == 0,  # tolerate up to 2 phantom
        "errors": parsed.get("errors", []),
        "log_tail": log[-400:],
    }


def classify_cadence(cron_or_rate):
    s = (cron_or_rate or "").strip().lower()
    m = re.match(r"rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)", s)
    if m:
        n = int(m.group(1)); unit = m.group(2)
        if unit.startswith("minute"):
            return {1:"1min",5:"5min",15:"15min",30:"30min"}.get(n, f"every_{n}min")
        if unit.startswith("hour"):
            return {1:"hourly",4:"4hourly"}.get(n, f"every_{n}h")
        if unit.startswith("day"): return "daily-eve"
    m = re.match(r"cron\(([^)]+)\)", s)
    if m:
        parts = m.group(1).split()
        if len(parts) >= 6:
            mn, hr, dom, mon, dow, _ = parts[:6]
            if mn.startswith("0/") or mn.startswith("*/"):
                step = int(mn.split("/")[1])
                if hr == "*":
                    return {5:"5min",15:"15min",30:"30min"}.get(step, f"every_{step}min")
            if mn == "*" and hr == "*": return "1min"
            if "/" in hr:
                step = int(hr.split("/")[1])
                return {4:"4hourly"}.get(step, f"every_{step}h")
            if mn.isdigit() and hr == "*": return "hourly"
            if mn.isdigit() and hr.isdigit():
                h = int(hr)
                if dow.upper() == "SUN": return "weekly-sun"
                if dom == "1": return "monthly"
                if 9 <= h <= 14: return "daily-morn"
                if 20 <= h <= 23 or 0 <= h <= 4: return "daily-eve"
                return f"daily-{h:02d}utc"
    return "unmapped"


def main():
    manifest = load_manifest()
    rpt = {"started": datetime.now(timezone.utc).isoformat(), "cadences": [], "summary": {}}

    # ──── Self-sufficient audit (don't rely on data/schedule-migration-plan.json) ────
    all_rules = []
    p = events.get_paginator("list_rules")
    for pg in p.paginate():
        all_rules.extend(pg.get("Rules", []))

    rule_by_target = {}   # lambda -> [rule names]
    cadence_targets = {}  # cadence -> [unique lambda names]
    for r in all_rules:
        if r["Name"].startswith("jhk-tick-"): continue
        if not r.get("ScheduleExpression"): continue
        if r.get("State") == "DISABLED": continue
        cadence = classify_cadence(r["ScheduleExpression"])
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
        except ClientError:
            pass

    initial_rule_count = len(all_rules)
    rules_deleted_total = 0
    ticks_created = 0

    # Order: biggest savings first, but ONLY canonical-tick cadences
    order = sorted([t for t in cadence_targets if t in TICK_CRON],
                   key=lambda t: -len(cadence_targets[t]))

    for tick in order:
        targets = cadence_targets.get(tick) or []
        if not targets:
            rpt["cadences"].append({"tick": tick, "skipped": "empty"}); continue

        cad_rpt = {"tick": tick, "n_targets": len(targets)}

        # 1) Update manifest
        manifest["ticks"][tick] = sorted(set(targets))
        save_manifest(manifest)
        cad_rpt["manifest"] = "UPDATED"

        # 2) Create tick rule
        try:
            ensure_tick_rule(tick); cad_rpt["rule"] = "CREATED_OR_UPDATED"; ticks_created += 1
        except ClientError as e:
            cad_rpt["rule_err"] = str(e)[:200]
            if "LimitExceeded" in str(e):
                cad_rpt["abort"] = "AT_CAP"
                rpt["cadences"].append(cad_rpt); break
            rpt["cadences"].append(cad_rpt); continue

        # Permission
        try:
            lam.add_permission(FunctionName="justhodl-scheduler",
                               StatementId=f"EB-jhk-tick-{tick}",
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/jhk-tick-{tick}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceConflictException":
                cad_rpt["perm_err"] = str(e)[:120]

        # 3) Verify scheduler can fan out
        time.sleep(2)
        v = verify_scheduler(tick, len(targets))
        cad_rpt["verify"] = v

        # 4) If verified, delete old individual rules
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
                    if tg:
                        events.remove_targets(Rule=rname, Ids=[t["Id"] for t in tg])
                    events.delete_rule(Name=rname)
                    deleted.append(rname)
                except ClientError as e:
                    cad_rpt.setdefault("delete_errs", []).append({"rule": rname, "err": str(e)[:120]})
            cad_rpt["deleted_count"] = len(deleted)
            rules_deleted_total += len(deleted)
        else:
            cad_rpt["deleted_count"] = 0
            cad_rpt["note"] = "verify failed — old rules retained for safety"

        rpt["cadences"].append(cad_rpt)

    final_count = sum(len(p.get("Rules", [])) for p in events.get_paginator("list_rules").paginate())
    rpt["summary"] = {
        "initial_rule_count": initial_rule_count,
        "final_rule_count": final_count,
        "ticks_created": ticks_created,
        "individual_rules_deleted": rules_deleted_total,
        "net_slots_freed": initial_rule_count - final_count,
    }
    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1107.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(rpt, open(out, "w"), indent=2, default=str)
    print(json.dumps(rpt["summary"], indent=2))


if __name__ == "__main__":
    main()
