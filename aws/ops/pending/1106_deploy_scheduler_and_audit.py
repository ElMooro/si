"""ops 1106 — deploy justhodl-scheduler + audit current EventBridge state + emit migration plan.
NO destructive changes yet — pure deploy + audit + plan generation."""
import io, json, os, re, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"; ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-scheduler"
BUCKET = "justhodl-dashboard-live"

events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(t=120):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException": raise
        time.sleep(3)
    return False


def classify_cadence(cron_or_rate):
    """Map a schedule expression to a canonical tick name."""
    s = cron_or_rate.strip().lower()
    # rate() forms
    m = re.match(r"rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)", s)
    if m:
        n = int(m.group(1)); unit = m.group(2)
        if unit.startswith("minute"):
            if n == 1: return "1min"
            if n == 5: return "5min"
            if n == 15: return "15min"
            if n == 30: return "30min"
            return f"every_{n}min"
        if unit.startswith("hour"):
            if n == 1: return "hourly"
            if n == 4: return "4hourly"
            return f"every_{n}h"
        if unit.startswith("day"): return "daily-eve"  # ambiguous; default
    # cron() forms — examples: cron(0/5 * * * ? *) -> 5min ; cron(0 * * * ? *) -> hourly
    m = re.match(r"cron\(([^)]+)\)", s)
    if m:
        parts = m.group(1).split()
        if len(parts) >= 6:
            mn, hr, dom, mon, dow, yr = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
            # minute granularity
            if mn.startswith("0/") or mn.startswith("*/"):
                step = int(mn.split("/")[1])
                if hr == "*":
                    if step == 5: return "5min"
                    if step == 15: return "15min"
                    if step == 30: return "30min"
                    return f"every_{step}min"
            if mn == "*" and hr == "*": return "1min"
            if "/" in hr:
                step = int(hr.split("/")[1])
                if step == 4: return "4hourly"
                return f"every_{step}h"
            if mn.isdigit() and hr == "*": return "hourly"
            if mn.isdigit() and hr.isdigit():
                h = int(hr)
                if dow.upper() == "SUN": return "weekly-sun"
                if dom == "1" or dom.startswith("1L"): return "monthly"
                if 9 <= h <= 14: return "daily-morn"
                if 20 <= h <= 23 or 0 <= h <= 4: return "daily-eve"
                return f"daily-{h:02d}utc"
    return "unmapped"


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat(), "phase_A": {}, "phase_B": {}}

    # ──────── PHASE A: deploy scheduler Lambda ────────
    cfg = json.load(open(os.path.join(REPO_ROOT, "aws/lambdas", FN, "config.json")))
    src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
    try:
        lam.get_function_configuration(FunctionName=FN); exists = True
    except ClientError:
        exists = False

    if not exists:
        try:
            lam.create_function(
                FunctionName=FN, Runtime=cfg["runtime"], Role=cfg["role"],
                Handler=cfg["handler"], Code={"ZipFile": zip_src(src_dir)},
                Description=cfg["description"][:255], Timeout=cfg["timeout"],
                MemorySize=cfg["memory"], Architectures=cfg["architectures"])
            rpt["phase_A"]["deploy"] = "CREATED"
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                rpt["phase_A"]["deploy"] = "RACED"; exists = True
            else:
                rpt["phase_A"]["err"] = str(e)[:300]
        wait_active()
    if exists:
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["phase_A"]["deploy"] = "CODE_SYNCED"

    # Permission for events to invoke
    try:
        lam.add_permission(FunctionName=FN, StatementId="EB-scheduler-tick-any",
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/jhk-tick-*")
        rpt["phase_A"]["perm"] = "ADDED"
    except ClientError as e:
        rpt["phase_A"]["perm"] = "EXISTS" if e.response["Error"]["Code"] == "ResourceConflictException" else f"err:{e}"

    # Seed empty manifest in S3 (idempotent — only writes if missing)
    try:
        s3.head_object(Bucket=BUCKET, Key="config/schedule-manifest.json")
        rpt["phase_A"]["manifest"] = "EXISTS"
    except ClientError:
        empty = {"version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                 "ticks": {t: [] for t in ["1min","5min","15min","30min","hourly","4hourly","daily-morn","daily-eve","weekly-sun","monthly"]},
                 "disabled": []}
        s3.put_object(Bucket=BUCKET, Key="config/schedule-manifest.json",
                      Body=json.dumps(empty, indent=2).encode("utf-8"),
                      ContentType="application/json")
        rpt["phase_A"]["manifest"] = "SEEDED"

    # ──────── PHASE B: audit existing EB rules ────────
    paginator = events.get_paginator("list_rules")
    all_rules = []
    for page in paginator.paginate():
        all_rules.extend(page.get("Rules", []))
    rpt["phase_B"]["total_rules"] = len(all_rules)

    by_cadence = {}
    rules_detail = []
    safe_delete = []  # rules we can delete cheaply to make room
    skip_rules = set()  # rules to NOT migrate (e.g. our own tick rules)

    for r in all_rules:
        name = r["Name"]
        if name.startswith("jhk-tick-"):  # our scheduler ticks (don't migrate)
            skip_rules.add(name); continue
        state = r.get("State")
        sched = r.get("ScheduleExpression", "")
        cadence = classify_cadence(sched) if sched else "event-driven"
        # Get target Lambda(s)
        targets = []
        try:
            t = events.list_targets_by_rule(Rule=name)
            for tg in t.get("Targets", []):
                arn = tg.get("Arn", "")
                if ":function:" in arn:
                    targets.append(arn.split(":function:")[-1])
        except ClientError:
            pass
        entry = {"name": name, "state": state, "schedule": sched, "cadence": cadence, "targets": targets}
        rules_detail.append(entry)
        by_cadence.setdefault(cadence, []).append(name)
        # Safe-delete candidates: explicitly DISABLED rules
        if state == "DISABLED":
            safe_delete.append(name)

    rpt["phase_B"]["by_cadence"] = {k: len(v) for k, v in by_cadence.items()}
    rpt["phase_B"]["safe_delete_disabled"] = safe_delete

    # ──────── Generate migration plan ────────
    # For each schedule-driven rule, propose: {old_rule, target_lambda, tick}
    KNOWN_TICKS = {"1min","5min","15min","30min","hourly","4hourly","daily-morn","daily-eve","weekly-sun","monthly"}
    proposed_ticks = {t: [] for t in KNOWN_TICKS}
    unmapped = []
    for rd in rules_detail:
        if not rd["schedule"]: continue       # event-driven, leave alone
        if rd["state"] == "DISABLED": continue
        if not rd["targets"]: continue
        cad = rd["cadence"]
        if cad in KNOWN_TICKS:
            for fn in rd["targets"]:
                if fn not in proposed_ticks[cad]:
                    proposed_ticks[cad].append(fn)
        else:
            unmapped.append({"rule": rd["name"], "schedule": rd["schedule"], "cadence": cad, "targets": rd["targets"]})

    plan = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "current_state": {
            "total_rules": len(all_rules),
            "by_cadence_count": rpt["phase_B"]["by_cadence"],
            "disabled_count": len(safe_delete),
        },
        "proposed_ticks": proposed_ticks,
        "proposed_tick_counts": {t: len(v) for t, v in proposed_ticks.items()},
        "unmapped_cadences": unmapped,
        "safe_delete_disabled": safe_delete,
        "after_migration_estimated_rules": 10 + len([r for r in rules_detail if not r["schedule"]]),  # 10 ticks + event-driven
    }
    out_path = os.path.join(REPO_ROOT, "data/schedule-migration-plan.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    json.dump(plan, open(out_path, "w"), indent=2, default=str)
    rpt["phase_B"]["plan_path"] = "data/schedule-migration-plan.json"
    rpt["phase_B"]["plan_summary"] = {
        "tick_counts": plan["proposed_tick_counts"],
        "unmapped_count": len(unmapped),
        "current": len(all_rules), "after_full_migration": plan["after_migration_estimated_rules"]
    }

    # Save report
    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    out_rpt = os.path.join(REPO_ROOT, "aws/ops/reports/1106.json")
    os.makedirs(os.path.dirname(out_rpt), exist_ok=True)
    json.dump(rpt, open(out_rpt, "w"), indent=2, default=str)
    print(json.dumps(rpt, indent=2, default=str)[:4000])


if __name__ == "__main__":
    main()
