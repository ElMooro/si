"""ops 6555 - substrate for the $600 cap and governor v1.2.0 (Khalid, 2026-10-20: "never goes past $600").

Runs BEFORE the governor deploy (the function's first tick on v1.2.0 reads the new budget):
  1. AWS Budget JustHodl-Monthly-Hard-Cap-600: a clone of the -500 budget (same cost types / filters, open-ended),
     with the same notifications and subscribers (the e-mail addresses are copied from the existing budget inside the
     runner - never typed into this public repo; SNS topic justhodl-spend-governor so the governor ticks on alerts).
  2. Governor role: + events/scheduler read + retime rights for the daily cadence sentinel (iam:PassRole limited to
     the Scheduler role, because UpdateSchedule passes the target's role back).
  3. control.json: fuse_interval_h 2 (was 6) so a diffuse storm with no guard alarm is seen within two hours.
  4. JustHodl-Daily-Spend-Guard: $40 -> $30 a day (normal days are ~$22; the once-a-day cadence should take it lower).
  5. The legacy "My Monthly Cost Budget" ($150, e-mail only, five alerts a month that mean nothing) is deleted.
The -500 budget stays until ops 6556 has verified the deployed governor reads -600.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
OLD, NEW, DAILY, LEGACY = "JustHodl-Monthly-Hard-Cap-500", "JustHodl-Monthly-Hard-Cap-600", "JustHodl-Daily-Spend-Guard", "My Monthly Cost Budget"
ROLE, POLICY = "justhodl-spend-governor-role", "justhodl-spend-governor"
SCHEDULER_ROLE = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCOUNT
GOV_TOPIC = "arn:aws:sns:%s:%s:justhodl-spend-governor" % (REGION, ACCOUNT)
STATE_BUCKET = "justhodl-ops-private-%s" % ACCOUNT
NO_END = datetime(2087, 6, 15, tzinfo=timezone.utc)
CFG = Config(retries={"max_attempts": 8, "mode": "standard"})


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def mask(addr):
    return addr[:2] + "***" + addr[addr.find("@"):] if "@" in addr else "***"


def budget_600(r):
    b = c("budgets")
    old = b.describe_budget(AccountId=ACCOUNT, BudgetName=OLD)["Budget"]
    try:
        cur = b.describe_budget(AccountId=ACCOUNT, BudgetName=NEW)["Budget"]
        r.log("%s exists: limit %s" % (NEW, cur["BudgetLimit"]["Amount"]))
    except b.exceptions.NotFoundException:
        nb = {k: old[k] for k in ("CostFilters", "CostTypes", "TimeUnit", "BudgetType") if k in old and old[k] not in (None, {})}
        nb.update({"BudgetName": NEW, "BudgetLimit": {"Amount": "600", "Unit": "USD"},
                   "TimePeriod": {"Start": old["TimePeriod"]["Start"], "End": NO_END}})
        b.create_budget(AccountId=ACCOUNT, Budget=nb)
        r.ok("created %s ($600 MONTHLY, open-ended, cost types cloned from %s)" % (NEW, OLD))
    old_notes = b.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=OLD).get("Notifications", [])
    new_notes = b.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=NEW).get("Notifications", [])
    have = {(n["NotificationType"], n["ComparisonOperator"], float(n["Threshold"])) for n in new_notes}
    added = []
    for n in old_notes:
        key = (n["NotificationType"], n["ComparisonOperator"], float(n["Threshold"]))
        if key in have:
            continue
        subs = b.describe_subscribers_for_notification(AccountId=ACCOUNT, BudgetName=OLD, Notification=n)["Subscribers"]
        subs = [{"SubscriptionType": s["SubscriptionType"], "Address": s["Address"]} for s in subs]
        if not any(s["SubscriptionType"] == "SNS" for s in subs):
            subs.append({"SubscriptionType": "SNS", "Address": GOV_TOPIC})
        note = {k: n[k] for k in ("NotificationType", "ComparisonOperator", "Threshold", "ThresholdType") if k in n}
        b.create_notification(AccountId=ACCOUNT, BudgetName=NEW, Notification=note, Subscribers=subs)
        added.append("%s %s%% -> %s" % (n["NotificationType"], n["Threshold"],
                                       ", ".join(sorted({s["SubscriptionType"] + ":" + (mask(s["Address"]) if s["SubscriptionType"] == "EMAIL" else "governor topic") for s in subs}))))
    r.ok("notifications on %s: %d (added %d: %s)" % (NEW, len(have) + len(added), len(added), "; ".join(added) or "-"))
    after = b.describe_budget(AccountId=ACCOUNT, BudgetName=NEW)["Budget"]
    return {"limit": after["BudgetLimit"]["Amount"], "actual": (after.get("CalculatedSpend") or {}).get("ActualSpend", {}).get("Amount"),
            "notifications": len(have) + len(added)}


def role_policy(r):
    iam = c("iam")
    doc = iam.get_role_policy(RoleName=ROLE, PolicyName=POLICY)["PolicyDocument"]
    stmts = [s for s in doc["Statement"] if s.get("Sid") not in ("CadenceSentinel", "CadenceSentinelPassRole")]
    stmts.append({"Sid": "CadenceSentinel", "Effect": "Allow", "Action": [
        "events:ListEventBuses", "events:ListRules", "events:DescribeRule", "events:PutRule",
        "scheduler:ListScheduleGroups", "scheduler:ListSchedules", "scheduler:GetSchedule", "scheduler:UpdateSchedule"],
        "Resource": "*"})
    stmts.append({"Sid": "CadenceSentinelPassRole", "Effect": "Allow", "Action": "iam:PassRole", "Resource": SCHEDULER_ROLE,
                  "Condition": {"StringEquals": {"iam:PassedToService": "scheduler.amazonaws.com"}}})
    doc["Statement"] = stmts
    iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY, PolicyDocument=json.dumps(doc))
    r.ok("governor role: + cadence sentinel rights (events/scheduler list, describe, retime; PassRole to Scheduler only)")
    return len(stmts)


def control(r):
    s3 = c("s3")
    key = "spend-governor/control.json"
    ctl = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key=key)["Body"].read())
    before = {k: ctl.get(k) for k in ("mode", "fuse_interval_h", "cadence_enforce")}
    changed = False
    if ctl.get("fuse_interval_h") not in (None, 2):
        ctl["fuse_interval_h"] = 2
        changed = True
    if changed:
        s3.put_object(Bucket=STATE_BUCKET, Key=key, Body=json.dumps(ctl, indent=1, sort_keys=True).encode(),
                      ContentType="application/json", ServerSideEncryption="AES256")
    r.log("control.json: mode=%s fuse_interval_h %s -> %s (cadence_enforce default True)" % (
        ctl.get("mode"), before["fuse_interval_h"], ctl.get("fuse_interval_h")))
    if ctl.get("mode") != "enforce":
        r.warn("governor mode is %s, not enforce" % ctl.get("mode"))
    return ctl.get("mode")


def daily_guard(r):
    b = c("budgets")
    bud = b.describe_budget(AccountId=ACCOUNT, BudgetName=DAILY)["Budget"]
    if float(bud["BudgetLimit"]["Amount"]) != 30.0:
        nb = {k: bud[k] for k in ("BudgetName", "CostFilters", "CostTypes", "TimeUnit", "BudgetType", "TimePeriod")
              if k in bud and bud[k] not in (None, {})}
        nb["BudgetLimit"] = {"Amount": "30", "Unit": "USD"}
        b.update_budget(AccountId=ACCOUNT, NewBudget=nb)
        r.ok("%s: $%s -> $30 a day" % (DAILY, bud["BudgetLimit"]["Amount"]))
    else:
        r.log("%s already $30" % DAILY)


def delete_legacy(r):
    b = c("budgets")
    try:
        b.describe_budget(AccountId=ACCOUNT, BudgetName=LEGACY)
    except b.exceptions.NotFoundException:
        r.log("%s already gone" % LEGACY)
        return
    b.delete_budget(AccountId=ACCOUNT, BudgetName=LEGACY)
    r.ok("deleted legacy budget %r ($150, e-mail only)" % LEGACY)


def main():
    with report("ops_6555_cap_600_substrate") as r:
        r.heading("ops 6555 - $600 cap substrate (budget, role, control, daily guard, legacy budget)")
        res = budget_600(r)
        n = role_policy(r)
        mode = control(r)
        daily_guard(r)
        delete_legacy(r)
        time.sleep(5)
        names = sorted(x["BudgetName"] for x in c("budgets").describe_budgets(AccountId=ACCOUNT)["Budgets"])
        r.kv(new_budget_limit=res["limit"], actual=res["actual"], notifications=res["notifications"],
             policy_statements=n, mode=mode, budgets=", ".join(names))
        r.ok("substrate ready; deploy the governor (v1.2.0 reads %s), then ops 6556 verifies and retires %s" % (NEW, OLD))


if __name__ == "__main__":
    main()
