"""ops/749 — apply the APPROVED refresh-cadence right-sizing (ops 748 plan).

Mutates EventBridge schedule rules. Khalid approved this plan explicitly.
Each operation captures before + after state for a full audit trail and
self-verifies. All 7 target rules are standalone (not config-managed),
so no config.json edits are needed.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(retries={"max_attempts": 4})
events = boto3.client("events", region_name="us-east-1", config=cfg)

report = {"ops": 749, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "apply approved refresh-cadence right-sizing"}

# action: 'delete' | 'disable' | 'reschedule'
PLAN = [
    {"rule": "justhodl-v9-auto-refresh",        "action": "delete",
     "why": "daily-report-v3 already has morning+evening crons; 5-min on macro data is waste"},
    {"rule": "test-immediate",                  "action": "delete",
     "why": "leftover 1-minute test rule"},
    {"rule": "news-sentiment-update",           "action": "disable",
     "why": "target news-sentiment-agent is a stub"},
    {"rule": "bloomberg-terminal-refresh",      "action": "reschedule",
     "new": "rate(1 hour)",
     "why": "bloomberg-v8 calls Claude every run — 5-min was the biggest token cost"},
    {"rule": "justhodl-crypto-intel-schedule",  "action": "reschedule",
     "new": "rate(30 minutes)",
     "why": "crypto-intel calls Claude; 15-min AI commentary is overkill"},
    {"rule": "bond-indices-hourly",             "action": "reschedule",
     "new": "cron(0 12 * * ? *)",
     "why": "bond/ICE indices update daily, not hourly"},
    {"rule": "justhodl-history-snapshotter-5m", "action": "reschedule",
     "new": "rate(30 minutes)",
     "why": "snapshotting slow macro data every 5 min"},
]


def describe(name):
    try:
        r = events.describe_rule(Name=name)
        return {"exists": True, "schedule": r.get("ScheduleExpression"),
                "state": r.get("State"), "description": r.get("Description", "")}
    except events.exceptions.ResourceNotFoundException:
        return {"exists": False}
    except Exception as e:
        return {"exists": None, "err": str(e)[:160]}


results = []
for item in PLAN:
    name, action = item["rule"], item["action"]
    row = {"rule": name, "action": action, "why": item["why"]}
    before = describe(name)
    row["before"] = before

    if not before.get("exists"):
        row["result"] = "SKIPPED — rule not found"
        results.append(row)
        continue

    try:
        if action == "delete":
            tg = events.list_targets_by_rule(Rule=name).get("Targets", [])
            ids = [t["Id"] for t in tg]
            if ids:
                events.remove_targets(Rule=name, Ids=ids)
            events.delete_rule(Name=name)
            after = describe(name)
            row["after"] = after
            row["result"] = ("DELETED" if not after.get("exists")
                             else "FAILED — still exists")

        elif action == "disable":
            events.disable_rule(Name=name)
            after = describe(name)
            row["after"] = after
            row["result"] = ("DISABLED" if after.get("state") == "DISABLED"
                             else f"CHECK — state={after.get('state')}")

        elif action == "reschedule":
            new = item["new"]
            kw = {"Name": name, "ScheduleExpression": new,
                  "State": before.get("state") or "ENABLED"}
            if before.get("description"):
                kw["Description"] = before["description"]
            events.put_rule(**kw)
            after = describe(name)
            row["after"] = after
            row["result"] = ("RESCHEDULED" if after.get("schedule") == new
                             else f"CHECK — schedule={after.get('schedule')}")
    except Exception as e:
        row["result"] = f"ERROR — {str(e)[:200]}"
    results.append(row)

report["operations"] = results
report["n_total"] = len(results)
report["n_ok"] = sum(1 for r in results if r["result"] in
                     ("DELETED", "DISABLED", "RESCHEDULED"))
report["n_skipped"] = sum(1 for r in results if "SKIPPED" in r["result"])
report["all_applied"] = (report["n_ok"] + report["n_skipped"]) == report["n_total"]
report["verdict"] = (
    f"APPLIED — {report['n_ok']} cadence changes done"
    + (f", {report['n_skipped']} skipped (rule absent)" if report["n_skipped"] else "")
    if report["all_applied"]
    else "REVIEW — one or more operations failed; see operations[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/749_cadence_changes_applied.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/749_cadence_changes_applied.json")
