"""ops 1095 — surgical cleanup + schedule creation
   (auto-continues from ops 1094 audit findings)

DELETES (audit-confirmed safe, all DISABLED, do not fire):
  1. autonomous-ai-schedule  → target: autonomous-ai-processor
  2. justhodl-8am            → target: justhodl-email-reports
  3. news-sentiment-update   → target: news-sentiment-agent

CREATES:
  1. tax-plan-daily          → cron(45 11 ? * * *)  → justhodl-tax-plan
  2. wealth-plan-daily-warmup → cron(30 11 ? * * *) → justhodl-wealth-plan

Net delta: -3 + 2 = -1 (300 → 299, frees 1 slot for future)
End state: tax-plan + wealth-plan have daily warmups, freshness alerts active.
"""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

DELETIONS = [
    "autonomous-ai-schedule",
    "justhodl-8am",
    "news-sentiment-update",
]

CREATIONS = [
    {
        "rule_name": "tax-plan-daily",
        "cron": "cron(45 11 ? * * *)",
        "target_fn": "justhodl-tax-plan",
        "description": "Daily 11:45 UTC tax-plan default snapshot refresh",
    },
    {
        "rule_name": "wealth-plan-daily-warmup",
        "cron": "cron(30 11 ? * * *)",
        "target_fn": "justhodl-wealth-plan",
        "description": "Daily 11:30 UTC wealth-plan warmup + snapshot refresh",
    },
]


def delete_rule(rule_name):
    """Safely delete a rule: first remove all targets, then delete rule."""
    out = {"rule": rule_name}
    try:
        # 1. List + remove all targets (required before delete)
        t = events.list_targets_by_rule(Rule=rule_name)
        target_ids = [tt["Id"] for tt in t.get("Targets", [])]
        if target_ids:
            events.remove_targets(Rule=rule_name, Ids=target_ids)
            out["targets_removed"] = len(target_ids)
        # 2. Delete the rule itself
        events.delete_rule(Name=rule_name)
        out["status"] = "DELETED"
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            out["status"] = "ALREADY_GONE"
        else:
            out["status"] = "ERR"
            out["error"] = str(e)[:200]
    return out


def create_schedule(spec):
    out = {"rule": spec["rule_name"]}
    try:
        fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{spec['target_fn']}"
        events.put_rule(
            Name=spec["rule_name"],
            ScheduleExpression=spec["cron"],
            State="ENABLED",
            Description=spec["description"][:512],
        )
        events.put_targets(Rule=spec["rule_name"], Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=spec["target_fn"],
                StatementId=f"AllowEB-{spec['rule_name']}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{spec['rule_name']}",
            )
            out["permission"] = "ADDED"
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                out["permission"] = "EXISTS"
            else:
                raise
        out["status"] = "CREATED"
        out["cron"] = spec["cron"]
    except ClientError as e:
        out["status"] = "ERR"
        out["error"] = str(e)[:200]
    return out


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # 1. Deletions first (free slots)
    print("Phase 1: deletions")
    report["deletions"] = [delete_rule(name) for name in DELETIONS]
    for d in report["deletions"]:
        print(f"  [{d['status']}] {d['rule']}")

    time.sleep(2)  # let EventBridge propagate

    # 2. Creations
    print("Phase 2: creations")
    report["creations"] = [create_schedule(s) for s in CREATIONS]
    for c in report["creations"]:
        print(f"  [{c['status']}] {c['rule']}  ({c.get('cron', '-')})")

    # 3. Verify final state
    print("Phase 3: verify")
    verify = {}
    for s in CREATIONS:
        try:
            r = events.describe_rule(Name=s["rule_name"])
            verify[s["rule_name"]] = {
                "state": r.get("State"),
                "schedule": r.get("ScheduleExpression"),
            }
        except Exception as e:
            verify[s["rule_name"]] = {"err": str(e)[:120]}
    report["verify"] = verify

    # 4. Test invoke both (proves end-to-end)
    print("Phase 4: test invoke")
    invokes = {}
    for fn in ("justhodl-tax-plan", "justhodl-wealth-plan"):
        try:
            inv = lam.invoke(FunctionName=fn, InvocationType="Event")  # async fire-and-forget
            invokes[fn] = {"status": inv["StatusCode"]}
        except Exception as e:
            invokes[fn] = {"err": str(e)[:120]}
    report["test_invokes"] = invokes

    # 5. Count current rules
    try:
        rules = []
        token = None
        while True:
            kw = {"Limit": 100}
            if token:
                kw["NextToken"] = token
            r = events.list_rules(**kw)
            rules.extend(r.get("Rules", []))
            token = r.get("NextToken")
            if not token:
                break
        report["final_rule_count"] = len(rules)
        report["headroom_vs_300"] = 300 - len(rules)
    except Exception as e:
        report["rule_count_err"] = str(e)[:120]

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1095.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
