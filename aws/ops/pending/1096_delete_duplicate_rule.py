"""ops 1096 — delete duplicate EventBridge rule fleet-error-monitor-5min.

Audit (ops 1094) found two rules targeting the same Lambda
justhodl-fleet-error-monitor at 5-min cadence:

  1. fleet-error-monitor-5min              (unprefixed — likely migration orphan)
  2. justhodl-fleet-error-monitor-5min     (prefixed — platform convention)

Both fire every 5 min → 2× wasted Lambda invocations + 2× slots used.
Delete the unprefixed one (does not follow naming convention).

Net delta: 299 → 298 rules (2 slot headroom vs 300 cap).
"""
import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
events = boto3.client("events", region_name=REGION)

DUPLICATE_TO_DELETE = "fleet-error-monitor-5min"
KEEPER = "justhodl-fleet-error-monitor-5min"


def describe(rule_name):
    try:
        r = events.describe_rule(Name=rule_name)
        t = events.list_targets_by_rule(Rule=rule_name)
        return {
            "name": rule_name,
            "state": r.get("State"),
            "schedule": r.get("ScheduleExpression"),
            "description": (r.get("Description") or "")[:120],
            "targets": [tt["Arn"] for tt in t.get("Targets", [])],
            "exists": True,
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return {"name": rule_name, "exists": False}
        return {"name": rule_name, "err": str(e)[:200]}


def delete_rule(rule_name):
    out = {"rule": rule_name}
    try:
        t = events.list_targets_by_rule(Rule=rule_name)
        target_ids = [tt["Id"] for tt in t.get("Targets", [])]
        if target_ids:
            events.remove_targets(Rule=rule_name, Ids=target_ids)
            out["targets_removed"] = len(target_ids)
        events.delete_rule(Name=rule_name)
        out["status"] = "DELETED"
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            out["status"] = "ALREADY_GONE"
        else:
            out["status"] = "ERR"
            out["error"] = str(e)[:200]
    return out


def list_rule_count():
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
    return len(rules)


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # 1. Pre-check: confirm both exist + identical target before deleting
    print("Phase 1: pre-check")
    dup = describe(DUPLICATE_TO_DELETE)
    keep = describe(KEEPER)
    report["pre_check"] = {"duplicate": dup, "keeper": keep}

    if not dup.get("exists"):
        report["verdict"] = "ALREADY_DONE"
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        return _save(report)

    if not keep.get("exists"):
        report["verdict"] = "ABORT_KEEPER_MISSING"
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        return _save(report)

    # Safety: ensure keeper's target is the same Lambda
    dup_targets = set(dup.get("targets", []))
    keep_targets = set(keep.get("targets", []))
    if not (dup_targets & keep_targets):
        report["verdict"] = "ABORT_TARGETS_DIFFER"
        report["safety_note"] = f"Duplicate targets {dup_targets} differ from keeper {keep_targets}; refusing to delete."
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        return _save(report)

    # 2. Pre-count
    pre_count = list_rule_count()
    report["pre_rule_count"] = pre_count

    # 3. Delete the duplicate
    print("Phase 2: delete")
    report["delete"] = delete_rule(DUPLICATE_TO_DELETE)

    time.sleep(2)

    # 4. Post-verify keeper still works
    print("Phase 3: post-verify")
    report["post_check_keeper"] = describe(KEEPER)
    report["post_check_duplicate"] = describe(DUPLICATE_TO_DELETE)
    post_count = list_rule_count()
    report["post_rule_count"] = post_count
    report["delta"] = pre_count - post_count
    report["headroom_vs_300"] = 300 - post_count

    report["verdict"] = "SUCCESS" if (
        report["delete"].get("status") == "DELETED"
        and not report["post_check_duplicate"].get("exists")
        and report["post_check_keeper"].get("exists")
    ) else "PARTIAL"

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    return _save(report)


def _save(report):
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1096.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
