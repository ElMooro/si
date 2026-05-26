"""ops 1097 — smoke test: push → run-ops workflow → AWS access → report writeback.
Pure read-only — proves the full deploy loop is healthy after the EventBridge fix.
"""
import os, json
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def main():
    events = boto3.client("events", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name="us-east-1")

    # Quick AWS reachability test
    rules = events.list_rules(Limit=5).get("Rules", [])
    funcs = lam.list_functions(MaxItems=5).get("Functions", [])
    s3_test = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/forward-returns.json")

    # Count current rule headroom
    all_rules = []
    token = None
    while True:
        kw = {"Limit": 100}
        if token:
            kw["NextToken"] = token
        r = events.list_rules(**kw)
        all_rules.extend(r.get("Rules", []))
        token = r.get("NextToken")
        if not token:
            break

    report = {
        "test": "push_workflow_aws_loop",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "github_push_received": True,
        "workflow_executed": True,
        "aws_lambda_reachable": True,
        "aws_eventbridge_reachable": True,
        "aws_s3_reachable": True,
        "current_eb_rule_count": len(all_rules),
        "eb_headroom_vs_300": 300 - len(all_rules),
        "compass_s3_last_modified": s3_test["LastModified"].isoformat(),
        "sample_lambda_count": len(funcs),
        "verdict": "ALL_GREEN" if len(all_rules) < 300 else "AT_CAP",
    }

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1097.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
