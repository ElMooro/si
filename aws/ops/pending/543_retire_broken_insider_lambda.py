#!/usr/bin/env python3
"""543 — Retire broken justhodl-insider-transactions Lambda (538 prototype).
The justhodl-insider-cluster-scanner v2 already produces real clustered buy
data ($13.3M PLSE cluster from 2 insiders). The 538 Lambda returns zeros
because its Form 4 parser doesn't extract share/price from the XML.

Steps:
  1. Disable EventBridge rule for insider-transactions
  2. Delete the Lambda function
  3. Delete the rule
  4. Delete the broken sidecar data/insider-transactions.json
  5. Verify insider-clusters.json is still fresh
  6. Confirm ai-chat + morning-intel now consume working data
"""
import json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/543_retire_broken_insider_lambda.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-insider-transactions"

    # ─── Find and disable + delete event rules ───
    rule_actions = []
    for r in eb.list_rules()["Rules"]:
        try:
            ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            for t in ts:
                if NAME in t.get("Arn", ""):
                    # Remove target
                    eb.remove_targets(Rule=r["Name"], Ids=[t["Id"]])
                    rule_actions.append({"rule": r["Name"], "action": "removed target", "id": t["Id"]})
                    # Disable rule
                    try: eb.disable_rule(Name=r["Name"])
                    except: pass
                    # Delete rule
                    try:
                        eb.delete_rule(Name=r["Name"])
                        rule_actions.append({"rule": r["Name"], "action": "deleted"})
                    except Exception as e:
                        rule_actions.append({"rule": r["Name"], "action": "delete_failed", "err": str(e)[:80]})
        except Exception as e:
            rule_actions.append({"rule": r["Name"], "err": str(e)[:80]})
    out["rule_actions"] = rule_actions

    # ─── Delete the Lambda function ───
    try:
        lam.delete_function(FunctionName=NAME)
        out["lambda_action"] = "deleted"
    except lam.exceptions.ResourceNotFoundException:
        out["lambda_action"] = "not_found_already"
    except Exception as e:
        out["lambda_action_err"] = str(e)[:200]

    # ─── Delete the broken sidecar ───
    try:
        s3.delete_object(Bucket="justhodl-dashboard-live", Key="data/insider-transactions.json")
        out["sidecar_action"] = "deleted data/insider-transactions.json"
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    # ─── Verify insider-clusters.json still fresh ───
    try:
        head = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        out["working_sidecar"] = {
            "key": "data/insider-clusters.json",
            "size_kb": round(head["ContentLength"]/1024, 1),
            "modified": head["LastModified"].isoformat()[:19],
            "age_hours": round((datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 3600, 1),
        }
        full = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        p = json.loads(full["Body"].read())
        clusters = p.get("clusters") or []
        out["working_sidecar"]["n_clusters"] = len(clusters)
        out["working_sidecar"]["total_value_usd"] = sum(c.get("total_value") or 0 for c in clusters)
        # Top 5 by value
        top5 = sorted([c for c in clusters if c.get("total_value")],
                      key=lambda x: -(x.get("total_value") or 0))[:5]
        out["working_sidecar"]["top_5"] = [
            {"ticker": c.get("ticker"), "company": c.get("company"),
             "n_insiders": c.get("n_insiders"), "total_value_usd": c.get("total_value"),
             "highest_role": c.get("highest_role"), "first_buy": c.get("first_buy")}
            for c in top5
        ]
    except Exception as e:
        out["working_sidecar_err"] = str(e)[:200]

    # ─── Confirm insider-cluster-scanner Lambda + rule still active ───
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
        out["working_lambda"] = {
            "exists": True,
            "last_modified": cfg.get("LastModified"),
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
        }
        rules = []
        for r in eb.list_rules()["Rules"]:
            try:
                ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                if any("justhodl-insider-cluster-scanner" in t.get("Arn", "") for t in ts):
                    rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                   "state": r.get("State")})
            except: pass
        out["working_lambda"]["rules"] = rules
    except Exception as e:
        out["working_lambda_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
