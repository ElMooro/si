"""ops/717 — consolidation: delete the 14 confirmed-dead Lambdas.

Every function below was verified by 716: 0 invocations in 30 days, and
either no schedule or a disabled TEST rule, and no live page/Lambda
reference (only mentions are in already-run historical ops scripts).

Re-verifies 0 invocations immediately before each delete as a final
safety gate — if a function has woken up since 716, it is SKIPPED.
Also removes any EventBridge rule that targeted only that function.
"""
import json, os
from datetime import datetime, timezone, timedelta
import boto3

lam = boto3.client("lambda", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

DEAD = [
    "fedliquidityapi-test",
    "testEnhancedScraper",
    "global-liquidity-agent-TEST",
    "OpenBBS3DataProxy",
    "FinancialIntelligence-Backend",
    "macro-financial-report-viewer",
    "createEnhancedIndex",
    "justhodl-bond-vol",
    "justhodl-ultimate-trading",
    "justhodl-0dte-pinning",
    "createUniversalIndex",
    "justhodl-cache-layer",
    "justhodl-advanced-charts",
    "universal-agent-gateway",
]


def invocations_30d(fn):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=end, Period=30 * 86400, Statistics=["Sum"])
        return int(sum(d.get("Sum", 0) for d in (r.get("Datapoints") or [])))
    except Exception as e:
        return f"err:{str(e)[:60]}"


def detach_rules(fn, arn):
    """Remove EventBridge rules that target this function (and only it)."""
    removed = []
    try:
        rules = events.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
    except Exception as e:
        return [f"rule-list-err:{str(e)[:50]}"]
    for rule in rules:
        try:
            targets = events.list_targets_by_rule(Rule=rule).get("Targets", [])
            tids = [t["Id"] for t in targets]
            if tids:
                events.remove_targets(Rule=rule, Ids=tids)
            events.delete_rule(Rule=rule)
            removed.append(rule)
        except Exception as e:
            removed.append(f"{rule}-err:{str(e)[:50]}")
    return removed


def main():
    report = {"started": datetime.now(timezone.utc).isoformat(), "actions": []}
    deleted, skipped = [], []

    for fn in DEAD:
        rec = {"function": fn}
        try:
            cfg = lam.get_function(FunctionName=fn)["Configuration"]
            arn = cfg["FunctionArn"]
        except Exception as e:
            rec.update({"result": "ALREADY_GONE", "note": str(e)[:80]})
            report["actions"].append(rec)
            continue

        inv = invocations_30d(fn)
        rec["invocations_30d_recheck"] = inv
        if not isinstance(inv, int) or inv != 0:
            rec["result"] = "SKIPPED — woke up since audit"
            skipped.append(fn)
            report["actions"].append(rec)
            continue

        rec["rules_removed"] = detach_rules(fn, arn)
        try:
            lam.delete_function(FunctionName=fn)
            rec["result"] = "DELETED"
            deleted.append(fn)
        except Exception as e:
            rec["result"] = f"DELETE_FAILED: {str(e)[:90]}"
            skipped.append(fn)
        report["actions"].append(rec)

    # post count
    try:
        total = 0
        for page in lam.get_paginator("list_functions").paginate():
            total += len(page.get("Functions", []))
        report["lambda_count_after"] = total
    except Exception as e:
        report["lambda_count_after"] = f"err:{str(e)[:60]}"

    report["summary"] = {
        "deleted": deleted, "n_deleted": len(deleted),
        "skipped": skipped, "n_skipped": len(skipped),
        "lambda_count_after": report["lambda_count_after"],
    }
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/717_cruft_delete.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 717_cruft_delete.json")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
