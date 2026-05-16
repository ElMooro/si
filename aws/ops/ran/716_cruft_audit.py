"""ops/716 — consolidation audit. For the suspected-cruft Lambdas AND a
full zero-invocation sweep, gather the facts needed to safely retire dead
functions: last-modified, EventBridge schedule, 14d/30d invocation counts.
Verdict is advisory — deletion is a separate, gated step (717)."""
import json, os
from datetime import datetime, timezone, timedelta
import boto3

lam = boto3.client("lambda", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

# explicit cruft candidates flagged in the system audit
CANDIDATES = [
    "justhodl-position-sizer-v2", "justhodl-risk-sizer", "justhodl-portfolio-sizer",
    "justhodl-backtest-harness", "justhodl-calls-backtest", "justhodl-backtest-engine",
    "global-liquidity-agent-v2", "global-liquidity-agent-TEST", "justhodl-global-liquidity",
    "fedliquidityapi-test", "fedliquidityapi",
    "justhodl-bloomberg-v8", "justhodl-ab-test", "testEnhancedScraper",
    "bls-employment-api-v2", "justhodl-divergence-engine-v2", "justhodl-email-reports-v2",
]


def invocations(fn, days):
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=end, Period=days * 86400, Statistics=["Sum"])
        dp = r.get("Datapoints") or []
        return int(sum(d.get("Sum", 0) for d in dp))
    except Exception as e:
        return f"err:{str(e)[:60]}"


def schedule_for(fn):
    try:
        arn = lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
        rules = events.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
        return rules
    except Exception:
        return []


def detail(fn):
    try:
        cfg = lam.get_function(FunctionName=fn)["Configuration"]
    except Exception as e:
        return {"function": fn, "exists": False, "note": str(e)[:80]}
    lm = cfg.get("LastModified", "")
    try:
        lm_dt = datetime.fromisoformat(lm.replace("Z", "+00:00").split("+")[0] + "+00:00")
        age_days = (datetime.now(timezone.utc) - lm_dt).days
    except Exception:
        age_days = None
    inv30 = invocations(fn, 30)
    inv14 = invocations(fn, 14)
    rules = schedule_for(fn)
    is_test = any(t in fn.lower() for t in ("test", "-ab-", "harness"))
    if isinstance(inv30, int) and inv30 == 0 and not rules:
        verdict = "SAFE_TO_DELETE"
    elif isinstance(inv30, int) and inv30 == 0 and rules:
        verdict = "REVIEW (scheduled but idle)"
    elif isinstance(inv30, int) and inv30 > 0:
        verdict = "KEEP (active)"
    else:
        verdict = "REVIEW"
    return {"function": fn, "exists": True, "last_modified": lm,
            "days_since_modified": age_days, "schedule_rules": rules,
            "invocations_14d": inv14, "invocations_30d": inv30,
            "name_looks_test": is_test, "verdict": verdict}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    report["candidates"] = [detail(fn) for fn in CANDIDATES]

    # full sweep — find any OTHER zero-invocation functions
    all_fns = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            all_fns.append(f["FunctionName"])
    report["total_lambdas"] = len(all_fns)

    cand_set = set(CANDIDATES)
    dead_other = []
    for fn in all_fns:
        if fn in cand_set:
            continue
        inv = invocations(fn, 30)
        if isinstance(inv, int) and inv == 0:
            rules = schedule_for(fn)
            dead_other.append({"function": fn, "invocations_30d": 0,
                               "schedule_rules": rules,
                               "verdict": "SAFE_TO_DELETE" if not rules
                                          else "REVIEW (scheduled but idle)"})
    report["other_zero_invocation"] = sorted(dead_other, key=lambda x: x["function"])

    cands = report["candidates"]
    report["summary"] = {
        "total_lambdas": len(all_fns),
        "candidates_safe_to_delete": [c["function"] for c in cands
                                       if c.get("verdict") == "SAFE_TO_DELETE"],
        "candidates_review": [c["function"] for c in cands
                               if c.get("verdict", "").startswith("REVIEW")],
        "candidates_keep": [c["function"] for c in cands
                             if c.get("verdict", "").startswith("KEEP")],
        "other_zero_inv_count": len(dead_other),
        "other_safe_to_delete": [d["function"] for d in dead_other
                                  if d["verdict"] == "SAFE_TO_DELETE"],
    }
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/716_cruft_audit.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 716_cruft_audit.json")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
