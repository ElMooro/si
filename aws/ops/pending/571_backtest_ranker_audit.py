#!/usr/bin/env python3
"""571 — Audit existing backtest-harness + master-ranker output. Find what's
working, what's stale, what's missing."""
import io, json, os
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/571_backtest_ranker_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Schedule check
    rules = {}
    for prefix in ["justhodl-backtest", "justhodl-master-ranker", "justhodl-alpha"]:
        try:
            resp = events.list_rules(NamePrefix=prefix)
            for r in resp.get("Rules", []):
                targets = events.list_targets_by_rule(Rule=r["Name"])
                rules[r["Name"]] = {
                    "schedule": r.get("ScheduleExpression"),
                    "state": r.get("State"),
                    "targets": [t.get("Arn", "").split(":")[-1] for t in targets.get("Targets", [])],
                }
        except Exception: pass
    out["eventbridge_rules"] = rules

    # Read backtest/results.json + summary.json
    for k in ["backtest/results.json", "backtest/summary.json",
               "backtest/calls-results.json", "data/master-ranker.json",
               "data/daily-ranker.json", "ranker/today.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            body = obj["Body"].read()
            p = json.loads(body)
            out.setdefault("sidecars", {})[k] = {
                "size_kb": round(len(body)/1024, 1),
                "modified": obj["LastModified"].isoformat()[:19],
                "top_keys": list(p.keys())[:20] if isinstance(p, dict) else f"list len={len(p)}",
                "sample": (
                    {k2: (str(v)[:200] if not isinstance(v, (list, dict))
                          else (f"list({len(v)})" if isinstance(v, list)
                                  else f"dict({len(v)})"))
                     for k2, v in list(p.items())[:8]}
                    if isinstance(p, dict) else None
                ),
            }
        except Exception as e:
            out.setdefault("sidecars", {})[k] = f"err: {str(e)[:60]}"

    # CloudWatch invocations for the 3 backtest/ranker Lambdas
    for name in ["justhodl-backtest-harness", "justhodl-backtest-engine",
                 "justhodl-master-ranker", "justhodl-alpha-calibrator"]:
        try:
            end = datetime.now(timezone.utc)
            start = end - timedelta(days=2)
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": name}],
                StartTime=start, EndTime=end, Period=3600,
                Statistics=["Sum"],
            )
            points = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
            total = sum(int(p.get("Sum", 0)) for p in points)
            out.setdefault("invocations_48h", {})[name] = {
                "total": total,
                "active_hours": [(p["Timestamp"].isoformat()[:13], int(p.get("Sum",0)))
                                  for p in points if p.get("Sum", 0) > 0][:8],
            }
        except Exception as e:
            out.setdefault("invocations_48h", {})[name] = f"err: {e}"

    # Read full backtest summary if exists (the key value file)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/summary.json")
        full = json.loads(obj["Body"].read())
        out["backtest_summary_full"] = full
    except Exception as e:
        out["backtest_summary_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
