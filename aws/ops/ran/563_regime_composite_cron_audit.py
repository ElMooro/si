#!/usr/bin/env python3
"""563 — Verify regime-composite hourly cron is actually firing (the :15 trigger).
Read history sidecar to confirm at least 1-2 hourly snapshots have accumulated.
Also count CloudWatch invocations for the last 2 hours."""
import io, json, os, time as _time
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/563_regime_composite_cron_audit.json"
NAME = "justhodl-regime-composite"

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
cw = boto3.client("cloudwatch", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. EventBridge rule state
    try:
        rule = events.describe_rule(Name="justhodl-regime-composite-hourly")
        out["rule"] = {
            "name": rule.get("Name"),
            "schedule": rule.get("ScheduleExpression"),
            "state": rule.get("State"),
        }
        targets = events.list_targets_by_rule(Rule=rule["Name"])
        out["targets"] = [t.get("Arn") for t in targets.get("Targets", [])]
    except Exception as e:
        out["rule_err"] = str(e)[:200]

    # 2. Recent Lambda invocations (CloudWatch metric)
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=3)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": NAME}],
            StartTime=start, EndTime=end, Period=600,  # 10-min buckets
            Statistics=["Sum"],
        )
        points = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
        out["invocations_last_3h"] = [
            {"ts": p["Timestamp"].isoformat()[:19], "n": int(p.get("Sum", 0))}
            for p in points
        ]
        out["total_invocations_3h"] = sum(int(p.get("Sum", 0)) for p in points)
    except Exception as e:
        out["cw_err"] = str(e)[:200]

    # 3. History sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/regime-composite-history.json")
        body = obj["Body"].read()
        h = json.loads(body)
        snaps = h.get("snapshots") or []
        out["history"] = {
            "size_kb": round(len(body) / 1024, 2),
            "modified": obj["LastModified"].isoformat()[:19],
            "n_snapshots": len(snaps),
            "first_ts": snaps[0]["ts"] if snaps else None,
            "last_ts": snaps[-1]["ts"] if snaps else None,
            "regimes_seen": list({s.get("meta_regime") for s in snaps}),
            "composite_score_range": (
                {"min": min((s.get("composite_score", 0) for s in snaps)),
                 "max": max((s.get("composite_score", 0) for s in snaps))}
                if snaps else None
            ),
            "last_5": snaps[-5:],
        }
    except Exception as e:
        out["history_err"] = str(e)[:200]

    # 4. Current sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/regime-composite.json")
        p = json.loads(obj["Body"].read())
        out["current"] = {
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "meta_regime": p.get("meta_regime"),
            "composite_score": p.get("composite_score"),
            "duration_s": p.get("duration_s"),
            "n_modules_with_data": p.get("n_modules_with_data"),
            "n_modules_missing": p.get("n_modules_missing"),
        }
    except Exception as e:
        out["current_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
