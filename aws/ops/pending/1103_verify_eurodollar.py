"""ops 1103 — verify eurodollar-stress engine is alive + producing data
   (the page depends on data/eurodollar-stress.json)."""
import json, os
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def main():
    report = {"generated_at": datetime.now(timezone.utc).isoformat()}

    # 1. Data file
    try:
        o = s3.get_object(Bucket=BUCKET, Key="data/eurodollar-stress.json")
        d = json.loads(o["Body"].read())
        age_h = (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600
        report["data"] = {
            "exists": True,
            "size_kb": round(o["ContentLength"] / 1024, 1),
            "last_modified": o["LastModified"].isoformat(),
            "age_hours": round(age_h, 1),
            "composite_score": d.get("composite_score"),
            "severity": d.get("severity"),
            "regime": d.get("regime"),
            "n_signals_used": d.get("n_signals_used"),
            "n_signals_total": d.get("n_signals_total"),
            "signal_labels": [s.get("label") for s in d.get("signals", [])],
        }
    except Exception as e:
        report["data"] = {"exists": False, "err": str(e)[:200]}

    # 2. Lambda
    try:
        c = lam.get_function_configuration(FunctionName="justhodl-eurodollar-stress")
        report["lambda"] = {"exists": True, "state": c.get("State"), "last_modified": c.get("LastModified")}
    except Exception as e:
        report["lambda"] = {"exists": False, "err": str(e)[:150]}

    # 3. Schedule
    try:
        rules = events.list_rule_names_by_target(TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:justhodl-eurodollar-stress")
        report["schedules"] = rules.get("RuleNames", [])
    except Exception as e:
        report["schedules_err"] = str(e)[:120]

    # 4. If data missing/stale, invoke now
    age = report.get("data", {}).get("age_hours", 999)
    if not report.get("data", {}).get("exists") or age > 3:
        try:
            inv = lam.invoke(FunctionName="justhodl-eurodollar-stress", InvocationType="RequestResponse")
            report["forced_invoke"] = inv["StatusCode"]
            import time
            time.sleep(3)
            o = s3.get_object(Bucket=BUCKET, Key="data/eurodollar-stress.json")
            d = json.loads(o["Body"].read())
            report["post_invoke"] = {
                "composite_score": d.get("composite_score"),
                "severity": d.get("severity"),
                "n_signals_used": d.get("n_signals_used"),
            }
        except Exception as e:
            report["forced_invoke_err"] = str(e)[:200]

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1103.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
