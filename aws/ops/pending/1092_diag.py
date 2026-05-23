"""ops 1092 — diagnostic: deploy-lambdas.yml succeeded for tax-plan
   but run-ops scripts failed. Check current state:
     - Is justhodl-tax-plan Lambda deployed?
     - What's its code SHA, env, schedule, Function URL?
     - Is wealth-plan freshness manifest entry present?
"""
import json, os
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def main():
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    report = {}

    # 1. Tax plan Lambda existence
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-tax-plan")
        report["tax_plan_lambda"] = {
            "exists": True,
            "code_sha": cfg.get("CodeSha256", "")[:12],
            "last_modified": cfg.get("LastModified"),
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "state": cfg.get("State"),
            "env_keys": list((cfg.get("Environment", {}).get("Variables", {})).keys()),
        }
    except Exception as e:
        report["tax_plan_lambda"] = {"exists": False, "err": str(e)[:200]}

    # 2. Function URL
    try:
        u = lam.get_function_url_config(FunctionName="justhodl-tax-plan")
        report["tax_plan_function_url"] = u["FunctionUrl"]
    except Exception as e:
        report["tax_plan_function_url"] = None
        report["tax_plan_function_url_err"] = str(e)[:200]

    # 3. Schedule
    try:
        r = events.describe_rule(Name="tax-plan-daily")
        report["tax_plan_schedule"] = {"state": r.get("State"), "expression": r.get("ScheduleExpression")}
    except Exception:
        report["tax_plan_schedule"] = None

    # 4. Wealth plan schedule
    try:
        r = events.describe_rule(Name="wealth-plan-daily-warmup")
        report["wealth_plan_schedule"] = {"state": r.get("State"), "expression": r.get("ScheduleExpression")}
    except Exception:
        report["wealth_plan_schedule"] = None

    # 5. Manifest state
    try:
        m = json.loads(s3.get_object(Bucket=BUCKET, Key="data/_freshness-manifest.json")["Body"].read())
        ov = m.get("key_overrides", {})
        report["manifest"] = {
            "total_overrides": len(ov),
            "has_wealth_plan": "data/wealth-plan-snapshot.json" in ov,
            "has_tax_plan": "data/tax-plan-snapshot.json" in ov,
            "has_forward_returns": "data/forward-returns.json" in ov,
        }
    except Exception as e:
        report["manifest"] = {"err": str(e)[:120]}

    # 6. S3 outputs
    for k in ("data/tax-plan-snapshot.json", "data/wealth-plan-snapshot.json", "data/forward-returns.json"):
        try:
            o = s3.head_object(Bucket=BUCKET, Key=k)
            report[k] = {"size_kb": round(o["ContentLength"] / 1024, 1), "last_modified": o["LastModified"].isoformat()}
        except Exception as e:
            report[k] = {"err": str(e)[:80]}

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1092.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
