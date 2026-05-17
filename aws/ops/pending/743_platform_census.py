"""ops/743 — platform census (gap #4).

Lists every Lambda in the account and pulls 7-day CloudWatch Invocations
+ Errors for each, so orphaned (never-invoked) and silently-erroring
functions surface instead of hiding in a 268-function fleet.
"""
import json, os
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

cfg = Config(retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
cw = boto3.client("cloudwatch", region_name="us-east-1", config=cfg)

report = {"ops": 743, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "platform census — dormant + erroring Lambdas"}

now = datetime.now(timezone.utc)
start = now - timedelta(days=7)


def metric_sum(fn, metric):
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName=metric,
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start, EndTime=now, Period=604800, Statistics=["Sum"])
        dp = r.get("Datapoints") or []
        return int(sum(d["Sum"] for d in dp))
    except Exception:
        return None


# enumerate all functions
funcs = []
paginator = lam.get_paginator("list_functions")
for page in paginator.paginate():
    for f in page.get("Functions", []):
        funcs.append({"name": f["FunctionName"],
                      "last_modified": f.get("LastModified"),
                      "runtime": f.get("Runtime")})

rows = []
for f in funcs:
    inv = metric_sum(f["name"], "Invocations")
    err = metric_sum(f["name"], "Errors")
    rows.append({"name": f["name"], "runtime": f["runtime"],
                 "last_modified": f["last_modified"],
                 "invocations_7d": inv, "errors_7d": err})

dormant = [r for r in rows if r["invocations_7d"] == 0]
erroring = [r for r in rows if (r["errors_7d"] or 0) > 0]
# error-heavy = errors are a large share of invocations
error_heavy = [r for r in erroring
               if r["invocations_7d"] and
               (r["errors_7d"] / r["invocations_7d"]) >= 0.25]

report["fleet_size"] = len(rows)
report["counts"] = {
    "dormant_0_invocations_7d": len(dormant),
    "any_errors_7d": len(erroring),
    "error_heavy_25pct_plus": len(error_heavy),
}
report["dormant"] = sorted([r["name"] for r in dormant])
report["error_heavy"] = sorted(
    [{"name": r["name"], "invocations_7d": r["invocations_7d"],
      "errors_7d": r["errors_7d"]} for r in error_heavy],
    key=lambda x: -x["errors_7d"])
report["all_erroring"] = sorted(
    [{"name": r["name"], "invocations_7d": r["invocations_7d"],
      "errors_7d": r["errors_7d"]} for r in erroring],
    key=lambda x: -(x["errors_7d"] or 0))
report["verdict"] = (
    f"CENSUS COMPLETE — {len(rows)} Lambdas: {len(dormant)} dormant "
    f"(0 invocations/7d, candidates for prune or broken schedule), "
    f"{len(erroring)} with errors, {len(error_heavy)} error-heavy")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/743_platform_census.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/743_platform_census.json")
