#!/usr/bin/env python3
"""Confirm the fleet is FROZEN and the live cap is the $500 budget.

Does not restore anything. Drops a leftover October $3,500 override if the
previous run died before that write. Does not re-scan every Lambda metric.
"""
import json

import boto3
from botocore.config import Config

ACCOUNT = "857687956942"
BUDGET = "JustHodl-Monthly-Hard-Cap-500"
BUCKET = "justhodl-ops-private-857687956942"
KEY = "spend-governor/control.json"
FN = "justhodl-spend-governor"

CFG = Config(read_timeout=180, connect_timeout=10, retries={"max_attempts": 2, "mode": "standard"})
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
budgets = boto3.client("budgets", region_name="us-east-1", config=CFG)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)


def main():
    described = budgets.describe_budget(AccountId=ACCOUNT, BudgetName=BUDGET)["Budget"]
    limit = float(described["BudgetLimit"]["Amount"])
    actual = float(((described.get("CalculatedSpend") or {}).get("ActualSpend") or {}).get("Amount") or 0)
    forecast = float(((described.get("CalculatedSpend") or {}).get("ForecastedSpend") or {}).get("Amount") or 0)
    print("budget", json.dumps({"limit": limit, "actual": round(actual, 2), "forecast": round(forecast, 2)}))
    if abs(limit - 500.0) > 0.01:
        raise SystemExit("budget limit is %s, not 500" % limit)

    control = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    overrides = dict(control.get("cap_overrides") or {})
    if "2026-10" in overrides:
        removed = overrides.pop("2026-10")
        control["cap_overrides"] = overrides
        control["mode"] = "enforce"
        s3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps(control, indent=1, sort_keys=True).encode(),
                      ContentType="application/json", ServerSideEncryption="AES256")
        print("override_removed", json.dumps(removed))
    else:
        print("override_absent", json.dumps(overrides))
    print("mode", control.get("mode"))

    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b'{"action":"status"}')
    body = json.loads(resp["Payload"].read() or b"{}")
    if resp.get("FunctionError"):
        raise SystemExit("status invoke failed: %s" % str(body)[:500])
    keep = {k: body.get(k) for k in (
        "version", "mode", "stage", "stage_since", "cap", "cap_source", "budget_limit",
        "budget_actual", "budget_forecast", "held", "thresholds", "suppress_until")}
    print("status", json.dumps(keep))
    problems = []
    if body.get("stage") != "FREEZE":
        problems.append("stage %s" % body.get("stage"))
    if abs(float(body.get("cap") or 0) - 500) > 0.01:
        problems.append("cap %s source %s" % (body.get("cap"), body.get("cap_source")))
    if not str(body.get("cap_source") or "").startswith("budget"):
        problems.append("cap_source %s" % body.get("cap_source"))
    held = (body.get("held") or {}).get("FREEZE/lambda") or 0
    if held < 800:
        problems.append("held lambdas %s" % held)
    if problems:
        raise SystemExit("not halted: " + "; ".join(problems))
    print("OK frozen at the $500 budget")


if __name__ == "__main__":
    main()
