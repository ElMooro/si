#!/usr/bin/env python3
"""Halt every JustHodl engine and enforce the $500 monthly cap.

ops 6525 already froze the fleet, but control.json still overrides October
to $3,500. This drops that override so AWS Budget JustHodl-Monthly-Hard-Cap-500
is the cap the governor enforces, then re-applies FREEZE (including the
SageMaker lever that failed last night).

Left running on purpose: the spend governor itself, the alert function,
the Supabase keepalive (login dies if that pauses), and the DR snapshot.
The website is not an engine and is not taken down. Storage, DNS and site
traffic keep a residual bill; AWS cannot claw back what is already spent.
"""
import json
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ACCOUNT = "857687956942"
BUDGET = "JustHodl-Monthly-Hard-Cap-500"
BUCKET = "justhodl-ops-private-857687956942"
CONTROL_KEY = "spend-governor/control.json"
FN = "justhodl-spend-governor"
EXEMPT = {
    "justhodl-spend-governor",
    "justhodl-guardrail-notify",
    "justhodl-supabase-keepalive",
    "justhodl-dr-snapshot",
}

CFG = Config(read_timeout=920, connect_timeout=10, retries={"max_attempts": 5, "mode": "standard"})
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
budgets = boto3.client("budgets", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
cw = boto3.client("cloudwatch", region_name="us-east-1", config=CFG)


def invoke(payload):
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
    raw = resp["Payload"].read() or b"{}"
    body = json.loads(raw)
    if resp.get("FunctionError"):
        raise SystemExit("invoke %s failed: %s" % (payload.get("action"), str(body)[:800]))
    return body


def hot_functions(minutes):
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(minutes=minutes)
    found, token = {}, None
    while True:
        kw = {
            "MetricDataQueries": [{
                "Id": "inv",
                "Expression": "SEARCH('{AWS/Lambda,FunctionName} MetricName=\"Invocations\"', 'Sum', 60)",
                "ReturnData": True,
            }],
            "StartTime": start,
            "EndTime": end,
        }
        if token:
            kw["NextToken"] = token
        page = cw.get_metric_data(**kw)
        for row in page.get("MetricDataResults") or []:
            total = sum(v for v in (row.get("Values") or []) if v)
            if total > 0:
                found[row.get("Label") or "?"] = round(total, 1)
        token = page.get("NextToken")
        if not token:
            return found


def main():
    described = budgets.describe_budget(AccountId=ACCOUNT, BudgetName=BUDGET)["Budget"]
    limit = float(described["BudgetLimit"]["Amount"])
    actual = float(((described.get("CalculatedSpend") or {}).get("ActualSpend") or {}).get("Amount") or 0)
    print("budget_before", json.dumps({"limit": limit, "actual": round(actual, 2)}))
    if abs(limit - 500.0) > 0.01:
        updated = {k: v for k, v in described.items() if k != "CalculatedSpend"}
        updated["BudgetLimit"] = {"Amount": "500", "Unit": "USD"}
        budgets.update_budget(AccountId=ACCOUNT, NewBudget=updated)
        print("budget_set", 500)

    control = json.loads(s3.get_object(Bucket=BUCKET, Key=CONTROL_KEY)["Body"].read())
    overrides = dict(control.get("cap_overrides") or {})
    removed = overrides.pop("2026-10", None)
    control["cap_overrides"] = overrides
    control["mode"] = "enforce"
    s3.put_object(
        Bucket=BUCKET, Key=CONTROL_KEY,
        Body=json.dumps(control, indent=1, sort_keys=True).encode(),
        ContentType="application/json", ServerSideEncryption="AES256",
    )
    print("override_removed", json.dumps(removed))

    before = invoke({"action": "status"})
    print("status_before", json.dumps({
        "stage": before.get("stage"), "cap": before.get("cap"), "cap_source": before.get("cap_source"),
        "held": before.get("held"), "budget_actual": before.get("budget_actual"),
        "mode": before.get("mode"),
    }))

    forced = invoke({
        "action": "force", "stage": "FREEZE", "confirm": "FREEZE",
        "reason": "Khalid: halt every engine and enforce the $500 hard cap",
    })
    print("forced", json.dumps({"stage": forced.get("stage"), "applied": forced.get("applied")})[:2000])

    after = invoke({"action": "status"})
    print("status_after", json.dumps({
        "stage": after.get("stage"), "cap": after.get("cap"), "cap_source": after.get("cap_source"),
        "budget_limit": after.get("budget_limit"), "budget_actual": after.get("budget_actual"),
        "budget_forecast": after.get("budget_forecast"), "held": after.get("held"),
        "thresholds": after.get("thresholds"), "stage_since": after.get("stage_since"),
        "mode": after.get("mode"),
    }))

    try:
        hot = hot_functions(20)
    except ClientError as exc:
        hot = None
        print("invocation_check_error", exc.response.get("Error", {}).get("Code"))
    if hot is not None:
        stray = {name: n for name, n in hot.items() if not any(x in name for x in EXEMPT)}
        print("invocations_20m", json.dumps(hot)[:2000])
        print("non_exempt_invocations", json.dumps(stray)[:2000])
    else:
        stray = {}

    problems = []
    if after.get("stage") != "FREEZE":
        problems.append("stage %s" % after.get("stage"))
    if abs(float(after.get("cap") or 0) - 500.0) > 0.01:
        problems.append("cap %s" % after.get("cap"))
    if not str(after.get("cap_source") or "").startswith("budget"):
        problems.append("cap_source %s" % after.get("cap_source"))
    if after.get("mode") != "enforce":
        problems.append("mode %s" % after.get("mode"))
    held = (after.get("held") or {}).get("FREEZE/lambda") or 0
    if held < 800:
        problems.append("only %s lambdas held" % held)
    if stray:
        problems.append("still invoking: %s" % ",".join(sorted(stray)[:8]))
    if problems:
        raise SystemExit("halt incomplete: " + "; ".join(problems))
    print("OK fleet frozen under the $500 budget")


if __name__ == "__main__":
    main()
