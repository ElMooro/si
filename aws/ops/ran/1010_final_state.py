#!/usr/bin/env python3
"""Step 1010 — FINAL: re-invoke alpha-compass so it pulls in fresh magdist."""
import json, os, time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1010_final_state.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def invoke(name):
    r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    out = {"fn_err": r.get("FunctionError")}
    try:
        p = json.loads(body)
        out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        out["raw"] = body[:600]
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    out["compass_invoke"] = invoke("justhodl-alpha-compass")
    time.sleep(2)
    
    # Final state of all 3 + their S3 outputs
    out["s3"] = {}
    for k in ("data/magnitude-distributions.json",
              "data/alpha-compass.json",
              "data/miss-summary.json"):
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=k)
            out["s3"][k] = {"size": obj["ContentLength"],
                            "modified": str(obj["LastModified"])}
        except Exception as e:
            out["s3"][k] = {"missing": str(e)[:80]}
    
    # Read full alpha-compass payload
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/alpha-compass.json")
        d = json.loads(obj["Body"].read().decode())
        out["compass_full"] = d
    except Exception as e:
        out["compass_err"] = str(e)[:200]
    
    # Read top-line magdist
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/magnitude-distributions.json")
        d = json.loads(obj["Body"].read().decode())
        out["magdist_totals"] = d.get("totals")
        out["magdist_stack_count"] = len(d.get("stacks", []))
    except Exception as e:
        out["magdist_err"] = str(e)[:200]
    
    # Lambda schedules summary
    events = boto3.client("events", region_name=REGION)
    out["schedules"] = {}
    for fn in ("justhodl-magnitude-distributions",
               "justhodl-miss-detector",
               "justhodl-alpha-compass"):
        try:
            arn = f"arn:aws:lambda:us-east-1:857687956942:function:{fn}"
            rules = events.list_rule_names_by_target(TargetArn=arn).get("RuleNames", [])
            for rname in rules:
                rule = events.describe_rule(Name=rname)
                out["schedules"][fn] = {
                    "rule": rname,
                    "expression": rule.get("ScheduleExpression"),
                    "state": rule.get("State"),
                }
                break
        except Exception as e:
            out["schedules"][fn] = {"err": str(e)[:120]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
