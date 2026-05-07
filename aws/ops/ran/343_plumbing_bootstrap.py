#!/usr/bin/env python3
"""Step 343 — Bootstrap justhodl-plumbing-aggregator + verify."""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"
NAME = "justhodl-plumbing-aggregator"
RULE_NAME = "plumbing-agg-hourly"
SCHEDULE = "rate(1 hour)"
REPORT = "aws/ops/reports/343_plumbing_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip(lambda_name):
    src_dir = f"aws/lambdas/{lambda_name}/source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _d, files in os.walk(src_dir):
            for fn in files:
                if fn.endswith(".pyc") or "__pycache__" in root:
                    continue
                fp = os.path.join(root, fn)
                zf.write(fp, os.path.relpath(fp, src_dir))
    return buf.getvalue()


def deploy():
    zb = build_zip(NAME)
    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "S3_KEY_OUT": "data/plumbing-stress.json",
        "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
    }
    desc = "4-layer liquidity & risk plumbing composite — eurodollar/bank/real/cross-border."
    try:
        lam.get_function(FunctionName=NAME)
        action = "update"
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        lam.update_function_configuration(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            MemorySize=512, Timeout=240,
            Environment={"Variables": env}, Role=ROLE_ARN, Description=desc,
        )
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        action = "create"
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=512, Timeout=240, Code={"ZipFile": zb},
            Environment={"Variables": env}, Description=desc,
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
        try:
            lam.put_function_concurrency(FunctionName=NAME, ReservedConcurrentExecutions=1)
        except Exception:
            pass
    return {"action": action, "zip_kb": round(len(zb)/1024, 1)}


def schedule():
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE,
                    State="ENABLED", Description=f"Schedule for {NAME}")
    target_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{NAME}"
    events.put_targets(Rule=RULE_NAME,
        Targets=[{"Id": "1", "Arn": target_arn, "Input": "{}"}])
    try:
        lam.add_permission(
            FunctionName=NAME, StatementId=f"{RULE_NAME}-invoke",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise


def smoke():
    started = time.time()
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                      Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    out = {"status": resp.get("StatusCode"), "fn_err": resp.get("FunctionError"),
           "duration_s": round(time.time() - started, 1)}
    try:
        out["body"] = json.loads(body)
    except Exception:
        out["body_raw"] = body[:300]
    return out


def fetch_and_summarize_output():
    """Pull data/plumbing-stress.json + return summary."""
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/plumbing-stress.json")
        d = json.loads(obj["Body"].read())
    except Exception as e:
        return {"err": str(e)}
    raw = d.get("raw_indicators", {})
    n_total = len(raw)
    n_with_data = sum(1 for v in raw.values() if v.get("stress_score") is not None)
    by_layer = {}
    for v in raw.values():
        by_layer.setdefault(v.get("layer"), {"total": 0, "with_data": 0})
        by_layer[v["layer"]]["total"] += 1
        if v.get("stress_score") is not None:
            by_layer[v["layer"]]["with_data"] += 1
    # Find indicators with errors (likely OFR / ECB cache misses)
    errors = []
    for sid, v in raw.items():
        if v.get("err"):
            errors.append({"id": sid, "label": v.get("label"), "err": v.get("err")})
    return {
        "schema": d.get("schema_version"),
        "method": d.get("method"),
        "as_of": d.get("as_of"),
        "composite_score": d.get("composite_score"),
        "composite_label": d.get("composite_label"),
        "n_total": n_total,
        "n_with_data": n_with_data,
        "by_layer": by_layer,
        "n_alerts": len(d.get("alerts", [])),
        "alerts_top3": d.get("alerts", [])[:3],
        "duration_s": d.get("duration_s"),
        "errors": errors,
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("\n══ DEPLOY ══")
    out["deploy"] = deploy()
    time.sleep(2)
    print(f"  {out['deploy']}")

    print("\n══ SCHEDULE ══")
    schedule()
    out["schedule"] = {"rule": RULE_NAME, "expr": SCHEDULE}

    time.sleep(2)
    print("\n══ SMOKE TEST (sync invoke) ══")
    out["smoke"] = smoke()
    print(f"  status={out['smoke'].get('status')} dur={out['smoke'].get('duration_s')}s")

    time.sleep(2)
    print("\n══ OUTPUT SUMMARY ══")
    out["output"] = fetch_and_summarize_output()
    print(json.dumps(out["output"], indent=2, default=str)[:3000])

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
