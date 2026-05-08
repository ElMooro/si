#!/usr/bin/env python3
"""Step 364 — Audit existing WebSocket infrastructure.
What we need to know:
  1. Does DDB table WebSocketConnections exist?
  2. Is there an API Gateway WebSocket API deployed?
  3. Are the openbb-websocket-* Lambdas actually invoked by anything?
  4. What S3 keys could feasibly drive broadcasts (the "live" data feeds)?
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/364_websocket_audit.json"
NAME = "justhodl-tmp-ws-audit"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json
import boto3
from botocore.exceptions import ClientError

ddb = boto3.client("dynamodb", region_name="us-east-1")
apigw = boto3.client("apigatewayv2", region_name="us-east-1")
lambda_client = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events_client = boto3.client("events", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Does WebSocketConnections DDB table exist?
    try:
        info = ddb.describe_table(TableName="WebSocketConnections")
        out["ddb_table"] = {
            "exists": True,
            "status": info["Table"]["TableStatus"],
            "item_count": info["Table"]["ItemCount"],
            "schema": info["Table"]["KeySchema"],
            "billing": info["Table"].get("BillingModeSummary", {}).get("BillingMode"),
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            out["ddb_table"] = {"exists": False}
        else:
            out["ddb_table"] = {"error": str(e)}

    # 2. Any API Gateway v2 WebSocket APIs?
    try:
        apis = apigw.get_apis()
        ws_apis = [a for a in apis.get("Items", []) if a.get("ProtocolType") == "WEBSOCKET"]
        out["api_gateway_websockets"] = {
            "count": len(ws_apis),
            "apis": [{
                "name": a.get("Name"),
                "api_id": a.get("ApiId"),
                "endpoint": a.get("ApiEndpoint"),
                "created": str(a.get("CreatedDate")),
            } for a in ws_apis],
        }
        # Also note all v2 API names so we don't miss anything
        out["all_apigw_v2_apis"] = [a.get("Name") for a in apis.get("Items", [])]
    except Exception as e:
        out["api_gateway_websockets"] = {"error": str(e)}

    # 3. Lambda config for the two openbb-websocket-* Lambdas
    for fn in ["openbb-websocket-handler", "openbb-websocket-broadcast"]:
        try:
            cfg = lambda_client.get_function(FunctionName=fn)
            policy = None
            try:
                policy_resp = lambda_client.get_policy(FunctionName=fn)
                policy = json.loads(policy_resp["Policy"])
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceNotFoundException":
                    policy = {"error": str(e)}
            out[f"lambda_{fn}"] = {
                "exists": True,
                "runtime": cfg["Configuration"].get("Runtime"),
                "code_size": cfg["Configuration"].get("CodeSize"),
                "last_modified": cfg["Configuration"].get("LastModified"),
                "policy_statements": [s.get("Sid") for s in (policy or {}).get("Statement", [])] if policy else None,
            }
        except ClientError as e:
            out[f"lambda_{fn}"] = {"exists": False, "error": str(e)}

    # 4. EventBridge rules referencing these Lambdas (any S3-event triggers?)
    try:
        rules = events_client.list_rules()
        relevant_rules = []
        for r in rules.get("Rules", []):
            try:
                targets = events_client.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
                for t in targets:
                    arn = t.get("Arn", "")
                    if "openbb-websocket" in arn or "ws-broadcast" in arn:
                        relevant_rules.append({
                            "rule": r["Name"], "schedule": r.get("ScheduleExpression"),
                            "target_arn": arn,
                        })
            except Exception:
                pass
        out["eventbridge_rules"] = relevant_rules
    except Exception as e:
        out["eventbridge_rules"] = {"error": str(e)}

    # 5. Sample of "live" S3 keys that could feasibly drive broadcasts
    try:
        keys_to_check = [
            "data/report.json",  # daily-report-v3, every 5 min
            "data/compound-signals.json",
            "data/master-rank.json",
            "data/cross-asset-regime.json",
            "data/macro-nowcast.json",
            "data/eurodollar-stress.json",
            "data/options-flow.json",
            "data/historical-analogs.json",
        ]
        s3_keys = {}
        for k in keys_to_check:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                s3_keys[k] = {"size": obj["ContentLength"], "modified": str(obj["LastModified"])}
            except ClientError as e:
                s3_keys[k] = {"missing": True}
        out["broadcast_candidate_keys"] = s3_keys
    except Exception as e:
        out["broadcast_candidate_keys"] = {"error": str(e)}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=60, Code={"ZipFile": zb},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
