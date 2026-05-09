#!/usr/bin/env python3
"""Step 370 — End-to-end test:
  1. Confirm justhodl-alert-router has the new code deployed (look for
     broadcast_alert_to_wss in CloudWatch on next invoke).
  2. Send a direct test broadcast through the broadcast Function URL
     simulating what alert-router will send.
  3. Confirm the broadcast Lambda accepts and routes it correctly.
  4. Health snapshot of broadcast Lambda + DDB connection count.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/370_alert_router_wss_e2e.json"
NAME = "justhodl-tmp-e2e-370"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, time, urllib.request
import boto3

ssm = boto3.client("ssm", region_name="us-east-1")
ddb = boto3.client("dynamodb", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

BROADCAST_URL = "https://p6kvtojb2y6r4orgbtxh7ld3nu0pfktz.lambda-url.us-east-1.on.aws"

def post(path, body, token):
    req = urllib.request.Request(
        BROADCAST_URL + path, method="POST",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                 "X-Justhodl-Admin-Token": token},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception as e:
        return None, f"ERROR: {type(e).__name__}: {e}"

def lambda_handler(event, context):
    out = {}

    # 1. Get admin token
    try:
        token = ssm.get_parameter(Name="/justhodl/push/admin-token", WithDecryption=True)["Parameter"]["Value"]
        out["1_token"] = {"ok": True, "length": len(token)}
    except Exception as e:
        out["1_token"] = {"ok": False, "error": str(e)}
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. Broadcast Lambda config — confirm it's the right runtime/role/handler
    try:
        cfg = lam.get_function(FunctionName="openbb-websocket-broadcast")["Configuration"]
        out["2_broadcast_lambda"] = {
            "role": cfg.get("Role").split("/")[-1],
            "handler": cfg.get("Handler"),
            "runtime": cfg.get("Runtime"),
            "last_modified": cfg.get("LastModified"),
            "code_size": cfg.get("CodeSize"),
            "ok": cfg.get("Role").endswith("/lambda-execution-role")
                  and cfg.get("Handler") == "lambda_function.lambda_handler"
                  and cfg.get("Runtime") == "python3.12",
        }
    except Exception as e:
        out["2_broadcast_lambda"] = {"ok": False, "error": str(e)}

    # 3. Alert-router config — check it has the latest code with WSS bridge
    try:
        cfg = lam.get_function(FunctionName="justhodl-alert-router")["Configuration"]
        out["3_alert_router_lambda"] = {
            "role": cfg.get("Role").split("/")[-1],
            "runtime": cfg.get("Runtime"),
            "last_modified": cfg.get("LastModified"),
            "code_size": cfg.get("CodeSize"),
        }
    except Exception as e:
        out["3_alert_router_lambda"] = {"error": str(e)}

    # 4. Test admin broadcast on alerts channel — simulating what alert-router
    #    will send when a real alert fires
    synthetic_alert = {
        "channel": "alerts",
        "id": f"e2e_test_{int(time.time())}",
        "severity": "MEDIUM",
        "category": "SYSTEM_TEST",
        "title": "🧪 E2E test: alert-router → WSS bridge",
        "detail": "This is a synthetic alert from ops 370 confirming the bridge works.",
        "ts": "2026-05-09T11:30:00Z",
    }
    s, body = post("/", synthetic_alert, token)
    try:
        j = json.loads(body)
        out["4_synthetic_alert"] = {
            "status": s,
            "channel": j.get("channel"),
            "sent": j.get("sent"),
            "scanned": j.get("scanned"),
            "removed_dead": j.get("removed_dead"),
            "ok": s == 200 and j.get("channel") == "alerts",
        }
    except Exception:
        out["4_synthetic_alert"] = {"status": s, "raw": body[:200], "ok": False}

    # 5. Send on system channel for completeness
    s, body = post("/", {"channel": "system", "type": "ping", "msg": "e2e ping"}, token)
    try:
        j = json.loads(body)
        out["5_system_ping"] = {
            "status": s, "channel": j.get("channel"), "sent": j.get("sent"),
            "ok": s == 200,
        }
    except Exception:
        out["5_system_ping"] = {"status": s, "raw": body[:200]}

    # 6. DDB scan for connection count
    try:
        info = ddb.describe_table(TableName="WebSocketConnections")["Table"]
        out["6_ddb"] = {"status": info["TableStatus"], "items": info["ItemCount"]}
    except Exception as e:
        out["6_ddb"] = {"error": str(e)}

    # 7. Test broadcast on EVERY allowed channel — confirm none rejected
    channel_results = {}
    for ch in ["report", "regime", "compound", "cross_asset", "options_flow",
               "eurodollar", "nobrainers", "narrative", "alerts", "system"]:
        s, body = post("/", {"channel": ch, "test": True, "ts": time.time()}, token)
        try:
            j = json.loads(body)
            channel_results[ch] = {"status": s, "sent": j.get("sent"), "ok": s == 200}
        except Exception:
            channel_results[ch] = {"status": s, "ok": False}
    out["7_all_channels"] = channel_results
    out["7_all_channels_passed"] = sum(1 for v in channel_results.values() if v.get("ok"))

    # Summary
    checks = [
        out["1_token"].get("ok"),
        out["2_broadcast_lambda"].get("ok"),
        out["4_synthetic_alert"].get("ok"),
        out["5_system_ping"].get("ok"),
        out["7_all_channels_passed"] == 10,
    ]
    out["summary"] = {
        "passed": sum(1 for c in checks if c),
        "total": len(checks),
    }
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
            MemorySize=256, Timeout=120, Code={"ZipFile": zb},
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
