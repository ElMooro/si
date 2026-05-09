#!/usr/bin/env python3
"""Step 366 — End-to-end verify of the WebSocket pipeline."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/366_websocket_verify.json"
NAME = "justhodl-tmp-wss-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.error, time, socket, ssl, base64, os
import boto3

ddb = boto3.client("dynamodb", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
apigw = boto3.client("apigatewayv2", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

BROADCAST_URL = "https://p6kvtojb2y6r4orgbtxh7ld3nu0pfktz.lambda-url.us-east-1.on.aws"

def http_json(method, url, body=None, headers=None, timeout=15):
    h = {"Content-Type": "application/json"} if body else {}
    if headers: h.update(headers)
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")

def ws_handshake_probe(host, path, port=443):
    """Just probe that the WS upgrade returns 101 — minimal HTTP/1.1 handshake.
    We don't keep the connection open or send messages; just confirm the
    server accepts the upgrade and the routes are wired to a Lambda.
    """
    try:
        ctx = ssl.create_default_context()
        sock = socket.create_connection((host, port), timeout=10)
        wrapped = ctx.wrap_socket(sock, server_hostname=host)
        # WebSocket key = base64 of 16 random bytes
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {path} HTTP/1.1\\r\\n"
            f"Host: {host}\\r\\n"
            f"Upgrade: websocket\\r\\n"
            f"Connection: Upgrade\\r\\n"
            f"Sec-WebSocket-Key: {key}\\r\\n"
            f"Sec-WebSocket-Version: 13\\r\\n"
            f"\\r\\n"
        )
        wrapped.send(req.encode())
        # Read response status line
        chunks = []
        wrapped.settimeout(5)
        for _ in range(8):
            try:
                data = wrapped.recv(2048)
                if not data: break
                chunks.append(data)
                if b"\\r\\n\\r\\n" in b"".join(chunks): break
            except socket.timeout:
                break
        wrapped.close()
        head = b"".join(chunks).decode("utf-8", errors="ignore")
        first_line = head.split("\\r\\n", 1)[0] if head else ""
        return {"status_line": first_line, "got_101": "101" in first_line, "head_bytes": len(head)}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

def lambda_handler(event, context):
    out = {}

    # 1. SSM endpoint configured
    try:
        ep = ssm.get_parameter(Name="/justhodl/wss/endpoint")["Parameter"]["Value"]
        out["1_ssm_endpoint"] = {"value": ep, "ok": ep.startswith("wss://")}
    except Exception as e:
        out["1_ssm_endpoint"] = {"error": str(e)}

    # 2. WSS handshake probe — confirms API Gateway accepts WebSocket upgrade
    try:
        ep = out["1_ssm_endpoint"].get("value", "")
        if ep.startswith("wss://"):
            host = ep.split("wss://")[1].split("/")[0]
            path = "/" + ep.split("/", 3)[3] if ep.count("/") >= 3 else "/"
            out["2_ws_handshake"] = ws_handshake_probe(host, path)
            out["2_ws_handshake"]["ok"] = out["2_ws_handshake"].get("got_101", False)
    except Exception as e:
        out["2_ws_handshake"] = {"error": str(e)}

    # 3. Broadcast GET /  — health
    try:
        s, body = http_json("GET", BROADCAST_URL + "/")
        j = json.loads(body)
        out["3_broadcast_health"] = {
            "status": s, "service": j.get("service"),
            "ws_api_id": j.get("ws_api_id"), "stage": j.get("stage"),
            "connections": j.get("connections"),
            "channels": len(j.get("channels", [])),
            "tracked_keys": len(j.get("tracked_s3_keys", [])),
            "ok": s == 200 and j.get("service") == "openbb-websocket-broadcast",
        }
    except Exception as e:
        out["3_broadcast_health"] = {"error": str(e)}

    # 4. POST /send without admin → 401
    try:
        s, body = http_json("POST", BROADCAST_URL + "/", {"channel": "alerts", "title": "test"})
        out["4_send_no_token"] = {"status": s, "ok": s == 401}
    except Exception as e:
        out["4_send_no_token"] = {"error": str(e)}

    # 5. POST /send with admin token → 200 (will return sent=0 if nobody connected)
    try:
        token = ssm.get_parameter(Name="/justhodl/push/admin-token", WithDecryption=True)["Parameter"]["Value"]
        s, body = http_json("POST", BROADCAST_URL + "/",
                             {"channel": "system", "title": "verify-366", "body": "wss test"},
                             {"X-Justhodl-Admin-Token": token})
        j = json.loads(body)
        out["5_admin_broadcast"] = {
            "status": s, "channel": j.get("channel"), "sent": j.get("sent"),
            "scanned": j.get("scanned"),
            "ok": s == 200 and "channel" in j,
        }
    except Exception as e:
        out["5_admin_broadcast"] = {"error": str(e)}

    # 6. DDB table state
    try:
        info = ddb.describe_table(TableName="WebSocketConnections")["Table"]
        out["6_ddb"] = {
            "status": info["TableStatus"], "items": info["ItemCount"],
            "ok": info["TableStatus"] == "ACTIVE",
        }
    except Exception as e:
        out["6_ddb"] = {"error": str(e)}

    # 7. S3 bucket notification configured
    try:
        cfg = s3.get_bucket_notification_configuration(Bucket="justhodl-dashboard-live")
        ws_configs = [c for c in cfg.get("LambdaFunctionConfigurations", []) if (c.get("Id") or "").startswith("ws-broadcast-")]
        out["7_s3_events"] = {"count": len(ws_configs), "ok": len(ws_configs) >= 6}
    except Exception as e:
        out["7_s3_events"] = {"error": str(e)}

    # 8. API Gateway routes
    try:
        apis = apigw.get_apis().get("Items", [])
        ws_apis = [a for a in apis if a.get("Name") == "justhodl-wss"]
        if ws_apis:
            api_id = ws_apis[0]["ApiId"]
            routes = apigw.get_routes(ApiId=api_id).get("Items", [])
            stages = apigw.get_stages(ApiId=api_id).get("Items", [])
            out["8_api_gateway"] = {
                "api_id": api_id, "n_routes": len(routes),
                "route_keys": sorted([r["RouteKey"] for r in routes]),
                "n_stages": len(stages),
                "ok": len(routes) >= 3 and len(stages) >= 1,
            }
        else:
            out["8_api_gateway"] = {"ok": False, "error": "API not found"}
    except Exception as e:
        out["8_api_gateway"] = {"error": str(e)}

    # 9. Frontend assets — wss-client.js patched with real URL
    try:
        for asset in ["/wss-client.js", "/index.html"]:
            try:
                req = urllib.request.Request("https://justhodl.ai" + asset)
                with urllib.request.urlopen(req, timeout=15) as r:
                    body = r.read().decode("utf-8", errors="ignore")
                    out.setdefault("9_frontend", {})[asset] = {
                        "status": r.status, "size": len(body),
                        "has_placeholder": "__WS_API_ID__" in body,
                        "has_real_url": "q7vco36knh" in body if asset == "/wss-client.js" else None,
                        "has_live_pill": "wss-status" in body if asset == "/index.html" else None,
                    }
            except Exception as e:
                out.setdefault("9_frontend", {})[asset] = {"error": str(e)[:100]}
    except Exception as e:
        out["9_frontend"] = {"error": str(e)}

    # Summary
    checks = [
        out["1_ssm_endpoint"].get("ok"),
        out.get("2_ws_handshake", {}).get("ok"),
        out["3_broadcast_health"].get("ok"),
        out["4_send_no_token"].get("ok"),
        out["5_admin_broadcast"].get("ok"),
        out["6_ddb"].get("ok"),
        out["7_s3_events"].get("ok"),
        out["8_api_gateway"].get("ok"),
    ]
    passed = sum(1 for c in checks if c)
    out["summary"] = {"passed": passed, "total": len(checks), "rate": f"{passed}/{len(checks)}"}
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
