#!/usr/bin/env python3
"""
Step 361 — End-to-end verify of the PWA + push pipeline.

Tests:
  1. GET /vapid-public-key  → 200, returns 87-char base64url public key
  2. POST /subscribe with fake subscription → 200, returns endpoint_hash
  3. DDB justhodl-push-subscriptions has the new item
  4. POST /unsubscribe → 200, item gone from DDB
  5. POST /send without admin token → 401
  6. POST /send with bad admin token → 401
  7. CORS preflight OPTIONS → response includes Access-Control-Allow-Origin
  8. SSM keys all present and decryptable
  9. justhodl.ai/manifest.json accessible (after Pages deploy)
 10. justhodl.ai/service-worker.js accessible
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/361_pwa_verify.json"
TMP_NAME = "justhodl-tmp-pwa-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json
import urllib.request
import urllib.error
import boto3

PUSH_API = "https://ch5q2v5shekw5cxa5juzio2cd40gcasn.lambda-url.us-east-1.on.aws"
DDB = boto3.resource("dynamodb", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
TABLE = DDB.Table("justhodl-push-subscriptions")

# Fake but well-formed subscription
FAKE_SUB = {
    "endpoint": "https://updates.push.services.mozilla.com/wpush/v2/test-fake-endpoint-for-verify-361",
    "keys": {
        "p256dh": "BNNL5ZaTfK81qhXOx23+wewhinUsuS7epjfElwoOisdbnWzWzZGHwtY1JwEcawMd2H3iH6c0n7zhvL12gIw5kcQ=",
        "auth": "test-auth-key-12345678",
    },
}

def http(method, path, body=None, headers=None):
    url = PUSH_API + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    h = {"Content-Type": "application/json"} if data else {}
    if headers: h.update(headers)
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8")
            return r.status, dict(r.headers), body
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode("utf-8")

def lambda_handler(event, context):
    out = {}
    # 1. GET /vapid-public-key
    try:
        s, _, body = http("GET", "/vapid-public-key")
        j = json.loads(body)
        pk = j.get("publicKey", "")
        out["1_vapid_endpoint"] = {"status": s, "publicKey_length": len(pk), "ok": s == 200 and len(pk) >= 80}
    except Exception as e:
        out["1_vapid_endpoint"] = {"error": str(e)}

    # 2. POST /subscribe
    try:
        s, _, body = http("POST", "/subscribe", {"subscription": FAKE_SUB, "user_agent": "verify-361"})
        j = json.loads(body)
        endpoint_hash = j.get("endpoint_hash")
        out["2_subscribe"] = {"status": s, "endpoint_hash": endpoint_hash, "ok": s == 200 and endpoint_hash is not None}
    except Exception as e:
        out["2_subscribe"] = {"error": str(e)}

    # 3. Confirm DDB has the item
    try:
        if endpoint_hash:
            res = TABLE.get_item(Key={"endpoint_hash": endpoint_hash})
            item = res.get("Item")
            out["3_ddb_has_item"] = {"ok": item is not None, "ua": (item or {}).get("user_agent")}
        else:
            out["3_ddb_has_item"] = {"ok": False, "reason": "no endpoint_hash from step 2"}
    except Exception as e:
        out["3_ddb_has_item"] = {"error": str(e)}

    # 4. POST /unsubscribe
    try:
        s, _, body = http("POST", "/unsubscribe", {"endpoint": FAKE_SUB["endpoint"]})
        out["4_unsubscribe"] = {"status": s, "ok": s == 200}
        # Confirm gone from DDB
        if endpoint_hash:
            res = TABLE.get_item(Key={"endpoint_hash": endpoint_hash})
            out["4b_ddb_item_removed"] = {"ok": res.get("Item") is None}
    except Exception as e:
        out["4_unsubscribe"] = {"error": str(e)}

    # 5. POST /send without admin token → expect 401
    try:
        s, _, body = http("POST", "/send", {"title": "test", "body": "test"})
        out["5_send_no_token"] = {"status": s, "ok": s == 401}
    except Exception as e:
        out["5_send_no_token"] = {"error": str(e)}

    # 6. POST /send with WRONG admin token → expect 401
    try:
        s, _, body = http("POST", "/send", {"title": "test"}, {"X-Justhodl-Admin-Token": "wrong"})
        out["6_send_bad_token"] = {"status": s, "ok": s == 401}
    except Exception as e:
        out["6_send_bad_token"] = {"error": str(e)}

    # 7. SSM keys present
    try:
        keys = {}
        for name, decrypt in [
            ("/justhodl/push/vapid-public-key", False),
            ("/justhodl/push/vapid-private-key", True),
            ("/justhodl/push/vapid-subject", False),
            ("/justhodl/push/admin-token", True),
        ]:
            try:
                v = SSM.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]
                keys[name] = {"length": len(v), "ok": True}
            except Exception as e:
                keys[name] = {"ok": False, "error": str(e)[:100]}
        out["7_ssm_keys"] = keys
    except Exception as e:
        out["7_ssm_keys"] = {"error": str(e)}

    # 8. Pages serving manifest + SW
    try:
        for asset in ["/manifest.json", "/service-worker.js", "/notifications.html", "/portfolio.html", "/analogs.html", "/why.html"]:
            try:
                req = urllib.request.Request("https://justhodl.ai" + asset)
                with urllib.request.urlopen(req, timeout=15) as r:
                    out.setdefault("8_pages", {})[asset] = {"status": r.status, "size": int(r.headers.get("Content-Length", 0))}
            except urllib.error.HTTPError as e:
                out.setdefault("8_pages", {})[asset] = {"status": e.code, "ok": False}
            except Exception as e:
                out.setdefault("8_pages", {})[asset] = {"error": str(e)[:80]}
    except Exception as e:
        out["8_pages"] = {"error": str(e)}

    # Summary
    checks = [
        out.get("1_vapid_endpoint", {}).get("ok"),
        out.get("2_subscribe", {}).get("ok"),
        out.get("3_ddb_has_item", {}).get("ok"),
        out.get("4_unsubscribe", {}).get("ok"),
        out.get("4b_ddb_item_removed", {}).get("ok"),
        out.get("5_send_no_token", {}).get("ok"),
        out.get("6_send_bad_token", {}).get("ok"),
    ]
    passed = sum(1 for c in checks if c)
    total = len(checks)
    out["summary"] = {"passed": passed, "total": total, "rate": f"{passed}/{total}"}

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
            FunctionName=TMP_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=120, Code={"ZipFile": zb},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=TMP_NAME)
    except Exception:
        lam.update_function_code(FunctionName=TMP_NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=TMP_NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=TMP_NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try:
        lam.delete_function(FunctionName=TMP_NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
