"""justhodl-subscribe — Morning Brief email capture (audit finding 04).
POST {"email","source"?} -> validates, stores in DDB justhodl-subscribers,
returns {"ok":true}. Delivery wiring (email-reports-v2/SES) is a separate step."""
import json
import os
import re
import time

import boto3

TABLE = os.environ.get("SUB_TABLE", "justhodl-subscribers")
ddb = boto3.client("dynamodb")
EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$")


def _resp(code, body):
    return {"statusCode": code,
            "headers": {"content-type": "application/json",
                        "access-control-allow-origin": "*"},
            "body": json.dumps(body)}


def lambda_handler(event, context):
    try:
        raw = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            import base64
            raw = base64.b64decode(raw).decode()
        d = json.loads(raw)
        email = (d.get("email") or "").strip().lower()
        if not EMAIL.match(email) or len(email) > 254:
            return _resp(400, {"ok": False, "error": "invalid email"})
        ddb.put_item(TableName=TABLE, Item={
            "email": {"S": email},
            "ts": {"N": str(int(time.time()))},
            "source": {"S": str(d.get("source") or "unknown")[:40]},
            "status": {"S": "pending"},
        })
        return _resp(200, {"ok": True})
    except Exception as e:
        print("subscribe error:", e)
        return _resp(500, {"ok": False, "error": "server error"})
