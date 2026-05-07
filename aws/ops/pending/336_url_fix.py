#!/usr/bin/env python3
"""Step 336 — Add Lambda URLs to watchlist + trade-journal (CORS fix).

Issue: AllowMethods rejected "OPTIONS" (7 chars > 6 max).
Fix: Use ["*"] to allow all methods (canonical AWS pattern).
The Lambdas already deployed successfully — only URL creation failed.
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
REPORT = "aws/ops/reports/336_url_fix.json"
lam = boto3.client("lambda", region_name=REGION)

CORS = {
    "AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai", "*"],
    "AllowMethods": ["*"],  # all methods incl OPTIONS
    "AllowHeaders": ["content-type", "x-justhodl-token"],
    "MaxAge": 86400,
}


def ensure_url(name):
    try:
        cur = lam.get_function_url_config(FunctionName=name)
        lam.update_function_url_config(
            FunctionName=name, AuthType="NONE", Cors=CORS, InvokeMode="BUFFERED",
        )
        return cur.get("FunctionUrl")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        out = lam.create_function_url_config(
            FunctionName=name, AuthType="NONE", Cors=CORS, InvokeMode="BUFFERED",
        )
        return out.get("FunctionUrl")


def add_invoke_perm(name):
    try:
        lam.add_permission(
            FunctionName=name, StatementId="public-url-invoke",
            Action="lambda:InvokeFunctionUrl", Principal="*",
            FunctionUrlAuthType="NONE",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise


def smoke_url(name, url):
    """Sync invoke via Lambda URL simulation."""
    payload = {
        "requestContext": {"http": {"method": "GET"}},
        "rawPath": "/", "headers": {"origin": "https://justhodl.ai"}, "body": "",
    }
    started = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                      Payload=json.dumps(payload).encode("utf-8"))
    out = {
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "duration_s": round(time.time() - started, 1),
    }
    body = resp["Payload"].read().decode("utf-8")
    try:
        out["response"] = json.loads(body)
    except Exception:
        out["response_raw"] = body[:300]
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}
    for name in ("justhodl-watchlist", "justhodl-trade-journal"):
        info = {"name": name}
        try:
            info["url"] = ensure_url(name)
            time.sleep(1)
            add_invoke_perm(name)
            time.sleep(1)
            info["smoke_test"] = smoke_url(name, info["url"])
        except Exception as e:
            import traceback
            info["err"] = str(e)
            info["trace"] = traceback.format_exc()[-1000:]
        out["lambdas"][name] = info
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:4000])


if __name__ == "__main__":
    main()
