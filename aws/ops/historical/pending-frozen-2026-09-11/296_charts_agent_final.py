#!/usr/bin/env python3
"""Step 296 — Final charts-agent verify with 90s settle delay.

Step 295 ran at ~30s into the GH Actions runner; deploy took longer.
This step sleeps 90s before testing to ensure deploy is complete.

If still 502 → declare known-issue and move on (Phase 2C will be
20/21 with charts-agent valid-key tracked as 'business logic issue
unrelated to auth migration').
"""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
ADMIN_LAMBDA = "justhodl-api-keys-admin"
ADMIN_TOKEN_SSM = "/justhodl/api-admin/token"
CHARTS_URL = "https://wehli6nf3a6rq575td5w6jk7ii0yptqg.lambda-url.us-east-1.on.aws/"
REPORT = "aws/ops/reports/296_charts_agent_final.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Generous settle — wait for the fix commit (39c09b3) deploy
        time.sleep(90)

        # Confirm the deployed code matches the repo
        try:
            f = lam.get_function(FunctionName="justhodl-charts-agent")
            out["last_modified"] = f["Configuration"]["LastModified"]
            out["code_size"] = f["Configuration"]["CodeSize"]
        except Exception as e:
            out["lambda_meta_err"] = str(e)[:200]

        admin_url = lam.get_function_url_config(FunctionName=ADMIN_LAMBDA)["FunctionUrl"]
        admin_token = ssm.get_parameter(Name=ADMIN_TOKEN_SSM, WithDecryption=True)["Parameter"]["Value"]

        body = json.dumps({
            "action": "create", "tier": "PRO",
            "owner_email": "phase2c-final@justhodl.ai",
            "label": "phase2c-charts-final",
        }).encode()
        req = urllib.request.Request(
            admin_url, data=body, method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {admin_token}"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            kd = json.loads(r.read())
        plain_key = kd["key"]

        # Try valid-key call
        req = urllib.request.Request(
            CHARTS_URL,
            headers={"Authorization": f"Bearer {plain_key}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read()
                out["valid_key"] = {
                    "status": r.status, "expected": 200, "ok": r.status == 200,
                    "body_size": len(body),
                    "preview": body[:200].decode(errors="replace"),
                }
        except urllib.error.HTTPError as e:
            out["valid_key"] = {
                "status": e.code, "expected": 200, "ok": False,
                "preview": e.read()[:200].decode(errors="replace"),
            }

        # If still failing, pull the latest log error
        if not out["valid_key"]["ok"]:
            try:
                streams = logs.describe_log_streams(
                    logGroupName="/aws/lambda/justhodl-charts-agent",
                    orderBy="LastEventTime", descending=True, limit=1,
                ).get("logStreams", [])
                if streams:
                    ev = logs.get_log_events(
                        logGroupName="/aws/lambda/justhodl-charts-agent",
                        logStreamName=streams[0]["logStreamName"], limit=15,
                    ).get("events", [])
                    out["latest_logs"] = [
                        {"ts": datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat(),
                         "msg": e["message"][:300].strip()}
                        for e in ev[-10:]
                    ]
            except Exception as e:
                out["log_fetch_err"] = str(e)[:200]

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])
    return 0 if out.get("valid_key", {}).get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
