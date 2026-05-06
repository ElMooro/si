#!/usr/bin/env python3
"""Step 295 — Re-verify charts-agent after the UnboundLocalError fix.

Step 293 saw 20/21 with one fail: charts-agent strict_valid_key=502.
Step 294 confirmed pre-existing bug. Commit 39c09b3 fixed it.
This step re-runs only the failing assertion to confirm 21/21.

If status=200, Phase 2C is fully verified across all 4 Lambdas.
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
REPORT = "aws/ops/reports/295_charts_agent_reverify.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # Settle delay — give the deploy time to land
        time.sleep(20)

        admin_url = lam.get_function_url_config(FunctionName=ADMIN_LAMBDA)["FunctionUrl"]
        admin_token = ssm.get_parameter(
            Name=ADMIN_TOKEN_SSM, WithDecryption=True
        )["Parameter"]["Value"]

        # Issue PRO key
        body = json.dumps({
            "action": "create", "tier": "PRO",
            "owner_email": "phase2c-finalize@justhodl.ai",
            "label": "phase2c-charts-reverify",
        }).encode()
        req = urllib.request.Request(
            admin_url, data=body, method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {admin_token}"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            kd = json.loads(r.read())
        plain_key = kd["key"]

        # Call charts-agent with valid key — expect 200 now
        req = urllib.request.Request(
            CHARTS_URL,
            headers={"Authorization": f"Bearer {plain_key}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read()
                content_type = r.headers.get("Content-Type", "")
                out["valid_key"] = {
                    "status": r.status,
                    "expected": 200,
                    "ok": r.status == 200,
                    "content_type": content_type,
                    "body_size": len(body),
                    "body_preview": body[:200].decode(errors="replace"),
                }
        except urllib.error.HTTPError as e:
            out["valid_key"] = {
                "status": e.code,
                "expected": 200,
                "ok": False,
                "body_preview": e.read()[:200].decode(errors="replace"),
            }

        out["fix_verified"] = out["valid_key"]["ok"]
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:2000])
    return 0 if out.get("fix_verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())
