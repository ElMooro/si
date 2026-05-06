#!/usr/bin/env python3
"""Step 292 — Phase 2B verification: treasury-proxy + nasdaq-datalink-agent.

Same matrix as Phase 2A — both Lambdas should now require auth or
matching Origin. Includes 30s settle delay to avoid the deploy-race
issue that bit step 290.

Tests for each Lambda:
  X1: no key, no origin              → 401
  X2: Origin: justhodl.ai             → 200 (bypass)
  X3: Origin: evil.com                → 401
  X4: Referer: justhodl.ai/page.html  → 200 (bypass)
  X5: valid PRO key                   → 200 with real data
  X6: revoked key                     → 403
"""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlencode

import boto3

REGION = "us-east-1"
ADMIN_LAMBDA = "justhodl-api-keys-admin"
ADMIN_TOKEN_SSM = "/justhodl/api-admin/token"
TREASURY_URL = "https://x6ssu4ow2iinjhsmgdwqzqspqm0dtglv.lambda-url.us-east-1.on.aws/"
NASDAQ_URL = "https://ff66pvk3anywuluh75k7khvpaq0tslft.lambda-url.us-east-1.on.aws/"
REPORT_PATH = "aws/ops/reports/292_phase2b_verification.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def call(url, headers=None, query=None, timeout=15):
    if query:
        url = url.rstrip("/") + "?" + urlencode(query)
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            try:
                return r.status, json.loads(text)
            except Exception:
                return r.status, text[:300]
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        try:
            return e.code, json.loads(text)
        except Exception:
            return e.code, text[:300]
    except Exception as e:
        return 0, {"err": str(e)[:300]}


def test_lambda(name, url, settle_seconds, query, ok_data_check, admin_url, admin_token):
    """Run the 6-assertion matrix against a single Lambda."""
    out = {"settle_s": settle_seconds}
    time.sleep(settle_seconds)

    # Issue a PRO key for this Lambda's tests
    body = json.dumps({
        "action": "create", "tier": "PRO",
        "owner_email": f"phase2b-{name}@justhodl.ai",
        "label": f"phase2b-{name}-test",
    }).encode()
    req = urllib.request.Request(
        admin_url, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {admin_token}"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        kd = json.loads(r.read())
    plain_key = kd["key"]
    key_hash = kd["key_hash"]

    # X1: no key, no origin
    s, b = call(url, query=query)
    out["X1_no_key_no_origin"] = {"status": s, "expected": 401, "ok": s == 401}

    # X2: Origin: justhodl.ai
    s, b = call(url, query=query, headers={"Origin": "https://justhodl.ai"})
    out["X2_origin_bypass"] = {
        "status": s, "expected": 200, "ok": s == 200,
        "got_data": ok_data_check(b) if isinstance(b, dict) else False,
    }

    # X3: Origin: evil.com
    s, b = call(url, query=query, headers={"Origin": "https://evil.com"})
    out["X3_evil_origin"] = {"status": s, "expected": 401, "ok": s == 401}

    # X4: Referer
    s, b = call(url, query=query,
                headers={"Referer": "https://justhodl.ai/somepage.html"})
    out["X4_referer_bypass"] = {
        "status": s, "expected": 200, "ok": s == 200,
        "got_data": ok_data_check(b) if isinstance(b, dict) else False,
    }

    # X5: valid PRO key
    s, b = call(url, query=query,
                headers={"Authorization": f"Bearer {plain_key}"})
    out["X5_api_key"] = {
        "status": s, "expected": 200, "ok": s == 200,
        "got_data": ok_data_check(b) if isinstance(b, dict) else False,
    }

    # Revoke + verify
    body = json.dumps({"action": "revoke", "key_hash": key_hash}).encode()
    req = urllib.request.Request(
        admin_url, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {admin_token}"},
    )
    urllib.request.urlopen(req, timeout=15).read()
    time.sleep(1)

    s, b = call(url, query=query, headers={"Authorization": f"Bearer {plain_key}"})
    out["X6_revoked_key"] = {"status": s, "expected": 403, "ok": s == 403}

    out["all_passed"] = all(out[k].get("ok") for k in
                              ["X1_no_key_no_origin", "X2_origin_bypass",
                               "X3_evil_origin", "X4_referer_bypass",
                               "X5_api_key", "X6_revoked_key"])
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        admin_url = lam.get_function_url_config(FunctionName=ADMIN_LAMBDA)["FunctionUrl"]
        admin_token = ssm.get_parameter(
            Name=ADMIN_TOKEN_SSM, WithDecryption=True
        )["Parameter"]["Value"]

        # treasury-proxy
        out["treasury"] = test_lambda(
            "treasury", TREASURY_URL, settle_seconds=30,
            query={"action": "health"},
            ok_data_check=lambda b: b.get("status") == "ok",
            admin_url=admin_url, admin_token=admin_token,
        )

        # nasdaq-datalink-agent (use /health path via rawPath)
        out["nasdaq"] = test_lambda(
            "nasdaq", NASDAQ_URL.rstrip("/") + "/health", settle_seconds=2,
            query=None,
            ok_data_check=lambda b: b.get("status") == "healthy",
            admin_url=admin_url, admin_token=admin_token,
        )

        out["all_passed"] = (
            out["treasury"].get("all_passed", False)
            and out["nasdaq"].get("all_passed", False)
        )
        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0 if out.get("all_passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
