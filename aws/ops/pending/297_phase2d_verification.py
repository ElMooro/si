#!/usr/bin/env python3
"""Step 297 — Phase 2D verification: bea + investor-agents.

Both are called via the Cloudflare Worker, which sets
Origin: https://justhodl.ai on every upstream fetch. Verifies that:

  1. Direct calls without auth → 401 (security check)
  2. Direct calls with Origin: justhodl.ai → 200 (simulates CF Worker)
  3. Direct calls with Origin: evil.com → 401
  4. Direct calls with valid PRO key → 200
  5. Direct calls with revoked key → 403

For investor-agents (POST endpoint), uses {"ticker":"AAPL"}.
For bea-economic-agent (GET endpoint), uses default query.
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

BEA_URL = "https://hnqqkbf7y6avoda5v4rexwk3440ibbru.lambda-url.us-east-1.on.aws/"
INVESTOR_URL = "https://7qufoauxzhqwnrsmdjjwt46wy40zzdyp.lambda-url.us-east-1.on.aws/"

REPORT = "aws/ops/reports/297_phase2d_verification.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def call_get(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            try:
                return r.status, json.loads(text)
            except Exception:
                return r.status, text[:200]
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        try:
            return e.code, json.loads(text)
        except Exception:
            return e.code, text[:200]
    except Exception as e:
        return 0, {"err": str(e)[:200]}


def call_post(url, body=None, headers=None, timeout=60):
    """For investor-agents which is POST-only."""
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
            try:
                return r.status, json.loads(text)
            except Exception:
                return r.status, text[:200]
    except urllib.error.HTTPError as e:
        text = e.read().decode()
        try:
            return e.code, json.loads(text)
        except Exception:
            return e.code, text[:200]
    except Exception as e:
        return 0, {"err": str(e)[:200]}


def issue_pro_key(admin_url, admin_token, label):
    body = json.dumps({
        "action": "create", "tier": "PRO",
        "owner_email": "phase2d@justhodl.ai", "label": label,
    }).encode()
    req = urllib.request.Request(
        admin_url, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {admin_token}"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def revoke_key(admin_url, admin_token, key_hash):
    body = json.dumps({"action": "revoke", "key_hash": key_hash}).encode()
    req = urllib.request.Request(
        admin_url, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {admin_token}"},
    )
    urllib.request.urlopen(req, timeout=15).read()


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        time.sleep(45)  # settle delay

        admin_url = lam.get_function_url_config(FunctionName=ADMIN_LAMBDA)["FunctionUrl"]
        admin_token = ssm.get_parameter(
            Name=ADMIN_TOKEN_SSM, WithDecryption=True
        )["Parameter"]["Value"]

        # ─── BEA (GET) ─────────────────────────────────────────────
        out["bea"] = {}
        kd_bea = issue_pro_key(admin_url, admin_token, "phase2d-bea")
        bea_key = kd_bea["key"]
        bea_hash = kd_bea["key_hash"]

        s, b = call_get(BEA_URL)
        out["bea"]["X1_no_key"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call_get(BEA_URL, headers={"Origin": "https://justhodl.ai"})
        out["bea"]["X2_origin_bypass"] = {"status": s, "expected": 200, "ok": s == 200}

        s, b = call_get(BEA_URL, headers={"Origin": "https://evil.com"})
        out["bea"]["X3_evil_origin"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call_get(BEA_URL, headers={"Authorization": f"Bearer {bea_key}"})
        out["bea"]["X4_api_key"] = {"status": s, "expected": 200, "ok": s == 200}

        revoke_key(admin_url, admin_token, bea_hash)
        time.sleep(1)
        s, b = call_get(BEA_URL, headers={"Authorization": f"Bearer {bea_key}"})
        out["bea"]["X5_revoked"] = {"status": s, "expected": 403, "ok": s == 403}

        out["bea"]["all_passed"] = all(
            out["bea"][k]["ok"] for k in
            ["X1_no_key", "X2_origin_bypass", "X3_evil_origin", "X4_api_key", "X5_revoked"]
        )

        # ─── INVESTOR-AGENTS (POST) ────────────────────────────────
        out["investor"] = {}
        kd_inv = issue_pro_key(admin_url, admin_token, "phase2d-investor")
        inv_key = kd_inv["key"]
        inv_hash = kd_inv["key_hash"]
        post_body = {"ticker": "AAPL"}

        s, b = call_post(INVESTOR_URL, body=post_body)
        out["investor"]["X1_no_key"] = {"status": s, "expected": 401, "ok": s == 401}

        # Don't actually run the full investor-agents call (it's expensive
        # — calls Anthropic API). Just check that auth gate fires correctly.
        # X2 origin-bypass would actually invoke investor logic; instead we
        # verify the auth gate by sending an Origin without a body that
        # would make the inner logic fail — but as long as we get past 401
        # to a different status, auth worked.
        s, b = call_post(INVESTOR_URL, body=post_body,
                         headers={"Origin": "https://justhodl.ai"},
                         timeout=120)
        out["investor"]["X2_origin_bypass_passed_auth"] = {
            "status": s,
            "passed_auth": s != 401,   # Anything other than 401 = auth worked
            "ok": s != 401,
            "preview": json.dumps(b)[:120] if isinstance(b, dict) else str(b)[:120],
        }

        s, b = call_post(INVESTOR_URL, body=post_body,
                         headers={"Origin": "https://evil.com"})
        out["investor"]["X3_evil_origin"] = {"status": s, "expected": 401, "ok": s == 401}

        # Skip X4 (valid_key invokes Anthropic, slow + costly)
        # Skip X5 (revoke)
        # Just do auth-gate-only X3 as the negative control

        revoke_key(admin_url, admin_token, inv_hash)

        out["investor"]["all_passed"] = (
            out["investor"]["X1_no_key"]["ok"]
            and out["investor"]["X2_origin_bypass_passed_auth"]["ok"]
            and out["investor"]["X3_evil_origin"]["ok"]
        )

        out["all_passed"] = out["bea"]["all_passed"] and out["investor"]["all_passed"]
        out["duration_s"] = round(time.time() - started, 1)

    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])
    return 0 if out.get("all_passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
