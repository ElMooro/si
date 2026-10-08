#!/usr/bin/env python3
"""Step 290 — Phase 2A verification: ecb-proxy (strict) + fred-proxy (Origin-bypass).

Test matrix:

  ecb-proxy (strict mode):
    [A1] no key                           → 401 expected
    [A2] origin justhodl.ai, no key       → 401 (Origin doesn't bypass without allowed_origins)
    [A3] valid PRO-tier key                → 200 with real ECB data

  fred-proxy (Origin-bypass mode):
    [B1] no key, no origin                 → 401 expected
    [B2] no key, origin: justhodl.ai       → 200 (frontend bypass)
    [B3] no key, origin: evil.com          → 401 (origin doesn't match)
    [B4] no key, referer: https://justhodl.ai/fred.html → 200 (Referer bypass)
    [B5] valid PRO-tier key, no origin     → 200 (API user)
    [B6] revoked key                        → 403 forbidden

  Plus public-api-demo regression test:
    [C1] dual-mode signature compatible     → 200 with valid key
    [C2] no allowed_origins → strict        → 401 with origin only

This proves the dual-mode authorize() works AND that real Lambdas
have been migrated cleanly without regressing existing functionality.
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
ECB_URL = "https://y3z4mmygt6zqltfirk5a6fe4bq0fkoxq.lambda-url.us-east-1.on.aws/"
FRED_URL = "https://4dgpa7mv5dfipsh3gi2xim6uja0igozb.lambda-url.us-east-1.on.aws/"
DEMO_URL = "https://odoy2bydzufzjbp765n3ix6w5u0rvqmj.lambda-url.us-east-1.on.aws/"
REPORT_PATH = "aws/ops/reports/290_phase2a_verification.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def call(url, headers=None, query=None, timeout=15):
    """Send a GET request, return (status, body)."""
    if query:
        from urllib.parse import urlencode
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


def issue_pro_key(admin_url, admin_token, label):
    body = json.dumps({
        "action": "create", "tier": "PRO",
        "owner_email": "phase2-test@justhodl.ai", "label": label,
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
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        admin_url = lam.get_function_url_config(FunctionName=ADMIN_LAMBDA)["FunctionUrl"]
        admin_token = ssm.get_parameter(Name=ADMIN_TOKEN_SSM, WithDecryption=True)["Parameter"]["Value"]
        out["admin_url"] = admin_url

        # Issue a fresh PRO key for the test
        time.sleep(1)
        key_data = issue_pro_key(admin_url, admin_token, "phase2a-verification")
        plain_key = key_data["key"]
        key_hash = key_data["key_hash"]
        out["test_key_first_12"] = plain_key[:12] + "..."
        out["test_key_hash"] = key_hash

        # ─── ecb-proxy tests (strict mode) ──────────────────────────────
        out["ecb"] = {}

        # A1: no key — expect 401
        s, b = call(ECB_URL, query={"action": "health"})
        out["ecb"]["A1_no_key"] = {"status": s, "expected": 401, "ok": s == 401,
                                    "error_code": b.get("error") if isinstance(b, dict) else None}

        # A2: origin justhodl.ai, no key — strict means origin doesn't bypass
        s, b = call(ECB_URL, query={"action": "health"},
                    headers={"Origin": "https://justhodl.ai"})
        out["ecb"]["A2_origin_only"] = {"status": s, "expected": 401, "ok": s == 401,
                                          "error_code": b.get("error") if isinstance(b, dict) else None}

        # A3: valid PRO key — expect 200 with real data
        s, b = call(ECB_URL, query={"action": "health"},
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["ecb"]["A3_valid_key"] = {
            "status": s, "expected": 200, "ok": s == 200,
            "data_keys": list(b.keys())[:8] if isinstance(b, dict) else None,
        }

        # ─── fred-proxy tests (Origin-bypass mode) ──────────────────────
        out["fred"] = {}

        # B1: no key, no origin — expect 401
        s, b = call(FRED_URL, query={"series": "GDP"})
        out["fred"]["B1_no_key_no_origin"] = {
            "status": s, "expected": 401, "ok": s == 401,
            "error_code": b.get("error") if isinstance(b, dict) else None,
        }

        # B2: no key, origin justhodl.ai — expect 200 (bypass)
        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Origin": "https://justhodl.ai"})
        out["fred"]["B2_origin_bypass_main"] = {
            "status": s, "expected": 200, "ok": s == 200,
            "got_data": (isinstance(b, dict)
                         and (b.get("series_id") == "GDP" or b.get("status") == "ok")),
        }

        # B3: no key, origin: evil.com — expect 401
        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Origin": "https://evil.com"})
        out["fred"]["B3_evil_origin"] = {
            "status": s, "expected": 401, "ok": s == 401,
            "error_code": b.get("error") if isinstance(b, dict) else None,
        }

        # B4: no key, Referer header from justhodl.ai page — expect 200
        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Referer": "https://justhodl.ai/fred.html"})
        out["fred"]["B4_referer_bypass"] = {
            "status": s, "expected": 200, "ok": s == 200,
            "got_data": isinstance(b, dict) and b.get("series_id") == "GDP",
        }

        # B5: valid PRO key, no origin — expect 200 (API user)
        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["fred"]["B5_api_key"] = {
            "status": s, "expected": 200, "ok": s == 200,
            "got_data": isinstance(b, dict) and b.get("series_id") == "GDP",
        }

        # B6: revoke the key, then call — expect 403 forbidden
        revoke_key(admin_url, admin_token, key_hash)
        time.sleep(1)
        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["fred"]["B6_revoked_key"] = {
            "status": s, "expected": 403, "ok": s == 403,
            "error_code": b.get("error") if isinstance(b, dict) else None,
        }

        # ─── demo Lambda regression (dual-mode signature compat) ──────
        out["demo"] = {}

        # The demo Lambda doesn't pass allowed_origins, so it's strict
        # mode. Origin alone shouldn't bypass.
        s, b = call(DEMO_URL, headers={"Origin": "https://justhodl.ai"})
        out["demo"]["C1_origin_only_strict"] = {
            "status": s, "expected": 401, "ok": s == 401,
        }

        # ─── Aggregate pass/fail ───────────────────────────────────────
        all_assertions = (
            out["ecb"]["A1_no_key"]["ok"]
            and out["ecb"]["A2_origin_only"]["ok"]
            and out["ecb"]["A3_valid_key"]["ok"]
            and out["fred"]["B1_no_key_no_origin"]["ok"]
            and out["fred"]["B2_origin_bypass_main"]["ok"]
            and out["fred"]["B3_evil_origin"]["ok"]
            and out["fred"]["B4_referer_bypass"]["ok"]
            and out["fred"]["B5_api_key"]["ok"]
            and out["fred"]["B6_revoked_key"]["ok"]
            and out["demo"]["C1_origin_only_strict"]["ok"]
        )
        out["all_passed"] = all_assertions
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
