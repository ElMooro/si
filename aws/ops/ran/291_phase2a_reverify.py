#!/usr/bin/env python3
"""Step 291 — Re-run Phase 2A verification after deploys have settled.

Step 290's verification ran in parallel with deploy-lambdas.yml, so
some calls hit the OLD (unauthed) Lambda code before the new code was
deployed. Tests A2 (ecb 401 on origin-only) PASSED, proving the new
code IS deployed — A1, B1, B3, B6 racing the deploy is the cleanest
explanation.

This step is the same matrix as 290 but runs after a 30s settle delay
to ensure deploys are fully propagated. If all 10 still fail, there's
a real bug; if all pass, the issue was just racing.
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
ECB_URL = "https://y3z4mmygt6zqltfirk5a6fe4bq0fkoxq.lambda-url.us-east-1.on.aws/"
FRED_URL = "https://4dgpa7mv5dfipsh3gi2xim6uja0igozb.lambda-url.us-east-1.on.aws/"
DEMO_URL = "https://odoy2bydzufzjbp765n3ix6w5u0rvqmj.lambda-url.us-east-1.on.aws/"
REPORT_PATH = "aws/ops/reports/291_phase2a_reverify.json"

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


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        admin_url = lam.get_function_url_config(FunctionName=ADMIN_LAMBDA)["FunctionUrl"]
        admin_token = ssm.get_parameter(
            Name=ADMIN_TOKEN_SSM, WithDecryption=True
        )["Parameter"]["Value"]

        # Confirm deploys settled — wait 30s
        print("[291] settle delay…")
        time.sleep(30)

        # Issue a fresh PRO key
        body = json.dumps({
            "action": "create", "tier": "PRO",
            "owner_email": "phase2a-reverify@justhodl.ai",
            "label": "phase2a-reverify-291",
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
        out["test_key_first_12"] = plain_key[:12] + "..."

        # ─── ECB strict tests ──────────────────────────────────────
        out["ecb"] = {}

        s, b = call(ECB_URL, query={"action": "health"})
        out["ecb"]["A1_no_key"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call(ECB_URL, query={"action": "health"},
                    headers={"Origin": "https://justhodl.ai"})
        out["ecb"]["A2_origin_only"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call(ECB_URL, query={"action": "health"},
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["ecb"]["A3_valid_key"] = {
            "status": s, "expected": 200, "ok": s == 200,
            "data_keys": list(b.keys())[:8] if isinstance(b, dict) else None,
        }

        # ─── FRED Origin-bypass tests ──────────────────────────────
        out["fred"] = {}

        s, b = call(FRED_URL, query={"series": "GDP"})
        out["fred"]["B1_no_key_no_origin"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Origin": "https://justhodl.ai"})
        out["fred"]["B2_origin_bypass_main"] = {
            "status": s, "expected": 200, "ok": s == 200,
            "got_data": (isinstance(b, dict)
                         and (b.get("series_id") == "GDP" or b.get("status") == "ok")),
        }

        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Origin": "https://evil.com"})
        out["fred"]["B3_evil_origin"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Referer": "https://justhodl.ai/fred.html"})
        out["fred"]["B4_referer_bypass"] = {
            "status": s, "expected": 200, "ok": s == 200,
            "got_data": isinstance(b, dict) and b.get("series_id") == "GDP",
        }

        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["fred"]["B5_api_key"] = {
            "status": s, "expected": 200, "ok": s == 200,
            "got_data": isinstance(b, dict) and b.get("series_id") == "GDP",
        }

        # Revoke the key
        body = json.dumps({"action": "revoke", "key_hash": key_hash}).encode()
        req = urllib.request.Request(
            admin_url, data=body, method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {admin_token}"},
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            r.read()
        time.sleep(2)

        s, b = call(FRED_URL, query={"series": "GDP"},
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["fred"]["B6_revoked_key"] = {"status": s, "expected": 403, "ok": s == 403}

        # ─── Demo regression ───────────────────────────────────────
        out["demo"] = {}
        s, b = call(DEMO_URL, headers={"Origin": "https://justhodl.ai"})
        out["demo"]["C1_origin_only_strict"] = {
            "status": s, "expected": 401, "ok": s == 401,
        }

        out["all_passed"] = all([
            out["ecb"]["A1_no_key"]["ok"],
            out["ecb"]["A2_origin_only"]["ok"],
            out["ecb"]["A3_valid_key"]["ok"],
            out["fred"]["B1_no_key_no_origin"]["ok"],
            out["fred"]["B2_origin_bypass_main"]["ok"],
            out["fred"]["B3_evil_origin"]["ok"],
            out["fred"]["B4_referer_bypass"]["ok"],
            out["fred"]["B5_api_key"]["ok"],
            out["fred"]["B6_revoked_key"]["ok"],
            out["demo"]["C1_origin_only_strict"]["ok"],
        ])
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
