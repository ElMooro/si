#!/usr/bin/env python3
"""Step 293 — Phase 2C verification: 4 Tier B analysis Lambdas.

Same matrix as 290/291/292 with 30s settle delay.

Lambdas verified:
  justhodl-stock-analyzer    (Origin-bypass)  — uses /stock/index.html
  justhodl-options-flow      (Origin-bypass)  — uses flow.html
  justhodl-charts-agent      (STRICT mode)    — no frontend callers
  justhodl-edge-engine       (Origin-bypass)  — uses edge.html

For each, 6 assertions (24 total). Strict-mode Lambdas only get the
no-bypass version: no key→401, valid key→200, revoked→403.
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

URLS = {
    "stock-analyzer": "https://enxmdjjowjfwiydpslyykokjee0qdvml.lambda-url.us-east-1.on.aws/",
    "options-flow":   "https://g65enkjk3uu4woaoy764ow3q340sppvg.lambda-url.us-east-1.on.aws/",
    "charts-agent":   "https://wehli6nf3a6rq575td5w6jk7ii0yptqg.lambda-url.us-east-1.on.aws/",
    "edge-engine":    "https://vsxv2775x5aojiuwaoqb7wipam0rmuln.lambda-url.us-east-1.on.aws/",
}
QUERIES = {
    # Use lightweight queries that won't hit external APIs heavily
    "stock-analyzer": {"ticker": "AAPL"},
    "options-flow":   None,  # GET / typically returns help/index
    "charts-agent":   {"type": "line", "indicator": "sp500"},
    "edge-engine":    None,
}
STRICT_MODE = {"charts-agent": True}   # no Origin-bypass

REPORT_PATH = "aws/ops/reports/293_phase2c_verification.json"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def call(url, headers=None, query=None, timeout=30):
    if query:
        url = url.rstrip("/") + "?" + urlencode(query)
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


def issue_pro_key(admin_url, admin_token, label):
    body = json.dumps({
        "action": "create", "tier": "PRO",
        "owner_email": "phase2c@justhodl.ai", "label": label,
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


def test_lambda(name, url, query, strict, admin_url, admin_token):
    out = {"strict": strict}

    kd = issue_pro_key(admin_url, admin_token, f"phase2c-{name}")
    plain_key = kd["key"]
    key_hash = kd["key_hash"]

    if strict:
        # Strict-mode: no Origin bypass. Test 3 assertions: no-key, valid, revoked.
        s, b = call(url, query=query)
        out["strict_no_key"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call(url, query=query,
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["strict_valid_key"] = {"status": s, "expected": 200, "ok": s == 200}

        revoke_key(admin_url, admin_token, key_hash)
        time.sleep(1)
        s, b = call(url, query=query,
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["strict_revoked"] = {"status": s, "expected": 403, "ok": s == 403}

        out["all_passed"] = (out["strict_no_key"]["ok"]
                             and out["strict_valid_key"]["ok"]
                             and out["strict_revoked"]["ok"])
    else:
        # Origin-bypass mode: full 6-assertion matrix.
        s, b = call(url, query=query)
        out["X1_no_key_no_origin"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call(url, query=query,
                    headers={"Origin": "https://justhodl.ai"})
        out["X2_origin_bypass"] = {"status": s, "expected": 200, "ok": s == 200}

        s, b = call(url, query=query,
                    headers={"Origin": "https://evil.com"})
        out["X3_evil_origin"] = {"status": s, "expected": 401, "ok": s == 401}

        s, b = call(url, query=query,
                    headers={"Referer": "https://justhodl.ai/somepage.html"})
        out["X4_referer_bypass"] = {"status": s, "expected": 200, "ok": s == 200}

        s, b = call(url, query=query,
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["X5_api_key"] = {"status": s, "expected": 200, "ok": s == 200}

        revoke_key(admin_url, admin_token, key_hash)
        time.sleep(1)
        s, b = call(url, query=query,
                    headers={"Authorization": f"Bearer {plain_key}"})
        out["X6_revoked"] = {"status": s, "expected": 403, "ok": s == 403}

        out["all_passed"] = all(
            out[k].get("ok") for k in
            ["X1_no_key_no_origin", "X2_origin_bypass", "X3_evil_origin",
             "X4_referer_bypass", "X5_api_key", "X6_revoked"]
        )

    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        admin_url = lam.get_function_url_config(FunctionName=ADMIN_LAMBDA)["FunctionUrl"]
        admin_token = ssm.get_parameter(
            Name=ADMIN_TOKEN_SSM, WithDecryption=True
        )["Parameter"]["Value"]

        # Settle delay (in addition to the 60s the bash already slept)
        time.sleep(15)

        for name, url in URLS.items():
            print(f"[293] testing {name}…")
            out[name] = test_lambda(
                name, url,
                QUERIES.get(name),
                STRICT_MODE.get(name, False),
                admin_url, admin_token,
            )

        out["all_passed"] = all(
            out[name].get("all_passed", False) for name in URLS.keys()
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
