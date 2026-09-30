"""
Ops verification: FINRA Gateway credentials.

Verifies:
  1. SSM parameters /justhodl/finra/client_id and /justhodl/finra/client_secret exist
  2. OAuth2 client-credentials token request succeeds
  3. A test FINRA Data API call succeeds (treasuryDailyAggregates metadata)

Writes report to s3://justhodl-dashboard-live/ops/reports/finra-verify.json

Credential values are NEVER logged or written to the report.
"""
from __future__ import annotations

import base64
import json
import time
import urllib.request
import urllib.error

import boto3

SSM_CLIENT_ID = "/justhodl/finra/client_id"
SSM_CLIENT_SECRET = "/justhodl/finra/client_secret"
TOKEN_URL = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token"
API_BASE = "https://api.finra.org/data/group/fixedIncomeMarket"
S3_BUCKET = "justhodl-dashboard-live"
REPORT_KEY = "ops/reports/finra-verify.json"


def check_ssm(ssm):
    """Check both SSM params exist. Returns (ok, detail). Never returns values."""
    results = {}
    for name in (SSM_CLIENT_ID, SSM_CLIENT_SECRET):
        try:
            resp = ssm.get_parameter(Name=name, WithDecryption=True)
            val = resp["Parameter"]["Value"]
            # Only record existence and length, never the value
            results[name] = {"exists": True, "length": len(val) if val else 0}
        except ssm.exceptions.ParameterNotFound:
            results[name] = {"exists": False, "length": 0}
        except Exception as e:
            results[name] = {"exists": False, "error": type(e).__name__}
    ok = all(r.get("exists") and r.get("length", 0) > 0 for r in results.values())
    return ok, results


def get_token(client_id, client_secret):
    """OAuth2 client-credentials flow. Returns (token, error)."""
    creds = f"{client_id}:{client_secret}"
    b64 = base64.b64encode(creds.encode()).decode()
    data = b"grant_type=client_credentials"
    req = urllib.request.Request(
        TOKEN_URL,
        data=data,
        headers={
            "Authorization": f"Basic {b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode())
            token = body.get("access_token")
            if token:
                return token, None
            return None, "no access_token in response"
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}"
    except Exception as e:
        return None, type(e).__name__


def test_api_call(token):
    """Test a real FINRA Data API call. Returns (ok, detail)."""
    # Try treasuryDailyAggregates metadata (public, documented)
    url = f"{API_BASE}/name/treasuryDailyAggregates"
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode())
            # Metadata returns field definitions
            n_fields = len(body) if isinstance(body, list) else 0
            return True, {"endpoint": "treasuryDailyAggregates/metadata",
                          "fields_returned": n_fields}
    except urllib.error.HTTPError as e:
        return False, {"endpoint": "treasuryDailyAggregates/metadata",
                       "error": f"HTTP {e.code}"}
    except Exception as e:
        return False, {"error": type(e).__name__}


def main():
    ssm = boto3.client("ssm", region_name="us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")

    report = {
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ssm": {},
        "oauth": {},
        "api_test": {},
        "overall": "UNKNOWN",
    }

    # 1. SSM check
    ssm_ok, ssm_detail = check_ssm(ssm)
    # Strip lengths too - only keep exists flags in report
    report["ssm"] = {
        k: {"exists": v.get("exists", False)}
        for k, v in ssm_detail.items()
    }
    if not ssm_ok:
        report["overall"] = "FAIL_SSM_MISSING"
        s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                      Body=json.dumps(report, indent=2).encode(),
                      ContentType="application/json")
        print(json.dumps(report, indent=2))
        return

    # 2. Get actual values for OAuth (in memory only, never logged)
    client_id = ssm.get_parameter(Name=SSM_CLIENT_ID, WithDecryption=True)["Parameter"]["Value"]
    client_secret = ssm.get_parameter(Name=SSM_CLIENT_SECRET, WithDecryption=True)["Parameter"]["Value"]

    token, oauth_err = get_token(client_id, client_secret)
    # Clear from memory
    del client_id, client_secret

    if token:
        report["oauth"] = {"token_obtained": True}
    else:
        report["oauth"] = {"token_obtained": False, "error": oauth_err}
        report["overall"] = "FAIL_OAUTH"
        s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                      Body=json.dumps(report, indent=2).encode(),
                      ContentType="application/json")
        print(json.dumps(report, indent=2))
        return

    # 3. Test API call
    api_ok, api_detail = test_api_call(token)
    del token  # clear from memory
    report["api_test"] = api_detail

    report["overall"] = "PASS" if api_ok else "FAIL_API"
    s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                  Body=json.dumps(report, indent=2).encode(),
                  ContentType="application/json")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
