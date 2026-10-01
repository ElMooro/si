"""
Ops verification: OpenFIGI API key in SSM + key validity + wiring.

Context: the OpenFIGI key was the one outstanding item for full-speed 9/10
(bond-CUSIP enrichment in justhodl-symbology-master falls back to anonymous
rate limits without it). Khalid confirmed on 2026-09-30 that the key is now
stored in AWS Parameter Store.

Verifies:
  1. SSM parameter /justhodl/openfigi/api-key exists, is a SecureString, non-empty
  2. The stored key is VALID: a real OpenFIGI v3 mapping call (AAPL -> ISIN)
     succeeds with the key and returns results
  3. (best-effort) aws/shared/openfigi.py imports and its keyed client path works

Writes report to s3://justhodl-dashboard-live/ops/reports/1055.json

Credential values are NEVER logged or written to the report.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
import urllib.error

import boto3

SSM_KEY_PATH = "/justhodl/openfigi/api-key"
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
HTTP_TIMEOUT = 30
S3_BUCKET = "justhodl-dashboard-live"
REPORT_KEY = "ops/reports/1055.json"
USER_AGENT = "JustHodl-Symbology/1.0"


def check_ssm(ssm):
    """Check the param exists, is SecureString, and is non-empty. Never returns the value."""
    detail = {"path": SSM_KEY_PATH}
    try:
        param = ssm.get_parameter(Name=SSM_KEY_PATH, WithDecryption=True)["Parameter"]
        detail["exists"] = True
        detail["type"] = param.get("Type")
        detail["length"] = len(param.get("Value") or "")
        detail["ok"] = detail["type"] == "SecureString" and detail["length"] > 0
    except ssm.exceptions.ParameterNotFound:
        detail["exists"] = False
        detail["ok"] = False
    except Exception as e:  # permissions / network failure
        detail["exists"] = False
        detail["ok"] = False
        detail["error"] = type(e).__name__
    return detail


def probe_openfigi(api_key):
    """Real OpenFIGI v3 mapping call with the stored key. Never raises."""
    detail = {"url": OPENFIGI_URL}
    payload = json.dumps([{"idType": "TICKER", "idValue": "AAPL"}]).encode("utf-8")
    req = urllib.request.Request(
        OPENFIGI_URL,
        data=payload,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "X-OPENFIGI-APIKEY": api_key,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            detail["http_status"] = getattr(resp, "status", None)
            body = json.loads(resp.read().decode("utf-8", errors="replace"))
            detail["parsed_ok"] = True
            results = body[0].get("data", []) if isinstance(body, list) and body else []
            detail["results"] = len(results)
            detail["sample_isin"] = results[0].get("figi") if results else None
            detail["valid"] = detail["results"] > 0
    except urllib.error.HTTPError as e:
        detail["http_status"] = e.code
        detail["parsed_ok"] = False
        detail["valid"] = False
        try:
            detail["body_preview"] = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:  # unreadable error body
            detail["body_preview"] = ""
    except Exception as e:  # URLError, timeout, JSON failure
        detail["http_status"] = None
        detail["parsed_ok"] = False
        detail["valid"] = False
        detail["error"] = type(e).__name__
    return detail


def probe_shared_module():
    """Best-effort: import aws/shared/openfigi and confirm keyed path exists. Never raises."""
    detail = {"openfigi_importable": False}
    try:
        here = os.path.abspath(__file__)
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(here))))
        shared_dir = os.path.join(repo_root, "aws", "shared")
        if shared_dir not in sys.path:
            sys.path.insert(0, shared_dir)
        import openfigi  # noqa: E402
        detail["openfigi_importable"] = True
        detail["has_keyed_client"] = any(
            kw in dir(openfigi) for kw in ("map_securities", "ticker_to_figi", "OpenFIGIClient"))
    except Exception as e:  # module missing or import-time failure
        detail["import_error"] = type(e).__name__
    return detail


def main():
    """Run the probes and write the JSON report to S3."""
    ssm = boto3.client("ssm", region_name="us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")

    report = {
        "script": "1055_verify_openfigi",
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ssm": {},
        "api_key_valid": None,
        "shared_module": {},
        "overall": "UNKNOWN",
    }

    # 1. SSM existence / type / non-empty
    ssm_detail = check_ssm(ssm)
    report["ssm"] = ssm_detail
    if not ssm_detail.get("ok"):
        report["overall"] = "FAIL_SSM_MISSING" if not ssm_detail.get("exists") else "FAIL_SSM_INVALID"
        _write_report(s3, report)
        return

    # 2. Key validity (value in memory only, never logged)
    api_key = ssm.get_parameter(Name=SSM_KEY_PATH, WithDecryption=True)["Parameter"]["Value"]
    probe = probe_openfigi(api_key)
    del api_key
    report["api_key_valid"] = probe.get("valid")
    report["openfigi_probe"] = {k: v for k, v in probe.items()}

    # 3. Shared-module wiring (best-effort)
    report["shared_module"] = probe_shared_module()

    # 4. Verdict
    if probe.get("valid") and report["shared_module"].get("openfigi_importable"):
        report["overall"] = "PASS"
    elif probe.get("valid"):
        report["overall"] = "PARTIAL_KEY_OK"
    else:
        report["overall"] = "FAIL_API"

    _write_report(s3, report)


def _write_report(s3, report):
    """Write the report to S3 and print it."""
    s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                  Body=json.dumps(report, indent=2).encode(),
                  ContentType="application/json")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
