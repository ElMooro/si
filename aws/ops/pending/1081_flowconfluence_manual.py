"""
Ops 1081: manually deploy flow-confluence v1.3 with proper bundling.

Replicates the GitHub workflow's bundling:
 - aws/shared/*.py (all shared modules)
 - aws/lambdas/justhodl-flow-confluence/source/* (lambda source)

Then updates the lambda function code via boto3.

Writes: aws/ops/reports/1081_flowconfluence_manual.json. Never raises.
"""
from __future__ import annotations

import io
import json
import os
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1081_flowconfluence_manual.json")
REGION = "us-east-1"
FUNCTION = "justhodl-flow-confluence"
REPO = "https://raw.githubusercontent.com/ElMooro/si/main"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402

lam = boto3.client("lambda", region_name=REGION)


def fetch(url):
    """Fetch a URL. Returns bytes or None."""
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return r.read()
    except Exception:
        return None


def main():
    """Bundle and deploy. Write report."""
    rep = {"script": "1081_flowconfluence_manual",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "bundled": []}
    try:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            # Lambda source
            src = fetch(f"{REPO}/aws/lambdas/justhodl-flow-confluence/source/lambda_function.py")
            if not src or b'VERSION = "1.3"' not in src:
                rep["status"] = "SKIP: v1.3 source not found on main"
                raise ValueError("no v1.3")
            z.writestr("lambda_function.py", src)
            rep["source_bytes"] = len(src)
            rep["bundled"].append("lambda_function.py")

            # Shared modules (like the GitHub workflow does)
            # Fetch the list of shared .py files
            # We know the key ones from the imports; fetch them
            shared_mods = [
                "holdings_derived_boundary", "capital_research_boundary",
                "holdings_authority", "offexchange_context",
                "short_interest_context", "short_volume_context",
                "provider_flow_research", "option_scanner_boundary",
                "ticker_360",
            ]
            for mod in shared_mods:
                data = fetch(f"{REPO}/aws/shared/{mod}.py")
                if data:
                    z.writestr(f"{mod}.py", data)
                    rep["bundled"].append(f"{mod}.py")
        buf.seek(0)
        rep["zip_bytes"] = len(buf.getvalue())

        resp = lam.update_function_code(
            FunctionName=FUNCTION,
            ZipFile=buf.getvalue(),
            Publish=True)
        rep["status"] = "DEPLOYED"
        rep["version"] = resp.get("Version")
        rep["last_modified"] = resp.get("LastModified")
    except ClientError as e:
        rep["status"] = "AWS_ERROR"
        rep["error"] = str(e)[:200]
    except Exception as e:  # noqa: BLE001
        if "status" not in rep:
            rep["status"] = "ERROR"
            rep["error"] = str(e)[:200]

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2))


if __name__ == "__main__":
    main()
