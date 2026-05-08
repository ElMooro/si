#!/usr/bin/env python3
"""Diag — list all justhodl-push* Lambdas + check deploy-lambdas workflow run."""
import json, os
from datetime import datetime, timezone
import boto3
REPORT = "aws/ops/reports/360b_diag_lambda_state.json"
lam = boto3.client("lambda", region_name="us-east-1")

def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    # List all Lambdas matching push
    matching = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page["Functions"]:
            n = f["FunctionName"]
            if "push" in n.lower() or n == "justhodl-push-api":
                matching.append({
                    "name": n, "runtime": f.get("Runtime"),
                    "last_modified": f.get("LastModified"),
                    "code_size": f.get("CodeSize"),
                    "role": f.get("Role"),
                })
    out["matching_lambdas"] = matching
    # Try get_function specifically for the expected name
    try:
        info = lam.get_function(FunctionName="justhodl-push-api")
        out["push_api_exists"] = True
        out["push_api_state"] = info["Configuration"].get("State")
        out["push_api_last_modified"] = info["Configuration"].get("LastModified")
    except lam.exceptions.ResourceNotFoundException:
        out["push_api_exists"] = False
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
