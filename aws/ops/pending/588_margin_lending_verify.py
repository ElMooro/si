#!/usr/bin/env python3
"""588 — Verify margin-lending FRED ID fixes by force-invoking after CI/CD redeploy."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/588_margin_lending_verify.json"
NAME = "justhodl-margin-lending"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for CI/CD to redeploy
    pre_mod = None
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        pre_mod = cfg.get("LastModified")
    except Exception: pass
    out["pre_modified"] = pre_mod

    for i in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=NAME)
            mod = cfg.get("LastModified")
            state = cfg.get("State")
            status = cfg.get("LastUpdateStatus")
            if mod != pre_mod and state == "Active" and status == "Successful":
                out["new_modified"] = mod
                break
        except Exception: pass
        _time.sleep(8)

    _time.sleep(3)

    # Force invoke
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:300]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            # Extract FRED-related lines
            fred_lines = [l for l in log.split("\n")
                            if "fred" in l.lower() or "FRED" in l or "BOGZ" in l
                            or "margin" in l.lower() or "SOFR" in l]
            out["fred_log_lines"] = fred_lines[:30]
            out["log_tail"] = log[-1800:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/margin-lending.json")
        body = obj["Body"].read()
        out["sidecar"] = json.loads(body)
        out["sidecar_size_kb"] = round(len(body)/1024, 1)
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
