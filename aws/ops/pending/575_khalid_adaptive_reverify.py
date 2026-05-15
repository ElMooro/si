#!/usr/bin/env python3
"""575 — Re-invoke justhodl-khalid-adaptive after defensive None fix.
Capture sidecar + log."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/575_khalid_adaptive_reverify.json"
NAME = "justhodl-khalid-adaptive"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for redeploy
    for i in range(20):
        try:
            cfg = lam.get_function(FunctionName=NAME)["Configuration"]
            out["lambda_last_modified"] = cfg.get("LastModified")
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
        except Exception: pass
        _time.sleep(6)

    _time.sleep(3)

    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:500]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_tail"] = log[-2500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/khalid-adaptive.json")
        out["sidecar"] = json.loads(obj["Body"].read())
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
