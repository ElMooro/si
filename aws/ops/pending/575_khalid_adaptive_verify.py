#!/usr/bin/env python3
"""575 — Invoke khalid-adaptive after defensive None fix, verify sidecar."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/575_khalid_adaptive_verify.json"
NAME = "justhodl-khalid-adaptive"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    for i in range(15):
        try:
            cfg = lam.get_function(FunctionName=NAME)["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["lambda_last_modified"] = cfg.get("LastModified")
                break
        except Exception: pass
        _time.sleep(6)

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
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2200:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    _time.sleep(3)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/khalid-adaptive.json")
        body = obj["Body"].read()
        out["sidecar_size_kb"] = round(len(body)/1024, 2)
        out["sidecar"] = json.loads(body)
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
