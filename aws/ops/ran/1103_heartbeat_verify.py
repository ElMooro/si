"""Ops 1103: verify feed-heartbeat Lambda deployed and trigger first run."""
from __future__ import annotations
import json, os, sys
from datetime import datetime, timezone
OUT = os.path.join("aws", "ops", "reports", "1103_heartbeat_verify.json")
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "shared"))
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
def main():
    rep = {"read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-feed-heartbeat")
        rep["lambda_exists"] = True
        rep["memory"] = cfg.get("MemorySize")
        rep["timeout"] = cfg.get("Timeout")
        r = lam.invoke(FunctionName="justhodl-feed-heartbeat",
                       InvocationType="Event", Payload=json.dumps({}))
        rep["triggered"] = r["StatusCode"] == 202
    except Exception as e:
        rep["lambda_exists"] = False
        rep["error"] = str(e)[:120]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT,"w").write(json.dumps(rep,indent=2))
    print(json.dumps(rep))
if __name__=="__main__":
    main()
