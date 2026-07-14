"""ops 3308b — sentinel recon: sync-invoke for the exact error + dump
what data/_alerts/last.json actually contains (key list)."""
import json
import sys

import boto3
from botocore.config import Config

from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))

with report("3308b_sentinel_diag") as rep:
    r = LAM.invoke(FunctionName="justhodl-alert-sentinel",
                   InvocationType="RequestResponse", Payload=b"{}")
    rep.kv(function_error=r.get("FunctionError"),
           status=r.get("StatusCode"))
    rep.log("PAYLOAD: %s"
            % r["Payload"].read().decode("utf-8", "replace")[:2500])
    try:
        st = json.loads(S3.get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/_alerts/last.json")["Body"].read())
        rep.kv(last_json_type=type(st).__name__,
               last_json_keys=sorted(st.keys())[:60]
               if isinstance(st, dict) else str(st)[:200])
    except Exception as e:
        rep.log("last.json read failed: %s" % str(e)[:150])
sys.exit(0)
