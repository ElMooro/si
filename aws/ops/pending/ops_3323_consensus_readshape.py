"""ops 3323 — read actual analyst-consensus.json shape to confirm the FMP
repair populated grade-change + beat data (3322 used wrong field names)."""
import json
from pathlib import Path
import boto3
from ops_report import report
S3=boto3.client("s3","us-east-1")
def j(k):
    try: return json.loads(S3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)}
with report("3323_consensus_readshape") as rep:
    d=j("data/analyst-consensus.json")
    rep.kv(top_level_keys=list(d.keys()))
    # scan every list-of-dicts for upgrade/beat evidence
    for key,val in d.items():
        if isinstance(val,list) and val and isinstance(val[0],dict):
            f=list(val[0].keys())
            rep.kv(**{f"list::{key}": {"n":len(val),"fields":f[:14],"sample":val[0]}})
    rep.kv(RESULT="DONE")
