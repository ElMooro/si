"""ops 2896 — code-vintage census: how many fns redeployed since the 17:45 router fix; the two metrics fns' vintage."""
import os, json, boto3, traceback
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1")
R={"ops":2896,"ts":datetime.now(timezone.utc).isoformat()}
CUT="2026-07-05T17:45"
try:
    newer=0; total=0; sample_old=[]
    p=lam.get_paginator("list_functions")
    for pg in p.paginate():
        for f in pg["Functions"]:
            total+=1
            if f["LastModified"][:16]>=CUT: newer+=1
            elif f["FunctionName"] in ("justhodl-khalid-metrics","justhodl-ka-metrics","justhodl-consumer-pulse","justhodl-ai-brief-router","justhodl-debate-engine"):
                sample_old.append({"fn":f["FunctionName"],"mod":f["LastModified"][:16]})
    R["fleet"]={"total":total,"redeployed_since_fix":newer}
    R["key_fns_vintage"]=sample_old or "all-newer"
    for fn in ("justhodl-khalid-metrics","justhodl-ka-metrics"):
        c=lam.get_function_configuration(FunctionName=fn)
        R[fn]={"last_modified":c.get("LastModified")[:19],"note":"config-resize updates LastModified too — CodeSha vintage is the truth"}
        R[fn]["code_sha"]=c.get("CodeSha256","")[:12]
    R["status"]="OK"
except Exception:
    R["errors"]=traceback.format_exc()[-400:]; R["status"]="FAILED"
print(json.dumps(R,indent=1,default=str)[:1500])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2896_vintage.json","w"),indent=1,default=str)
print("OPS 2896 COMPLETE")
