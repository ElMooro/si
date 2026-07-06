import json,boto3,os
from datetime import datetime,timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3")
BUCKET="justhodl-dashboard-live"; out={"ts":datetime.now(timezone.utc).isoformat(),"engines":{}}
for fn,s3k in [("justhodl-investor-lenses","data/investor-lenses/AAPL.json"),
               ("justhodl-technical-overlays","data/technical-overlays/AAPL.json")]:
    e={}
    c=lam.get_function_configuration(FunctionName=fn)
    e["state"]=c["State"]; e["runtime"]=c["Runtime"]; e["timeout"]=c["Timeout"]; e["memory"]=c["MemorySize"]
    e["env_keys"]=sorted((c.get("Environment",{}).get("Variables",{}) or {}).keys())
    e["last_modified"]=c["LastModified"]
    # schedule?
    try:
        ev=boto3.client("events",region_name="us-east-1")
        rules=ev.list_rule_names_by_target(TargetArn=c["FunctionArn"]) if False else None
    except: pass
    h=s3.head_object(Bucket=BUCKET,Key=s3k)
    e["s3_bytes"]=h["ContentLength"]; e["s3_key"]=s3k
    out["engines"][fn]=e
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(out,open("aws/ops/reports/final_status.json","w"),indent=2,default=str)
print(json.dumps(out,indent=2,default=str))
