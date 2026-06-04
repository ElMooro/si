"""1268 — capture etf-flows shape into report."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
out={}
for k in ["data/etf-flows.json","data/etf-fund-flows.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
        info={"top_keys":list(d.keys())[:15] if isinstance(d,dict) else f"LIST[{len(d)}]","arrays":{}}
        if isinstance(d,dict):
            for key,v in d.items():
                if isinstance(v,list) and v and isinstance(v[0],dict):
                    info["arrays"][key]={"len":len(v),"fields":list(v[0].keys())[:15],"sample":v[0]}
                elif isinstance(v,dict) and v:
                    fk=list(v.keys())[0]
                    if isinstance(v[fk],dict): info["arrays"][key+"(dict)"]={"fields":list(v[fk].keys())[:15],"sample":v[fk]}
        out[k]=info
    except Exception as e: out[k]={"error":str(e)[:100]}
open("aws/ops/reports/1268_etf_shape.json","w").write(json.dumps(out,indent=2,default=str))
print("done")
