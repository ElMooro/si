"""1278 — dump ai-brief registry to see contexts + find bond-vol gap."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
out={}
try:
    reg=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="config/ai-brief-contexts.json")["Body"].read())
    ctxs=reg.get("contexts",{})
    out["n_contexts"]=len(ctxs)
    out["context_ids"]=list(ctxs.keys())
    out["output_keys"]={k:v.get("output_key") for k,v in ctxs.items()}
    # is bond-vol there?
    out["has_bond_vol"]=any("bond" in (v.get("output_key","")+k).lower() for k,v in ctxs.items())
    # sample one context's full config to learn the schema
    sample_k=list(ctxs.keys())[0] if ctxs else None
    out["sample_context"]={sample_k: ctxs.get(sample_k)} if sample_k else {}
except Exception as e: out["err"]=str(e)[:200]
# check if bond-vol-decisive-call.json exists
try:
    s3.head_object(Bucket="justhodl-dashboard-live",Key="data/bond-vol-decisive-call.json")
    out["bond_vol_brief_exists"]=True
except Exception: out["bond_vol_brief_exists"]=False
open("aws/ops/reports/1278_registry.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
