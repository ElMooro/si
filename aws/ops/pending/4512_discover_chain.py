"""ops 4512 — (a) print the TRUE shapes of rollup + lambda-graph (for the
v4 feed->provider marriage), (b) runpy the canary v2 op inline."""
import gzip,json,os,runpy,traceback
from datetime import datetime,timezone
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":4512,"as_of":datetime.now(timezone.utc).isoformat()}
def gj(k):
    b=s3.get_object(Bucket=B,Key=k)["Body"].read()
    return json.loads(gzip.decompress(b) if k.endswith(".gz") else b)
def shape(o,depth=0):
    if depth>2: return type(o).__name__
    if isinstance(o,dict):
        ks=list(o.keys())[:6]
        return {k:shape(o[k],depth+1) for k in ks} | ({"__more__":len(o)} if len(o)>6 else {})
    if isinstance(o,list):
        return ["len="+str(len(o)), shape(o[0],depth+1) if o else "empty"]
    return type(o).__name__
try:
    r=gj("data/audit/data-source-rollup.json")
    R["rollup_shape"]=shape(r)
except Exception as e: R["rollup_err"]=str(e)[:80]
try:
    ls=s3.list_objects_v2(Bucket=B,Prefix="data/audit/",MaxKeys=60)
    R["audit_keys"]=[o["Key"] for o in ls.get("Contents",[]) if "graph" in o["Key"] or "lambda" in o["Key"] or "engine" in o["Key"]][:10]
except Exception as e: R["audit_err"]=str(e)[:60]
for cand in ("data/audit/lambda-graph.json","data/audit/engine-graph.json","data/lambda-graph.json"):
    try:
        g=gj(cand)
        R["graph_key"]=cand; R["graph_shape"]=shape(g)
        if isinstance(g,dict):
            for k,v in list(g.items())[:1]:
                if isinstance(v,dict): R["graph_engine_sample_keys"]=list(v.keys())[:12]
                elif isinstance(v,list) and v and isinstance(v[0],dict):
                    R["graph_engine_sample_keys"]=list(v[0].keys())[:12]
        break
    except Exception: continue
try:
    runpy.run_path("aws/ops/pending/4510_canary_v2.py",run_name="__main__")
    R["canary_chain"]="OK"
except SystemExit as e: R["canary_chain"]=f"exit {e.code}"
except Exception as e:
    R["canary_chain"]=f"{type(e).__name__}: {str(e)[:120]}"; traceback.print_exc()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4512_discover.json","w"),indent=1,default=str)
open("aws/ops/reports/4512_discover.md","w").write("# ops 4512 — discover+chain\n- rollup: "+json.dumps(R.get("rollup_shape"),default=str)[:600]+"\n- graph_key: "+str(R.get("graph_key"))+"\n- graph: "+json.dumps(R.get("graph_shape"),default=str)[:400]+"\n- engine_keys: "+json.dumps(R.get("graph_engine_sample_keys"))+"\n- audit: "+json.dumps(R.get("audit_keys"))+"\n- canary_chain: "+str(R.get("canary_chain"))+"\n")
print(json.dumps({k:R.get(k) for k in ("graph_key","graph_engine_sample_keys","canary_chain")},default=str))
