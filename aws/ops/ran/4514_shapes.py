"""ops 4514 — print engine-provider-map RAW sample (v5 marriage) + canary
panel statuses."""
import gzip,json,os
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={}
def gj(k):
    b=s3.get_object(Bucket=B,Key=k)["Body"].read()
    return json.loads(gzip.decompress(b) if k.endswith(".gz") else b)
try:
    m=gj("data/audit/engine-provider-map.json")
    R["map_top_keys"]=list(m.keys())[:8] if isinstance(m,dict) else type(m).__name__
    it=(list(m.items())[:3] if isinstance(m,dict) else m[:2])
    R["map_sample"]=json.dumps(it,default=str)[:600]
except Exception as e: R["map_err"]=str(e)[:80]
try:
    cs=gj("data/warm/canary-macro-summary.json")
    R["canary_panels"]={k:(v.get("ok") or v.get("reason","")[:40]) for k,v in cs.items() if isinstance(v,dict)}
except Exception as e: R["canary_err"]=str(e)[:80]
try:
    h=gj("data/canary-macro.json")
    R["flags"]=h.get("flags")
    R["SAHM"]=(h.get("SAHMREALTIME") or {}).get("value")
except Exception as e: R["hot_err"]=str(e)[:60]
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4514_shapes.md","w").write("# ops 4514\n- map_top: "+json.dumps(R.get("map_top_keys"),default=str)+"\n- map_sample: "+str(R.get("map_sample"))+"\n- canary_panels: "+json.dumps(R.get("canary_panels"),default=str)[:500]+"\n- flags: "+json.dumps(R.get("flags"),default=str)+" SAHM="+str(R.get("SAHM"))+"\n")
json.dump(R,open("aws/ops/reports/4514_shapes.json","w"),indent=1,default=str)
print(json.dumps(R,default=str)[:400])
