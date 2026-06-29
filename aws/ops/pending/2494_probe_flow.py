import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
print("===== etf-flows.json =====")
e=g("data/etf-flows.json"); print("keys:",list(e.keys()))
for fld in ["by_etf","rotation_in","rotation_out","heavy_inflow","by_category"]:
    v=e.get(fld)
    if isinstance(v,list) and v: print(f" .{fld}[0]:",json.dumps(v[0])[:300])
    elif isinstance(v,dict): 
        k0=list(v.keys())[:1]; print(f" .{fld} dict sample:",json.dumps({k0[0]:v[k0[0]]})[:300] if k0 else "{}")
print("\n===== flow-lookthrough.json =====")
f=g("data/flow-lookthrough.json"); print("keys:",list(f.keys()),"| thesis:",str(f.get("thesis"))[:120])
for fld in ["top_picks","inflow_leaders"]:
    v=f.get(fld)
    if isinstance(v,list) and v: print(f" .{fld}[0]:",json.dumps(v[0])[:320])
print("\n===== rotation-chains.json =====")
c=g("data/rotation-chains.json"); print("keys:",list(c.keys()),"| notes:",str(c.get("notes"))[:120])
for fld in ["top_next_up","chains","lag_candidates"]:
    v=c.get(fld)
    if isinstance(v,list) and v: print(f" .{fld}[0]:",json.dumps(v[0])[:340])
print("DONE 2494")
