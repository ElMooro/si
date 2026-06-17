import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
def num(v):
    try:
        if isinstance(v,bool):return None
        return float(v)
    except:return None
for feed,grp in [("crisis-canaries.json","canaries"),("crisis-canaries.json","families"),("funding-plumbing.json","signals")]:
    o=json.loads(s3.get_object(Bucket=B,Key="data/"+feed)["Body"].read())
    g=o.get(grp,{})
    print(f"\n{feed}.{grp}: {len(g)} members")
    for k,v in list(g.items())[:3]:
        if isinstance(v,dict):
            nums={kk:round(num(v[kk]),3) for kk in v if num(v[kk]) is not None}
            print(f"  {k}: nums={nums} strKeys={[kk for kk in v if isinstance(v[kk],str)][:4]}")
    # top-level scalars
    print("  TOPLEVEL nums:", {k:round(num(o[k]),3) for k in o if num(o[k]) is not None})
