import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
def num(v):
    try:
        if isinstance(v,bool):return None
        return float(v)
    except:return None
def show(feed):
    try:o=json.loads(s3.get_object(Bucket=B,Key="data/"+feed)["Body"].read())
    except Exception as e:print(f"{feed}: ERR {e.__class__.__name__}");return
    print(f"\n=== {feed} ===")
    if isinstance(o,dict):
        for k,v in o.items():
            if isinstance(v,dict):
                sc={kk:round(num(v[kk]),3) for kk in v if num(v[kk]) is not None}
                sub=[kk for kk in v if isinstance(v[kk],dict)]
                print(f"  {k}: scalars={sc} subdicts={sub[:6]}")
            elif isinstance(v,list) and v and isinstance(v[0],dict):
                e=v[0]; print(f"  {k}[{len(v)}] keys={list(e.keys())} nums={[kk for kk in e if num(e[kk]) is not None][:8]}")
for f in ["crisis-canaries.json","funding-plumbing.json","insider-radar.json","dislocations.json","implied-prob.json"]:
    show(f)
