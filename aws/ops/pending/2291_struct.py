import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
print("top-level type:", type(d).__name__)
if isinstance(d,dict):
    print("top keys:", list(d.keys())[:20])
    for k,v in d.items():
        if isinstance(v,dict):
            # is this a ticker->record map?
            sub=[kk for kk,vv in v.items() if isinstance(vv,dict) and (vv.get('name') or vv.get('fwd_val') or vv.get('pe'))]
            if sub:
                print(f"  '{k}' looks like candidate map: {len(sub)} records, sample {sub[:5]}")
        if isinstance(v,list) and v and isinstance(v[0],dict):
            print(f"  '{k}' is list[{len(v)}], keys0={list(v[0].keys())[:8]}")
# find LDOS wherever it is
import json as J
def walk(o,path=""):
    if isinstance(o,dict):
        if o.get("name") and o.get("fwd_val"):
            yield path,o
        for kk,vv in o.items(): yield from walk(vv,path+"/"+str(kk))
for path,rec in walk(d):
    if "LDOS" in path or (rec.get("name") or "").startswith("Leidos"):
        fv=rec["fwd_val"]
        print("FOUND LDOS at",path)
        print("  ",J.dumps(fv))
        break
print("DONE 2291")
