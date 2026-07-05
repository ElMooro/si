"""Find which live Lambdas actually have FMP_KEY / POLYGON_KEY / FRED_KEY populated."""
import boto3, json
lam=boto3.client("lambda",region_name="us-east-1")
NEED=["FMP_KEY","POLYGON_KEY","FRED_KEY"]
found={k:[] for k in NEED}
paginator=lam.get_paginator("list_functions")
n=0
for page in paginator.paginate():
    for f in page["Functions"]:
        fn=f["FunctionName"]
        if not fn.startswith("justhodl-"): continue
        n+=1
        env=(f.get("Environment",{}) or {}).get("Variables",{}) or {}
        for k in NEED:
            if env.get(k): found[k].append(fn)
out={"scanned":n}
for k in NEED:
    out[k]={"count":len(found[k]),"sample":found[k][:5]}
    # a function that has ALL three is ideal
have_all=[f for f in found["FMP_KEY"] if f in found["POLYGON_KEY"] and f in found["FRED_KEY"]]
out["has_FMP_POLYGON_FRED"]=have_all[:5]
print(json.dumps(out,indent=2))
import os; os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(out,open("aws/ops/reports/find_key_source.json","w"),indent=2)
