import boto3
ssm=boto3.client("ssm",region_name="us-east-1")
print("=== SSM params containing 'finviz' (any case) ===")
found=[]
p=ssm.get_paginator("describe_parameters")
for pg in p.paginate():
    for par in pg.get("Parameters",[]):
        if "finviz" in par["Name"].lower(): found.append(par["Name"])
print(found or "NONE — no Finviz token stored in SSM")
print("\n=== all SSM param names (for context, to see key-naming convention) ===")
names=[]
for pg in ssm.get_paginator("describe_parameters").paginate():
    names+=[par["Name"] for par in pg.get("Parameters",[])]
for n in sorted(names): print("  ",n)
