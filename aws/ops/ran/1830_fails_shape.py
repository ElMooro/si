import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/settlement-fails.json")["Body"].read())
print("TOP KEYS:", list(d.keys()))
def show(o,p="",depth=0):
    if depth>3: return
    if isinstance(o,dict):
        for k,v in o.items():
            if isinstance(v,(dict,list)): 
                print("  "*depth+f"{k}: {type(v).__name__}({len(v)})")
                show(v,p+k+".",depth+1)
            else:
                kl=str(k).lower()
                if any(s in kl for s in ["pctile","percentile","regime","latest","value","level","total","headline","ustet","combined","z","score","label","series"]):
                    print("  "*depth+f"{k} = {str(v)[:60]}")
    elif isinstance(o,list) and o:
        print("  "*depth+f"[0]:"); show(o[0],p,depth+1)
show(d)
