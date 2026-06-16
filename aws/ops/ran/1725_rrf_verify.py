import json, time, boto3
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
before=""
try: before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/russell-recon-frontrun.json")["Body"].read()).get("generated_at","")
except Exception as e: print("pre:",str(e)[:50])
lam.invoke(FunctionName="justhodl-russell-recon-frontrun",InvocationType="Event")
d=None
for i in range(12):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/russell-recon-frontrun.json")["Body"].read())
        if d.get("generated_at")!=before: break
    except Exception: pass
if not d: print("no refresh"); raise SystemExit
import re
blob=json.dumps(d)
print("output has fv_in_r2000 tags:", "fv_in_r2000" in blob, "| recon_disagree:", "recon_disagree" in blob)
# count disagreements if a list of names is present
names=[]
for k,v in d.items():
    if isinstance(v,list):
        dis=[x for x in v if isinstance(x,dict) and x.get("recon_disagree")]
        if dis: print(f"  list '{k}': {len(dis)}/{len(v)} recon_disagree (rank-implied != Finviz truth)")
