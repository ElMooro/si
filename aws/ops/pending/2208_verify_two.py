import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=400,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def ready(fn):
    for _ in range(25):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
ready("justhodl-flow-confluence"); lam.invoke(FunctionName="justhodl-flow-confluence",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-confluence.json")["Body"].read())
bb=[r for r in (d.get("multi_engine_confluence") or []) for t in (r.get("tags") or []) if "buyback yield" in t]
print(f"flow-confluence buyback-tagged (multi-engine): {len(bb)}")
for r in bb[:4]: print(f"  {r['ticker']} posture {r.get('posture')} tags={[t for t in r.get('tags',[]) if 'buyback' in t]}")
ready("justhodl-accumulation-radar"); lam.invoke(FunctionName="justhodl-accumulation-radar",InvocationType="RequestResponse")
a=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/accumulation-radar.json")["Body"].read())
sf=[]
for cl in ("stocks","etfs","countries"):
    for r in (a.get("bottoms",{}) or {}).get(cl,[]): 
        if r.get("squeeze_fuel"): sf.append(r)
    for r in (a.get("accumulating",{}) or {}).get(cl,[]):
        if r.get("squeeze_fuel"): sf.append(r)
print(f"accumulation-radar squeeze_fuel names: {len(sf)}")
for r in sf[:5]: print(f"  {r['ticker']} bottom {r.get('bottom_score')} short% {r.get('short_pct')} dtc {r.get('days_to_cover')}")
print("DONE 2208")
