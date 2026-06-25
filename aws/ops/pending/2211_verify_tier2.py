import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def ready(fn):
    for _ in range(28):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
ready("justhodl-equity-confluence"); lam.invoke(FunctionName="justhodl-equity-confluence",InvocationType="RequestResponse")
e=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/equity-confluence.json")["Body"].read())
fams=sorted({s.get("family") for s in (e.get("sources") or [])})
print("equity-confluence families now:", fams)
print("  has convexity:", "convexity" in fams)
ready("justhodl-accumulation-radar"); lam.invoke(FunctionName="justhodl-accumulation-radar",InvocationType="RequestResponse")
a=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/accumulation-radar.json")["Body"].read())
mc=a.get("market_context") or {}
print(f"accum market_context: washout={mc.get('market_washout')} cap_signal={mc.get('capitulation_signal')} cap_score={mc.get('capitulation_score')} consensus_names={mc.get('consensus_bottom_names')}")
cb=[r for cl in ("stocks","etfs") for r in (a.get("bottoms",{}) or {}).get(cl,[]) if r.get("consensus_bottom_confirm")]
print(f"bottoms with consensus_bottom_confirm: {len(cb)}")
print("DONE 2211")
