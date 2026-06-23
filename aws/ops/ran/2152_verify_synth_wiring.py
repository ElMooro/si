import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-best-setups")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
sw=b.get("synthesizer_wiring",{})
print("synthesizer_wiring:",json.dumps(sw))
print("\nALIGNED BULLISH (options + flow agree):")
for s in b.get("synth_aligned_bullish",[])[:8]:
    print(f"   {s['ticker']:<6} conv={s.get('conviction')} options={s.get('options')} flow={s.get('flow')}")
print("\nCONFLICTED (positioning vs institutional money):")
for s in b.get("synth_conflicted",[])[:8]:
    print(f"   {s['ticker']:<6} conv={s.get('conviction')} options={s.get('options')} flow={s.get('flow')}")
print("DONE 2152")
