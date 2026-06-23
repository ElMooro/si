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
atw=b.get("alpha_trust_wiring",{})
print("alpha_trust_wiring:")
print("  proven_squeeze_lift (eff_trust):",atw.get("proven_squeeze_lift"))
print("  n_signals_lifted:",atw.get("n_signals_lifted"),"| n_signals_pruned:",atw.get("n_signals_pruned"))
print("  lifted sample:",atw.get("lifted_sample",[])[:5])
print("  pruned sample:",atw.get("pruned_sample",[])[:5])
# show a lifted pick's signal with alpha_trust stamped
shown=0
for s in (b.get("top_setups") or b.get("setups") or b.get("strong_buys") or [])[:60]:
    for sg in (s.get("signals") or []):
        if isinstance(sg,dict) and sg.get("alpha_trust"):
            print(f"  e.g. {s.get('ticker')}: {sg['key']} -> {sg['alpha_trust']}"); shown+=1; break
    if shown>=4: break
print("DONE 2145")
