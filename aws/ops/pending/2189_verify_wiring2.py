import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def ready(fn):
    for _ in range(25):
        c=lam.get_function(FunctionName=fn)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(3)
# 1) accumulation-radar v1.1 — country flow corroboration
ready("justhodl-accumulation-radar")
lam.invoke(FunctionName="justhodl-accumulation-radar",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/accumulation-radar.json")["Body"].read())
print("accum-radar v",d.get("version"))
for book in ("tops","bottoms","accumulating","distributing"):
    for r in (d.get(book,{}) or {}).get("countries",[])[:4]:
        if r.get("flow_confirm"):
            print(f"  {book:<12} {r['ticker']} ({r.get('label')}) {r['phase']} + hot-money {r.get('hot_money_conviction')} -> {r['flow_confirm']}")
# 2) best-setups cycle overlay
ready("justhodl-best-setups")
lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse")
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/best-setups.json")["Body"].read())
setups=b.get("setups") or b.get("best_setups") or []
tagged=[s for s in setups if s.get("cycle_phase")]
warned=[s for s in setups if s.get("cycle_warning")]
print(f"\nbest-setups: {len(setups)} setups, {len(tagged)} cycle-tagged, {len(warned)} with cycle_warning")
for s in tagged[:6]:
    print(f"  {s['ticker']:<6} {s['verdict']:<14} conv {s['conviction']} phase {s['cycle_phase']} flag {s.get('cycle_flag')} warn {s.get('cycle_warning')}")
print("DONE 2189")
