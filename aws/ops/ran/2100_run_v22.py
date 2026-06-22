import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=600,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-resilience")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time()
print("invoke:",lam.invoke(FunctionName="justhodl-resilience",InvocationType="RequestResponse")["Payload"].read().decode()[:260],f"({time.time()-t:.0f}s)")
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/resilience.json")["Body"].read())
print("v",d.get("version"),"| counts:",d.get("counts"))
for x in d.get("diagnostics",[]):
    if "dark-pool" in x or "catalysts" in x or "universe" in x: print("  DIAG:",x)
print("\n🚀 ABOUT TO BOOM (flow-confirmed + idio first):")
for r in d.get("about_to_boom",[])[:14]:
    fl="📥F" if r.get("flow_confirmed") else "  "
    idio="⭐" if r.get("has_idiosyncratic_evidence") else " "
    ig=r.get("ignition")
    igs=f"IGNITE {ig['bars_ago']}d ago volz{ig['vol_z']} rx{ig['range_x']}{' CATALYST' if ig.get('on_catalyst') else ''}" if ig else ""
    print(f"  {r['ticker']:<6}{fl}{idio} {r['stage']:<9} res{r['resilience']} flow{r.get('flow_score')} | {r['abnormal_basis']:<9} OBV{r.get('obv_net_w')} AD{r.get('ad_net_w')} dp:{r.get('dark_pool_state')} | {igs}")
print("\n=== VALIDATION ===")
ign=[r for r in d.get("all_resilient",[]) if r["stage"]=="IGNITING"]
coil=[r for r in d.get("all_resilient",[]) if r["stage"]=="COILED"]
print(f"IGNITING {len(ign)}: all have ignition trigger? {all(r.get('ignition') for r in ign)}")
print(f"COILED {len(coil)}: all WITHOUT ignition (pure setup)? {all(not r.get('ignition') for r in coil)}")
print("flow-confirmed in boom:",sum(1 for r in d.get('about_to_boom',[]) if r.get('flow_confirmed')),"/",len(d.get('about_to_boom',[])))
dpset=[r for r in d.get("all_resilient",[]) if r.get("dark_pool_state")]
print("names with dark-pool join:",len(dpset),"e.g.",[(r['ticker'],r['dark_pool_state']) for r in dpset[:6]])
# page
def get(u):
    for _ in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r: return r.getcode(),r.read().decode("utf-8","replace")
        except Exception: time.sleep(12)
    return None,""
c1,b1=get("https://justhodl.ai/resilience.html?t="+str(int(time.time())))
print("\nresilience.html:",c1,"| flow badge code:", 'flow-confirmed' in b1, "| ignition code:", 'Ignition' in b1)
print("DONE 2100")
