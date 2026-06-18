import json, time, boto3
from botocore.config import Config
REGION="us-east-1"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":3}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
def wait(fn):
    for _ in range(30):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return
        time.sleep(5)
# 1) eurodollar-plumbing → HK hub from HKMA
wait("justhodl-eurodollar-plumbing")
lam.invoke(FunctionName="justhodl-eurodollar-plumbing",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("EURODOLLAR: verdict",d["verdict"],"health",d["plumbing_health"])
hubs=(d.get("layers",{}).get("hubs") or {}).get("metrics",[])
print("HK hub metrics ("+str(len(hubs))+"):")
for m in hubs: print("   ",m["status"],m["label"],"=",m["value"],m.get("unit",""))
print("AI short_term:",(d.get("ai") or {}).get("short_term",""))
# 2) signal-board → eurodollar feed present
wait("justhodl-signal-board")
r=lam.invoke(FunctionName="justhodl-signal-board",InvocationType="RequestResponse")
print("\nsignal-board invoke:",r["Payload"].read().decode()[:120])
sb=json.loads(s3.get_object(Bucket=B,Key="data/signal-board.json")["Body"].read())
eng=[e for e in sb.get("engines",[]) if "Eurodollar" in e.get("engine","")]
print("Eurodollar in signal-board:", eng if eng else "NOT FOUND")
print("composite posture:",sb.get("composite"),"| macro category:",(sb.get("categories") or {}).get("macro"))
