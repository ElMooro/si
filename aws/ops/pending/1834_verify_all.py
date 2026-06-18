import json, time, boto3
from botocore.config import Config
REGION="us-east-1"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":2}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
def wait(fn):
    for _ in range(24):
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return
        except Exception: return
        time.sleep(5)
def inv(fn):
    wait(fn)
    try:
        r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse")
        return r["Payload"].read().decode()[:150]
    except Exception as e: return "INVOKE ERR: "+str(e)[:120]

# 1) eurodollar-plumbing adapter fields
print("eurodollar-plumbing:",inv("justhodl-eurodollar-plumbing"))
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("  health=%s verdict=%s | composite_score=%s score=%s stress_score=%s severity=%s stress_regime=%s"%(
  d.get("plumbing_health"),d.get("verdict"),d.get("composite_score"),d.get("score"),d.get("stress_score"),d.get("severity"),d.get("stress_regime")))

# 2) plumbing-aggregator HK sub-layer in L4
print("\nplumbing-aggregator:",inv("justhodl-plumbing-aggregator"))
p=json.loads(s3.get_object(Bucket=B,Key="data/plumbing-stress.json")["Body"].read())
l4=(p.get("layers") or {}).get("L4") or {}
hk=[c for c in l4.get("contributors",[]) if str(c.get("id","")).startswith("HK_")]
print("  composite=%s | L4 score=%s | HK contributors in L4: %d"%(p.get("composite_score"),l4.get("score"),len(hk)))
for c in hk: print("    %s = %s (stress %s, pctile %s)"%(c["label"],c.get("value"),c.get("stress_score"),c.get("percentile")))

# 3) migrated consumers — invoke + confirm no error + eurodollar picked up
for fn,key,field in [("justhodl-crisis-composite","data/crisis-composite.json",None),
                     ("justhodl-vol-radar","data/vol-radar.json","euro"),
                     ("justhodl-tail-hedge","data/tail-hedge.json",None),
                     ("justhodl-canary-grid","data/canary-grid.json",None),
                     ("justhodl-sovereign-stress","data/sovereign-stress.json",None)]:
    print("\n%s: %s"%(fn,inv(fn)))
