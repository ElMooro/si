"""ops 2868 — dump exact element shapes of the heterogeneous canary feeds for the aggregator parser."""
import os, json, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":2868,"ts":datetime.now(timezone.utc).isoformat()}
def gj(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"__err__":str(e)[:80]}
cc=gj("data/crisis-canaries.json")
can=cc.get("canaries")
if isinstance(can,dict):
    R["crisis_canaries_type"]="dict"; k0=list(can.keys())[0]
    R["crisis_sample"]={k0:can[k0]}; R["crisis_keys"]=list(can.keys())[:20]
elif isinstance(can,list):
    R["crisis_canaries_type"]="list"; R["crisis_sample"]=can[0] if can else None; R["crisis_n"]=len(can)
R["crisis_composite"]={"composite_score":cc.get("composite_score"),"level":cc.get("level"),"alerts":cc.get("alerts"),"families":cc.get("families")}
lm=gj("data/leading-markets.json")
R["lm"]={"turning_point_signal":lm.get("turning_point_signal"),"signal_read":lm.get("signal_read"),
    "flashing_buckets":lm.get("flashing_buckets"),"benchmark":lm.get("benchmark"),
    "market_sample":(lm.get("markets") or [{}])[0]}
al=gj("data/alert-sentinel.json")
R["alert"]={"n_changes":al.get("n_changes"),"changes_sample":(al.get("changes") or [])[:2],"buffer_n":al.get("buffer_n")}
vr=gj("data/vol-radar.json")
firing=[c for c in (vr.get("spike_canaries") or []) if c.get("firing")]
R["vol"]={"posture":vr.get("posture"),"n_firing":len(firing),"firing_sample":firing[:2],"scores":vr.get("scores")}
dr=gj("data/dollar-radar.json")
R["dollar"]={"pressure":dr.get("dollar_pressure"),"regime":dr.get("regime"),"canary_sample":(dr.get("canaries") or [{}])[0]}
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2868_feed_detail.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2868 COMPLETE")
