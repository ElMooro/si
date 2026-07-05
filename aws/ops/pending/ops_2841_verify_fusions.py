"""ops 2841 — verify 3 fusions: EIA->refining-stress, control-group->consumer-pulse, margin->cycle-clock."""
import os, json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2841,"ts":datetime.now(timezone.utc).isoformat()}
def inv(fn):
    try: lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read()
    except Exception as e: R.setdefault("inv_note",{})[fn]=str(e)[:80]
for fn in ("justhodl-refining-stress","justhodl-consumer-pulse","justhodl-cycle-clock"): inv(fn)
time.sleep(3)
# refining-stress
rs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/refining-stress.json")["Body"].read())
eiam=[m["id"] for m in rs.get("metrics",[]) if m["id"] in ("refinery_util","distillate_stocks","gasoline_stocks","cushing_stocks")]
R["refining_stress"]={"regime":rs.get("regime"),"eia_metrics":eiam,"supply_context":rs.get("supply_context"),
    "has_cushing_todo":"note_cushing" in rs}
# consumer-pulse
cp=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/consumer-pulse.json")["Body"].read())
R["consumer_pulse"]={"pulse_index":cp.get("pulse_index"),"regime":cp.get("regime"),
    "retail_control_group":cp.get("retail_control_group")}
# cycle-clock
cc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read()).get("cycle",{})
R["cycle_clock"]={"profit_margin_cycle":cc.get("profit_margin_cycle"),"recession_prob":cc.get("recession_prob_pct"),
    "hard_data":(cc.get("hard_data_recession") or {}).get("read")}
ok=(len(eiam)>=3 and rs.get("supply_context") and not R["refining_stress"]["has_cushing_todo"]
    and (cp.get("retail_control_group") or {}).get("available") and cc.get("profit_margin_cycle"))
R["status"]="ALL 3 FUSIONS LIVE" if ok else "CHECK"
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2841_verify_fusions.json","w"),indent=1,default=str)
print("OPS 2841 COMPLETE")
