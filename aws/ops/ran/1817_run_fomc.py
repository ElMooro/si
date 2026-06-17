import json, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
try:
    cfg=lam.get_function_configuration(FunctionName="justhodl-fomc-reaction")
    print("FUNCTION EXISTS:", cfg["FunctionName"], "| env keys:", sorted((cfg.get("Environment",{}).get("Variables",{})).keys()))
except Exception as e:
    print("STILL MISSING:", e.__class__.__name__); raise SystemExit
r=lam.invoke(FunctionName="justhodl-fomc-reaction",InvocationType="RequestResponse")
print("invoke:", r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/fomc-reaction.json")["Body"].read())
print("\nmeeting:",d["meeting_date"],"decision_day:",d["is_decision_day"])
print("SURPRISE:",d["surprise"]["label"],"| Δ2y_bp:",d["surprise"]["d2y_bp"],"| tone:",d["surprise"]["statement_tone"])
print("calib events_by_sign:",d["calibration"]["events_by_sign"],"n=",d["calibration"]["n_events"])
print("\nREACTION MAP ("+d["surprise"]["label"]+"):")
for k,v in d["reaction_map"].items():
    s=v.get("short") or {}; l=v.get("long") or {}
    su="—" if not s else f"{s['median']:+}{v['unit']} [{s['p25']:+}..{s['p75']:+}] up{s['prob_up_pct']}% n{s['n']}"
    lu="—" if not l else f"{l['median']:+}{v['unit']} [{l['p25']:+}..{l['p75']:+}] up{l['prob_up_pct']}% n{l['n']}"
    print(f"  {k:24} 5d {su:42} 63d {lu}")
print("\nself_grading:",d["self_grading"])
