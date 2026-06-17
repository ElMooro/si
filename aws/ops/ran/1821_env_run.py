import json,time,boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-fomc-reaction"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":3}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
# wait for any in-progress update to finish
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus")!="InProgress" and c.get("State")=="Active": break
    time.sleep(6)
cur=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
for src_fn in ["justhodl-research-critique","justhodl-ai-chat","justhodl-fed-speak","justhodl-weekly-ai-review"]:
    try:
        se=lam.get_function_configuration(FunctionName=src_fn).get("Environment",{}).get("Variables",{})
        for k in ["ANTHROPIC_API_KEY","FMP_KEY","FRED_KEY","FMP_API_KEY","FRED_API_KEY","ZAI_API_KEY"]:
            if k in se and k not in cur: cur[k]=se[k]
        if "ANTHROPIC_API_KEY" in cur: break
    except Exception as e: print(src_fn,"err",e)
cur["S3_BUCKET"]="justhodl-dashboard-live"
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":cur})
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")!="InProgress": break
    time.sleep(5)
print("env ANTHROPIC present:", "ANTHROPIC_API_KEY" in cur, "| keys:", sorted(cur.keys()))
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/fomc-reaction.json")["Body"].read())
sp=d["surprise"]
print("\nSURPRISE:",sp["label"],"| basis:",sp.get("basis"),"| Δ2y_bp:",sp.get("d2y_change_bp"),"2y_fresh:",sp.get("two_y_fresh"))
print("tone:",sp.get("statement_tone"))
print("calib:",d["calibration"]["events_by_sign"],"n=",d["calibration"]["n_events"])
print("\nREACTION MAP ("+sp["label"]+"):")
for k,v in d["reaction_map"].items():
    s=v.get("short") or {}; l=v.get("long") or {}
    su="—" if not s else f"{s['median']:+g}{v['unit']} [{s['p25']:+g}..{s['p75']:+g}] up{s['prob_up_pct']}% n{s['n']}"
    lu="—" if not l else f"{l['median']:+g}{v['unit']} [{l['p25']:+g}..{l['p75']:+g}] up{l['prob_up_pct']}% n{l['n']}"
    print(f"  {k:24} 5d {su:46} 63d {lu}")
