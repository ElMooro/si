import json, time, boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-eurodollar-plumbing"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":3}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(5)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:140])
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("\nVERDICT:",d["verdict"],"| health:",d["plumbing_health"],"| gen:",d["generated_at"])
ai=d.get("ai",{})
print("\nAI state:",ai.get("state"))
print("AI summary:",ai.get("summary"))
print("AI short_term:",ai.get("short_term"))
print("key_drivers:",ai.get("key_drivers"))
print("\nred_flags:",d.get("red_flags"),"| yellow:",d.get("yellow_flags"))
print("\nLAYERS:")
for lk,lv in (d.get("layers") or {}).items():
    print(f"  [{lv['title']}]")
    for m in lv["metrics"]:
        v="n/a" if m.get("value") is None else f"{m['value']}{m.get('unit','')}"
        px=f" p{m['pctile']}" if m.get("pctile") is not None else ""
        print(f"    {m['status']:11} {m['label']:42} {v}{px}")
