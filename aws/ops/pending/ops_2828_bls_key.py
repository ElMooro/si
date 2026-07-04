"""ops 2828 — provision the valid BLS key (from updated secret): validate vs BLS v2,
set Lambda env + SSM, invoke, confirm v2 engaged. Never prints the key."""
import os, json, time, urllib.request
from datetime import datetime, timezone
import boto3
REGION="us-east-1"; FN="bls-labor-agent"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION); ssm=boto3.client("ssm",region_name=REGION)
R={"ops":2828,"ts":datetime.now(timezone.utc).isoformat()}
KEY=os.environ.get("BLS_API_KEY","").strip()
R["key_present"]=bool(KEY); R["key_len"]=len(KEY)
def wait_ready(t=40):
    for _ in range(t):
        try:
            c=lam.get_function_configuration(FunctionName=FN)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(3)
    return False
try:
    if not KEY: R["status"]="NO KEY IN ENV"; raise SystemExit
    # validate against BLS v2
    payload={"seriesid":["LNS14000000"],"startyear":"2025","endyear":"2026","registrationkey":KEY}
    req=urllib.request.Request("https://api.bls.gov/publicAPI/v2/timeseries/data/",data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"})
    resp=json.loads(urllib.request.urlopen(req,timeout=40).read())
    R["bls_v2_validate"]={"status":resp.get("status"),"message":resp.get("message")}
    if resp.get("status")!="REQUEST_SUCCEEDED":
        R["status"]="KEY REJECTED BY BLS v2 — not wired"; raise SystemExit
    # set env + SSM
    cur=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
    cur["BLS_API_KEY"]=KEY
    wait_ready(); lam.update_function_configuration(FunctionName=FN,Environment={"Variables":cur}); wait_ready()
    ssm.put_parameter(Name="/justhodl/bls-api-key",Value=KEY,Type="SecureString",Overwrite=True)
    # invoke + verify v2 engaged
    lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read(); time.sleep(2)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bls-labor.json")["Body"].read())
    R["result"]={"api_version":d.get("api_version"),"key_valid":d.get("key_valid"),"series_live":d.get("_series_live"),
        "summary":d.get("summary")}
    R["status"]="BLS v2 LIVE (valid key)" if d.get("api_version")=="v2" else "KEY SET but still v1 (check)"
except SystemExit: pass
except Exception as e:
    R["status"]="ERR"; R["error"]=repr(e)[:180]
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2828_bls_key.json","w"),indent=1,default=str)
print("OPS 2828 COMPLETE")
