"""ops 4559 — P0 SECURITY. (1) HALT the FRED scoped-import cron (stop the
fleet throttle problem while the key is being revoked). (2) Stand up the
SSM SecureString parameter /justhodl/fred-api-key so the NEW key Khalid
generates lives in ONE place, never in code. (3) Add a shared helper the
fleet can import. Does NOT write the key value (Khalid sets that after
revoking) and does NOT rewrite 156 files yet — that's the follow-on once
the param holds the new key. Reports blast radius."""
import json,os,time
from datetime import datetime,timezone
import boto3
ev=boto3.client("events",region_name="us-east-1")
ssm=boto3.client("ssm",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":4559,"at":datetime.now(timezone.utc).isoformat()}
# 1) HALT FRED cron
try:
    ev.disable_rule(Name="justhodl-fred-catalog-5min")
    R["fred_cron"]=ev.describe_rule(Name="justhodl-fred-catalog-5min").get("State")
except Exception as e: R["fred_cron_err"]=str(e)[:80]
# 2) SSM param placeholder (only if absent — never clobber a real key Khalid set)
try:
    ex=ssm.get_parameter(Name="/justhodl/fred-api-key",WithDecryption=False)
    R["ssm_param"]="already exists (leaving Khalid's value intact)"
except ssm.exceptions.ParameterNotFound:
    ssm.put_parameter(Name="/justhodl/fred-api-key",
        Value="REPLACE_ME_AFTER_REVOKING_OLD_KEY",Type="SecureString",
        Description="FRED API key — set by Khalid after revoking the leaked one. ops 4559.",
        Overwrite=False)
    R["ssm_param"]="created placeholder — Khalid must set the new key value"
except Exception as e: R["ssm_err"]=str(e)[:100]
# how many functions would need the SSM helper (informational)
R["files_with_hardcoded_key"]=156
# confirm nothing else disturbed
for slug,k in (("eurostat","data/_state/sdmx-walk-eurostat.json"),("statcan","data/_state/sdmx-walk-statcan.json")):
    try:
        st=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        R.setdefault("untouched",{})[slug]={"done":len(st.get("done") or []),"status":st.get("status")}
    except Exception as e: R.setdefault("untouched",{})[slug]=str(e)[:50]
def bus(p):
    i=lam.invoke(FunctionName="justhodl-a2a-bus",InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"P0 SECURITY (Perplexity): FRED key leaked in 156 files on public repo — Khalid revoking now. "
  f"FRED cron HALTED ({R.get('fred_cron')}). SSM /justhodl/fred-api-key {R.get('ssm_param')}. Fleet-wide "
  "per-key throttle problem acknowledged — a per-process 90/min cap across 114 Lambdas cannot bound one key's "
  "120/min budget; central token bucket needed. Next: rewrite 156 files to read SSM (no hardcoded key ever "
  "again) once Khalid sets the new value. Other providers untouched: "+json.dumps(R.get("untouched")))})
bus({"action":"fanout_pending"})
R["verdict"]=f"fred_cron={R.get('fred_cron')} ssm={R.get('ssm_param')} untouched={json.dumps(R.get('untouched'))}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4559.json","w"),indent=1,default=str)
open("aws/ops/reports/4559.md","w").write("# 4559 P0 SECURITY — "+R["verdict"]+"\n")
print(R["verdict"])
