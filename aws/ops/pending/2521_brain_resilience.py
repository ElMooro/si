import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(24):
    st=lam.get_function_configuration(FunctionName="justhodl-brain-sync").get("LastUpdateStatus")
    if st=="Successful": break
    time.sleep(5)
print("LastUpdateStatus:",st)
# confirm llm_router bundled
import io,zipfile,base64
code=lam.get_function(FunctionName="justhodl-brain-sync")["Code"]["Location"]
import urllib.request
z=zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(code).read()))
print("llm_router bundled:", "llm_router.py" in z.namelist())
r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError")); time.sleep(4)
br=json.loads(s3.get_object(Bucket=B,Key="data/brain.json")["Body"].read())
rr=br.get("regime_read") or {}
print("regime_read keys:",list(rr.keys())[:8])
if "_error" in rr: print("  regime_read STILL erroring:",rr["_error"][:90],"(both providers down — will self-heal on top-up)")
elif "regime" in rr: print("  regime_read RECOVERED via fallback! regime:",rr.get("regime"),"| headline:",str(rr.get("headline"))[:90])
else: print("  regime_read:",json.dumps(rr)[:120])
d=br.get("directive") or {}
print("directive populated:", bool(d), "| sector_tilts:", list((d.get("sector_tilts") or {}).keys())[:6] if d else None)
print("DONE 2521")
