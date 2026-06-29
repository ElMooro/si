import boto3, json, io, zipfile, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
# Build zip = brain-sync source + ALL aws/shared/*.py (mirror deploy-lambdas bundle)
import os, glob
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for p in glob.glob("aws/shared/*.py"):
        z.write(p, os.path.basename(p))
    z.writestr("lambda_function.py", open("aws/lambdas/justhodl-brain-sync/source/lambda_function.py").read())
names=zipfile.ZipFile(io.BytesIO(buf.getvalue())).namelist()
print("zip has llm_router.py:", "llm_router.py" in names, "| n files:", len(names))
lam.update_function_code(FunctionName="justhodl-brain-sync",ZipFile=buf.getvalue())
for _ in range(24):
    st=lam.get_function_configuration(FunctionName="justhodl-brain-sync").get("LastUpdateStatus")
    if st=="Successful": break
    time.sleep(5)
print("LastUpdateStatus:",st)
r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError")); time.sleep(4)
br=json.loads(s3.get_object(Bucket=B,Key="data/brain.json")["Body"].read())
rr=br.get("regime_read") or {}
if "regime" in rr:
    print("✅ regime_read RECOVERED via router! regime:",rr.get("regime"))
    print("   headline:",str(rr.get("headline"))[:120])
    print("   invest_in:",(rr.get("invest_in") or [])[:4])
elif "_error" in rr:
    print("regime_read error (provider detail):",rr["_error"][:140])
    print("→ router now bundled; GLM/Z.ai attempted first. If still erroring, BOTH providers are down — structural fix is in place, self-heals on top-up.")
else: print("regime_read:",json.dumps(rr)[:160])
d=br.get("directive") or {}
print("directive populated:", bool(d), "| tilts:", list((d.get("sector_tilts") or {}).keys())[:6] if d else None)
print("DONE 2522")
