import boto3, json, io, zipfile, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
def zip_has(fn, name):
    try:
        loc=lam.get_function(FunctionName=fn)["Code"]["Location"]
        z=zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc).read()))
        return name in z.namelist()
    except Exception as e: return f"ERR {str(e)[:50]}"
# 1) MI deployed via pipeline with hardening A → must contain llm_router.py
print("morning-intelligence zip has llm_router.py:", zip_has("justhodl-morning-intelligence","llm_router.py"))
print("brain-sync zip has llm_router.py:", zip_has("justhodl-brain-sync","llm_router.py"))
# 2) confirm MI imports it (sanity) and invoke to exercise the router path
for _ in range(20):
    st=lam.get_function_configuration(FunctionName="justhodl-morning-intelligence").get("LastUpdateStatus")
    if st=="Successful": break
    time.sleep(5)
print("MI LastUpdateStatus:",st)
r=lam.invoke(FunctionName="justhodl-morning-intelligence",InvocationType="RequestResponse",Payload=b"{}")
print("MI invoke err:",r.get("FunctionError"))
log=r.get("LogResult")
print("DONE 2523")
