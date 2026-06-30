import boto3, json, io, zipfile, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
def zip_has(fn):
    loc=lam.get_function(FunctionName=fn)["Code"]["Location"]
    n=zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc).read())).namelist()
    return ("claude_compat.py" in n, "llm_router.py" in n)
B1=["justhodl-ai-chat","justhodl-ask-desk","justhodl-my-brief","justhodl-devils-advocate","justhodl-page-ai-commentary","justhodl-nobrainer-rationale"]
print("=== bundled (claude_compat, llm_router) ===")
allok=True
for fn in B1:
    cc,lr=zip_has(fn); allok=allok and cc and lr
    print(f"  {fn}: compat={cc} router={lr}")
print("ALL BUNDLED:",allok)
# functional: invoke page-ai-commentary (runs on data, returns AI text) to prove it works live now
print("\n=== live functional test (page-ai-commentary) ===")
for _ in range(20):
    if lam.get_function_configuration(FunctionName="justhodl-page-ai-commentary").get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
try:
    r=lam.invoke(FunctionName="justhodl-page-ai-commentary",InvocationType="RequestResponse",Payload=b"{}")
    print("  invoke err:",r.get("FunctionError"))
except Exception as e: print("  invoke:",str(e)[:80])
print("DONE 2532")
