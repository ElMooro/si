import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; FN="justhodl-master-ranker"
src=open(glob.glob("**/justhodl-master-ranker/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:120])
except Exception as e: print("sync slow, async:",str(e)[:40]); lam.invoke(FunctionName=FN,InvocationType="Event"); time.sleep(60)
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/master-ranker.json")["Body"].read())
tt=d.get("top_tickers") or d.get("top") or []
withm=[t for t in tt if any((c.get("system")=="massive") for c in (t.get("contributions") or []))]
print("master-ranker top_tickers=%s | with massive system=%s"%(len(tt),len(withm)))
for t in withm[:6]:
    print("  %-6s score=%s | %s"%(t.get("ticker"),t.get("score"),(t.get("rationale") or "")[:70]))
