import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; FN="justhodl-ai-rerating-radar"
src=open(glob.glob("**/justhodl-ai-rerating-radar/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
for _ in range(60):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
for _ in range(60):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:120])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/ai-rerating-radar.json")["Body"].read())
sets=(d.get("summary",{}) or {}).get("top_setups",[]) or []
ib=[r["symbol"] for r in sets if r.get("insider_buying")]
au=[r["symbol"] for r in sets if r.get("analyst_upgrading")]
print("re-rating candidates with insider_buying:",ib[:10] or "none in top_setups")
print("re-rating candidates analyst_upgrading:",au[:10] or "none in top_setups")
print("\nTOP 6 setups (note new kickers in why):")
for r in sets[:6]:
    print("  %-6s comp=%-6s | %s"%(r["symbol"],r["composite"],(r.get("why") or "")[:90]))
