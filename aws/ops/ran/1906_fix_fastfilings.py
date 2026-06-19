import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; FN="justhodl-fast-filings"
src=open(glob.glob("**/justhodl-fast-filings/source/lambda_function.py",recursive=True)[0]).read()
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
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:140])
except Exception as e: print("sync timeout, async:",str(e)[:40]); lam.invoke(FunctionName=FN,InvocationType="Event")
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/fast-filings.json")["Body"].read())
print("activist=%s (w/ticker %s) clusters=%s"%(d.get("n_activist"),len(d.get("activist_with_ticker",[])),d.get("n_clusters")))
print("RECENT ACTIVIST (should be June 2026):")
for a in (d.get("activist_with_ticker") or [])[:10]:
    print("   %-6s %-8s %s  %s"%(a.get("subject_ticker"),a.get("form"),a.get("date"),(a.get("filer") or "")[:46]))
print("FORM4 CLUSTERS:",[(c["symbol"],c["n_buyers"]) for c in (d.get("form4_clusters") or [])][:6])
print("picks:",[p["symbol"] for p in (d.get("picks") or [])][:12])
