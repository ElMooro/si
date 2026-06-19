import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; FN="justhodl-massive-signals"
src=open(glob.glob("**/justhodl-massive-signals/source/lambda_function.py",recursive=True)[0]).read()
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
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:140])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/massive-signals.json")["Body"].read()); m=d["market"]
print("gamma_regime=%s smallcap_bid=%s n_tickers=%s top_prepump=%s"%(m.get("gamma_regime"),m.get("smallcap_bid"),d.get("n_tickers"),len(d.get("top_prepump",[]))))
print("\nTOP PRE-PUMP (unified gamma + options flow):")
for r in (d.get("top_prepump") or [])[:12]:
    print("  %-6s score=%-6s | %s"%(r["symbol"],r.get("prepump_score"),(r.get("massive_why") or "")[:62]))
