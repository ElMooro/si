import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
FN="justhodl-deal-scanner"
src=open(glob.glob("**/justhodl-deal-scanner/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
def wait():
    for _ in range(90):
        st=lam.get_function_configuration(FunctionName=FN)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": return st
        time.sleep(3)
    return st
wait()
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=code); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
wait(); print("deployed")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:180])
d=json.loads(s3.get_object(Bucket=B,Key="data/deal-scanner.json")["Body"].read())
mapped=sum(1 for x in d["deals"] if x.get("sector_etf")); tail=sum(1 for x in d["deals"] if x.get("sector_tailwind"))
print("deals=%s sector_mapped=%s sector_tailwind=%s"%(len(d["deals"]),mapped,tail))
print("(deals ranked by score; sector tailwind + AI = confluence boost)")
for x in d["deals"]:
    print("   %-7s sc=%-6s sec=%-20s etf=%-5s rot=%-5s tail=%-5s [%s%s] %s"%(x["symbol"],x["score"],(x.get("sector") or "?")[:20],
          x.get("sector_etf") or "-",x.get("sector_rotation_score"),str(x.get("sector_tailwind")),
          "AI" if x["ai_relevant"] else "",({"green":"G","yellow":"Y"}.get(x["highlight"],"")),x["title"][:38]))
