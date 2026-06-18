import boto3, json, zipfile, io, glob, time, urllib.request
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
wait(); print("deployed deal-scanner")
# does universe carry 'sector'?
try:
    u=json.loads(s3.get_object(Bucket=B,Key="data/universe.json")["Body"].read())
    s0=(u.get("stocks") or [{}])[0]
    print("universe stock keys:",list(s0.keys())," sector_sample=%r"%s0.get("sector"))
except Exception as e: print("universe read err",e)
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/deal-scanner.json")["Body"].read()); sm=d["summary"]
mapped=sum(1 for x in d["deals"] if x.get("sector_etf")); tail=sum(1 for x in d["deals"] if x.get("sector_tailwind"))
print("deals=%s sector_mapped=%s sector_tailwind=%s ai=%s"%(sm["n_deals"],mapped,tail,sm.get("n_ai")))
for x in d["deals"]:
    print("   %-7s sec=%-22s etf=%-5s score=%-5s tail=%s [%s%s] %s"%(x["symbol"],(x.get("sector") or "?")[:22],x.get("sector_etf") or "-",
          x.get("sector_rotation_score"),x.get("sector_tailwind"),"AI" if x["ai_relevant"] else "",({"green":"G","yellow":"Y"}.get(x["highlight"],"")),x["title"][:40]))
# sector-flow page flags live?
try:
    b=urllib.request.urlopen("https://justhodl.ai/sector-flow.html?t=%d"%time.time(),timeout=15).read().decode()
    print("sector-flow page flags-render present:", "score ${esc" in b or "flags" in b)
except Exception as e: print("page check skip",str(e)[:40])
