"""ops 1998: force-redeploy catalyst-calendar from repo source, invoke, verify EARNINGS now populate."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-catalyst-calendar"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)

SRCDIR=f"aws/lambdas/{FN}/source"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for root,_,files in os.walk(SRCDIR):
        for f in files:
            if f.endswith(".py"):
                p=os.path.join(root,f); z.write(p,os.path.relpath(p,SRCDIR))
buf.seek(0)
lam.update_function_code(FunctionName=FN, ZipFile=buf.read())
for _ in range(24):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")!="InProgress" and c.get("State")=="Active": break
    time.sleep(5)
print("redeployed:",lam.get_function(FunctionName=FN)["Configuration"]["LastModified"])

r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("invoke status:",r["StatusCode"])
print("payload:",r["Payload"].read().decode()[:400])
time.sleep(2)
cc=json.loads(s3.get_object(Bucket=B,Key="data/catalyst-calendar.json")["Body"].read())
ev=cc.get("events",[]); ern=[e for e in ev if e.get("type")=="EARNINGS"]
bz=[e for e in ern if "Benzinga" in (e.get("source") or "")]
fmp=[e for e in ern if e.get("source")=="FMP"]
print(f"\nEARNINGS total={len(ern)}  Benzinga={len(bz)}  FMP-supplement={len(fmp)}")
print("by_source:",cc.get("by_source"))
print("\nTop Benzinga earnings by importance:")
for e in sorted(bz,key=lambda x:-(x.get('importance') or 0))[:8]:
    print(f"  {e.get('date')} {e.get('ticker'):<6} imp={e.get('importance')} {e.get('impact'):<6} {e.get('session') or '-':<4} cons={e.get('consensus')}")
print("\nhigh_impact_next_7d:",cc.get("high_impact_next_7d"),"next_30d:",cc.get("high_impact_next_30d"))
print("DONE 1998")
