import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-deal-scanner"; B="justhodl-dashboard-live"
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
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/deal-scanner.json")["Body"].read())
sm=d["summary"]
print("DEAL-SCANNER: items=%s deals=%s ai=%s green=%s | sources=%s"%(sm["n_prs_scanned"],sm["n_deals"],sm.get("n_ai"),sm.get("n_green"),len(d.get("sources",[]))))
pubs={}
for x in d["deals"]: pubs[x.get("publisher","?")]=pubs.get(x.get("publisher","?"),0)+1
print("  deal publishers:",pubs)
for x in d["deals"][:8]:
    print("   %-7s %-9s [%s%s] %s"%(x["symbol"],(x.get("publisher") or "")[:9],"AI" if x["ai_relevant"] else "",({"green":"G","yellow":"Y"}.get(x["highlight"],"")),x["title"][:46]))
# ---- Part 2: freshness of existing ETF-flow / sector engines ----
print("\n=== EXISTING ETF-FLOW / SECTOR ENGINE FRESHNESS ===")
import datetime
now=datetime.datetime.now(datetime.timezone.utc)
for key in ["data/etf-fund-flows.json","data/etf-flows.json","data/etf-true-flows.json",
            "data/flow-anomaly-detector.json","data/flow-anomalies.json","data/sector-rotation.json",
            "data/capital-flow.json","data/rotation-radar.json"]:
    try:
        h=s3.head_object(Bucket=B,Key=key); lm=h["LastModified"]; age=(now-lm).total_seconds()/3600
        print("  %-34s LastModified %s  (%.0fh ago)%s"%(key,lm.strftime("%Y-%m-%d %H:%M"),age," STALE" if age>30 else " FRESH"))
    except Exception as e:
        print("  %-34s MISSING (%s)"%(key,str(e)[:30]))
# list any other flow/sector data keys
print("  -- other flow/sector outputs present --")
seen=set()
for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=B,Prefix="data/"):
    for o in pg.get("Contents",[]):
        k=o["Key"]
        if any(t in k for t in ["flow","sector","rotation","inflow"]) and k.endswith(".json") and k not in seen:
            seen.add(k); age=(now-o["LastModified"]).total_seconds()/3600
            print("     %-40s %.0fh%s"%(k,age," STALE" if age>30 else ""))
