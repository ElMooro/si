import boto3, json, zipfile, io, glob, time
from botocore.config import Config
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"; FN="justhodl-etf-fund-flows"
src=open(glob.glob("**/justhodl-etf-fund-flows/source/lambda_function.py",recursive=True)[0]).read()
import re
n_univ=len(re.findall(r'^\s*"[A-Z]{2,5}":\s*\{"category"', src, re.M))
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("UPDATED code, universe=%d"%n_univ); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
for _ in range(40):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
before=""
try: before=json.loads(s3.get_object(Bucket=B,Key="etf-flows/daily.json")["Body"].read()).get("generated_at","")
except Exception: pass
lam.invoke(FunctionName=FN,InvocationType="Event"); print("async invoked, polling daily.json…")
d=None
for _ in range(46):  # ~230s
    time.sleep(5)
    try:
        d=json.loads(s3.get_object(Bucket=B,Key="etf-flows/daily.json")["Body"].read())
        if d.get("generated_at") and d.get("generated_at")!=before: break
    except Exception: pass
m=d.get("metrics",[]) if d else []
ok=[x for x in m if x.get("ticker") and not x.get("error")]
print("\n=== ETF-FLOWS now: %d returned with data (was 84) ==="%len(ok))
tk=set(x["ticker"] for x in ok)
# leveraged coverage
lev=[x for x in ok if x.get("category")=="leveraged"]
print("LEVERAGED ETFs with flow data: %d"%len(lev))
# check key bull/bear pairs
pairs=[("SOXL","SOXS"),("TQQQ","SQQQ"),("SPXL","SPXS"),("TECL","TECS"),("FAS","FAZ"),
       ("LABU","LABD"),("ERX","ERY"),("YINN","YANG"),("NUGT","DUST"),("BOIL","KOLD"),
       ("NVDL","NVDS"),("TSLL","TSLQ"),("TNA","TZA"),("UCO","SCO")]
print("\nBULL/BEAR PAIR COVERAGE (the positioning read):")
for b,s in pairs:
    bf=next((x["flow_5d_usd"] for x in ok if x["ticker"]==b),None)
    sf=next((x["flow_5d_usd"] for x in ok if x["ticker"]==s),None)
    bok="✓" if b in tk else "✗"; sok="✓" if s in tk else "✗"
    print("  %-5s %s  vs  %-5s %s   bull5d=%s bear5d=%s"%(b,bok,s,sok,
        ("$%.0fM"%(bf/1e6) if bf else "—"),("$%.0fM"%(sf/1e6) if sf else "—")))
# which defined ETFs returned NO data (errored / not covered)
defined=set(re.findall(r'^\s*"([A-Z]{2,5})":\s*\{"category"', src, re.M))
missing=sorted(defined - tk)
print("\nDefined but NO data from ETF Global (%d): %s"%(len(missing)," ".join(missing)))
