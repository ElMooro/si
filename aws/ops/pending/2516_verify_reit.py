import boto3, json, io, zipfile, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
SRC="aws/lambdas/justhodl-gold-equity-rotation/source/lambda_function.py"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
lam.update_function_code(FunctionName="justhodl-gold-equity-rotation",ZipFile=buf.getvalue())
print("updated gold-equity code"); time.sleep(8)
r=lam.invoke(FunctionName="justhodl-gold-equity-rotation",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError"))
m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/gold-equity-rotation.json")["Body"].read()).get("current_metrics",{})
print("VNQ 20d:",m.get("vnq_20d_pct"),"| REM 20d:",m.get("rem_20d_pct"),"| TLT:",m.get("tlt_20d_pct"),"| GLD:",m.get("gld_20d_pct"))
time.sleep(140)
req=urllib.request.Request("https://justhodl.ai/sector-flow.html",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Cache-Control":"no-cache"})
html=urllib.request.urlopen(req,timeout=30).read().decode("utf-8","ignore")
for s in ["Equity REITs (VNQ)","Mortgage REITs (REM)"]:
    print(f"  {'FOUND' if s in html else 'MISSING':7} {s}")
print("DONE 2516")
