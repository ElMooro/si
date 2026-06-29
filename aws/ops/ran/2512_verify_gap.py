import boto3, json, io, zipfile, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
SRC="aws/lambdas/justhodl-money-flow-state/source/lambda_function.py"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
lam.update_function_code(FunctionName="justhodl-money-flow-state",ZipFile=buf.getvalue())
print("updated money-flow code"); time.sleep(8)
r=lam.invoke(FunctionName="justhodl-money-flow-state",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/money-flow-state.json")["Body"].read())
print("version:",d.get("version"))
print("=== institutional_sector_tilt (13F adds-trims by sector) ===")
for x in (d.get("institutional_sector_tilt") or [])[:11]:
    print("  %-22s net=%-5s adding=%-4s trimming=%s"%(x["sector"],x["net_fund_actions"],x["adding"],x["trimming"]))
time.sleep(140)
req=urllib.request.Request("https://justhodl.ai/sector-flow.html",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Cache-Control":"no-cache","Pragma":"no-cache"})
html=urllib.request.urlopen(req,timeout=30).read().decode("utf-8","ignore")
for m in ["renderCongress","Congressional money flow","INSTITUTIONS","political-trades.json"]:
    print(f"  {'FOUND' if m in html else 'MISSING':7} {m}")
print("bytes:",len(html));print("DONE 2512")
