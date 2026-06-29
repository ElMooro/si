import boto3, json, io, zipfile, time
from botocore.config import Config
REGION="us-east-1"
lam=boto3.client("lambda",REGION,config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3",REGION)
SRC="aws/lambdas/justhodl-sector-flow-state/source/lambda_function.py"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
lam.update_function_code(FunctionName="justhodl-sector-flow-state",ZipFile=buf.getvalue())
print("updated sector-flow-state code"); time.sleep(8)
r=lam.invoke(FunctionName="justhodl-sector-flow-state",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/sector-flow-state.json")["Body"].read())
print("version:",d.get("version"))
def money(v):
    if v is None: return "n/a"
    a=abs(v); s="+" if v>=0 else "-"
    return s+"$"+(f"{a/1e9:.2f}B" if a>=1e9 else f"{a/1e6:.0f}M" if a>=1e6 else f"{a/1e3:.0f}K")
print("=== sectors with dollar-flow folded in ===")
for x in d.get("sectors",[]):
    print("  %-5s conv=%-5s %-11s $flow=%-9s confirms=%-5s drivers=%s"%(x["symbol"],x["conviction"],x["posture"],money(x.get("dollar_flow_usd")),x.get("dollar_confirms"),", ".join(x.get("drivers",[]))))
# downstream: re-run deal-scanner, confirm it still gets sector_conviction (now dollar-aware)
lam.invoke(FunctionName="justhodl-deal-scanner",InvocationType="RequestResponse",Payload=b"{}")
dd=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
deals=dd.get("deals") or []
wc=[x for x in deals if x.get("sector_conviction") is not None]
print("downstream deal-scanner deals w/ conviction:",len(wc),"of",len(deals))
print("DONE 2508")
