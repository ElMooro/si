import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-deal-scanner",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
deals=d.get("deals") or d.get("results") or []
print("n_deals:",len(deals))
withconv=[x for x in deals if x.get("sector_conviction") is not None]
print("deals carrying sector_conviction:",len(withconv))
for x in withconv[:6]:
    print("  %-6s %-14s conv=%-5s posture=%-11s tailwind=%s"%(x.get("symbol"),(x.get("sector") or "")[:14],x.get("sector_conviction"),x.get("sector_posture"),x.get("sector_tailwind")))
print("DONE 2503")
