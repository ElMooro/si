import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"))
calls=d.get("early_bottleneck_calls") or []
print("early calls now (%d):"%len(calls))
for c in calls:
    flag="DROP?" if (c.get("capex_to_da") is None or c.get("capex_to_da")>=1) else ""
    print("  %-5s capex/DA=%-6s capexYoY=%-7s margin=%-7s ml=%s gap=%s  %s"%(
        c["ticker"],c.get("capex_to_da"),c.get("capex_yoy_pct"),c.get("net_margin_pct"),c.get("money_losing"),c.get("consensus_gap_score"),flag))
aaon_lyb=[c["ticker"] for c in calls if c["ticker"] in ("AAON","LYB")]
print("\nAAON/LYB still present:",aaon_lyb or "NONE (correctly dropped)")
print("all survivors capex/DA<1:",all((c.get("capex_to_da") is not None and c.get("capex_to_da")<1) for c in calls))
print("DONE 2457")
