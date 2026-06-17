import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-ecb-derived",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-derived.json")["Body"].read())
ch=d.get("charts",{})
print("=== IP YoY by sector ===")
for cid in ["ipyoy_total","ipyoy_manufacturing","ipyoy_intermediate","ipyoy_capital","ipyoy_durable","ipyoy_nondurable","ipyoy_energy"]:
    c=ch.get(cid)
    print(f"  {cid:22} "+(f"{c['points'][0][0]}→{c['points'][-1][0]} latest={c.get('latest')}% pctile={c.get('pctile')}" if c and c.get('points') else "MISSING"))
print("=== confidence (recap) ===")
for cid in ["conf_industrial","conf_services","conf_consumer","conf_retail","conf_construction","esi_sentiment"]:
    c=ch.get(cid); print(f"  {cid:22} latest={c.get('latest') if c else '—'} pctile={c.get('pctile') if c else '—'}")
print("total charts:", len(ch))
