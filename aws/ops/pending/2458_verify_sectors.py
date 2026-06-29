import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"))
print("\n=== SECTOR CLUSTERS (systematic shortage detection) ===")
for c in (d.get("early_sector_clusters") or []):
    tag=" <-- SYSTEMATIC" if c.get("systematic") else ""
    print("  %-26s n=%s  %s%s"%(c.get("industry"),c.get("n"),",".join(c.get("tickers") or []),tag))
print("\n=== EARLY CALLS now carry industry ===")
for c in (d.get("early_bottleneck_calls") or [])[:8]:
    print("  %-5s %-26s capex/DA=%s gap=%s"%(c["ticker"],c.get("industry"),c.get("capex_to_da"),c.get("consensus_gap_score")))
print("\n=== COMMODITY producers + sector ===")
for k,v in (d.get("commodity_cycle") or {}).items():
    prods=[p["ticker"]+("*" if (p.get("capex_to_da") is not None and p["capex_to_da"]<1) else "") for p in (v.get("producers") or [])]
    print("  %-11s [%s] cutting=%s/%s  %s"%(k,v.get("producer_industry"),v.get("n_producers_cutting"),len(v.get("producers") or []),",".join(prods)))
print("(* = capex/DA<1, i.e. that producer is cutting capacity)")
print("DONE 2458")
