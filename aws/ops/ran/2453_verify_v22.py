import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"| dur:",d.get("duration_s"),"s")
print("\nCOMMODITY CYCLE (#7):")
for k,v in (d.get("commodity_cycle") or {}).items():
    print("  %-12s price=%s z=%s yoy=%s%% | producer capex_yoy_med=%s | depressed=%s exiting=%s | CURE=%s"%(
        k,v.get("price"),v.get("price_z"),v.get("price_yoy_pct"),v.get("producer_capex_yoy_med"),
        v.get("price_depressed"),v.get("supply_exiting"),v.get("cure_for_low_prices_setup")))
print("cure setups:",d.get("cure_for_low_prices"))
print("\nCROSS-ENGINE CONFIRM (#8 read-side):")
print(json.dumps(d.get("cross_engine_confirm"),indent=1)[:700])
print("DONE 2453")
