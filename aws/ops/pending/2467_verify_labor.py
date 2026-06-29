import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
L=d.get("labor_bottleneck") or {}
print("=== #3 LABOR BOTTLENECK ===")
for sec in ("manufacturing","construction","mining_logging"):
    v=L.get(sec)
    if v: print("  %-14s oth=%-5s oth_z=%-5s openYoY=%-6s wageYoY=%-5s wageAccel=%-5s -> %s"%(
        sec,v.get("openings_to_hires"),v.get("oth_z"),v.get("openings_yoy_pct"),v.get("wage_yoy_pct"),v.get("wage_accel_pp"),v.get("labor_tightness")))
print("constrained_sectors:",L.get("constrained_sectors"),"| supply_ramp_blocked:",L.get("supply_ramp_blocked"))
print("DONE 2467")
