import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
try:
    r=lam.invoke(FunctionName="justhodl-ecb-derived",InvocationType="RequestResponse")["Payload"].read().decode()
    print("invoke:", r[:160])
except Exception as e: print("invoke err:", str(e)[:120])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-derived.json")["Body"].read())
ind=d.get("indicators",{})
print("\n=== PROBLEM INDICATORS ===")
for k in ["fragmentation_stress","ea_unemployment","ea_industrial_production","ea_confidence","real_m1_growth","country_unemployment"]:
    v=ind.get(k)
    if not isinstance(v,dict): print(f"  {k:26} ABSENT"); continue
    err=v.get("err"); sig=v.get("signal"); spark="spark" if "spark" in v else "NO-spark"
    vals={kk:v[kk] for kk in v if kk not in ("spark","err","signal","label","note")}
    print(f"  {k:26} signal={sig} | {spark} | err={err} | {json.dumps(vals)[:90]}")
print("\n=== charts available (full history keys) ===")
ch=d.get("charts",{})
print(" ", sorted(ch.keys()))
print("\n=== misses ===", json.dumps(d.get("misses",{}),indent=0)[:300])
print("n_sparks:", d.get("n_sparks"))
# sample a chart's date range
for cid in ["it_de_spread","ea_unemployment","ip_yoy","real_m1_growth","business_confidence"]:
    c=ch.get(cid)
    if c and c.get("points"): print(f"  chart {cid:22} {c['points'][0][0]} -> {c['points'][-1][0]} ({len(c['points'])} pts)")
    else: print(f"  chart {cid:22} MISSING/empty")
