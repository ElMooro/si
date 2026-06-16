import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
before=None
try: before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/regime-composite.json")["Body"].read()).get("generated_at")
except: pass
lam.invoke(FunctionName="justhodl-regime-composite",InvocationType="Event")
print("invoked regime-composite; waiting...")
r=None
for i in range(8):
    time.sleep(18)
    try:
        r=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/regime-composite.json")["Body"].read())
        if r.get("generated_at")!=before: break
    except: pass
mods=r.get("modules") or r.get("module_detail") or []
ciss_mods=[m for m in mods if 'CISS' in str(m.get('label',''))]
print(f"refreshed={r.get('generated_at')!=before} | CISS-family modules ({len(ciss_mods)}):")
for m in ciss_mods: print(f"  {m.get('label'):32} dim={m.get('dimension'):10} regime={m.get('regime')} pol={m.get('polarity')} | {str(m.get('signal'))[:60]}")
# manifest has fx_claims_nonea?
mani=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/_manifest.json")["Body"].read())
ser=mani.get("series") if isinstance(mani,dict) else mani
ids={s["id"] for s in ser}
print("\nmanifest count:", len(ids), "| fx_claims_nonea:", "fx_claims_nonea" in ids, "| unemp_it:", "unemp_it" in ids, "| indprod_energy:", "indprod_energy" in ids)
