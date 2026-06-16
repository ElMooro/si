import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
def rd(k): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
# 1) manifest has the new series (drives ecb.html labour + history)
m=rd("data/ecb-hist/_manifest.json")
ser=m.get("series") if isinstance(m,dict) else m
ids={s["id"]:s for s in ser}
print("manifest series count:", len(ids))
for i in ["unemployment_ea","indprod_total","indprod_core","eurusd","ciss_fx","ciss_mm"]:
    s=ids.get(i); print(f"  {i:18} {'OK latest='+str(s.get('latest'))+' '+str(s.get('latest_date')) if s else 'MISSING'}")
# 2) ciss.html dollar panel feeds
for k in ["data/ecb-hist/eurusd.json","data/ecb-hist/ilm_usd_claims.json"]:
    d=rd(k); print(f"  {k.split('/')[-1]:22} latest={d.get('latest')} n={d.get('n_points')} {d.get('first_date')}->{d.get('latest_date')}")
# 3) ciss-stress has FX + MM sub-indices (both pages)
C=rd("data/ciss-stress.json")
subs=[s for s in C["series"] if s["category"]=="ea_subindex"]
print("ciss ea_subindex:", [s["indicator"] for s in subs])
print("ciss n_series:", C["n_series"], "| has US/CN:", any(s["area"]=="US" for s in C["series"]), any(s["area"]=="CN" for s in C["series"]))
