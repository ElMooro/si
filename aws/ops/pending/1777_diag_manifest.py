import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
def gj(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"__err__":type(e).__name__}
man=gj("data/ecb-hist/_manifest.json")
ids=sorted([s["id"] for s in man.get("series",[])]) if "series" in man else []
print("MANIFEST n=",len(ids))
print(", ".join(ids))
print("\n=== SIGHIST targets present? + file check ===")
targets=["it_de_10y_bp","unemployment_ea","indprod_total_yoy","indprod_core_yoy","conf_esi","real_m1_growth"]
mset=set(ids)
for t in targets:
    inman = t in mset
    f=gj(f"data/ecb-hist/{t}.json")
    pts=f.get("points") if isinstance(f,dict) else None
    rng=f"{f.get('first_date')}→{f.get('latest_date')} n={f.get('n_points') or (len(pts) if pts else 0)}" if isinstance(f,dict) and "__err__" not in f else f.get("__err__","?")
    print(f"  {t:22} inManifest={inman!s:5} file={'OK' if pts else 'MISSING'} {rng}")
print("\n=== ecb-derived.json indicator keys (dump signals shown) ===")
der=gj("data/ecb-derived.json")
inds=der.get("indicators",{})
for k,v in inds.items():
    sig=v.get("signal") if isinstance(v,dict) else None
    print(f"  {k:30} signal={sig}")
print("\nhas der.charts keys:", list((der.get('charts') or {}).keys())[:20])
