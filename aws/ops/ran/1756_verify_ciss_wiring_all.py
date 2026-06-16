import json, time, urllib.request, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=60,retries={"max_attempts":0}))
def before(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read()).get("generated_at")
    except: return None
b={k:before(k) for k in ["data/regime-composite.json","data/global-stress.json","data/risk-radar.json"]}
for fn in ["justhodl-regime-composite","justhodl-global-stress","justhodl-risk-radar"]:
    lam.invoke(FunctionName=fn,InvocationType="Event")
print("invoked 3 async; waiting...")
time.sleep(125)
def rd(k):
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
# regime-composite: CISS module present?
r=rd("data/regime-composite.json")
mods=r.get("modules") or r.get("module_detail") or []
ciss_mod=[m for m in mods if "CISS" in str(m.get("label",""))]
print(f"\nregime-composite refreshed={r.get('generated_at')!=b['data/regime-composite.json']} | CISS module: {ciss_mod[0] if ciss_mod else 'NOT FOUND'}")
# global-stress: ciss_systemic + adj index
g=rd("data/global-stress.json")
print(f"global-stress refreshed={g.get('generated_at')!=b['data/global-stress.json']} | gsi={g.get('global_stress_index')} gsi_ciss_adj={g.get('global_stress_index_ciss_adj')} ciss_systemic={g.get('ciss_systemic')}")
# risk-radar: macro_stress
rr=rd("data/risk-radar.json")
print(f"risk-radar refreshed={rr.get('generated_at')!=b['data/risk-radar.json']} | macro_stress={rr.get('macro_stress')}")

# live-render check of ciss.html + feeds
def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"}),timeout=25) as x: return x.status,x.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__),""
PX="https://justhodl-data-proxy.raafouis.workers.dev"
sp,html=get("https://justhodl.ai/ciss.html?t=v")
print(f"\nciss.html live http={sp} | has AI panel={'aiPanel' in html} sparkline={'sparkline' in html} detailChart={'detailChart' in html}")
for k in ["data/ciss-stress.json","data/ciss-ai.json"]:
    s1,_=get(f"https://justhodl.ai/{k}?t=v"); s2,_=get(f"{PX}/{k}?t=v")
    print(f"  {k}: justhodl.ai={s1} proxy={s2}")
