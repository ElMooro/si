import urllib.request, json, time, boto3
B="justhodl-dashboard-live"; s3=boto3.client("s3","us-east-1")
def get(u,t=15):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=t); return r.getcode(), r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,"code",0), str(e)[:60]
# page + nav live
for label,url,marker in [("sector-flow","https://justhodl.ai/sector-flow.html","Early Momentum"),("index nav","https://justhodl.ai/index.html","sector-flow.html")]:
    for i in range(8):
        c,b=get(url)
        if c==200 and marker in b: print("OK  %-12s live (200) marker present"%label); break
        print("  %s attempt %d code=%s"%(label,i+1,c)); time.sleep(20)
    else: print("PENDING %s code=%s"%(label,c))
# schema confirm for defensive rendering
def pk(key):
    try: return json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}
sr=pk("data/sector-rotation.json")
print("\nsector-rotation: risk_appetite=%r"%(sr.get("risk_appetite")))
print("  macro_context type=%s sample=%s"%(type(sr.get("macro_context")).__name__, json.dumps(sr.get("macro_context"),default=str)[:140]))
ra=sr.get("rotation_alerts")
print("  rotation_alerts type=%s len=%s"%(type(ra).__name__, len(ra) if isinstance(ra,list) else "-"))
if isinstance(ra,list) and ra: print("   alert[0]=%s"%json.dumps(ra[0],default=str)[:200])
print("  summary=%s"%json.dumps(sr.get("summary"),default=str)[:200])
tf=pk("data/etf-true-flows.json")
infl=[x for x in tf.get("inflows",[]) if (x.get("net_flow_1d_usd") or 0)>0]
print("\netf-true-flows: n_inflows_nonzero=%s n_outflows=%s"%(len(infl),len(tf.get("outflows",[]))))
print("  category_rotation type=%s sample=%s"%(type(tf.get("category_rotation")).__name__, json.dumps(tf.get("category_rotation"),default=str)[:160]))
if infl: print("  top inflow:",json.dumps(infl[0],default=str)[:160])
