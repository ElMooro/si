import urllib.request, json, boto3
def get(url,t=25):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 JustHodl"}),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return "ERR:"+str(getattr(e,'code',e))
for page,markers in [("plumbing.html",["openChart(","plumbing-history.json","History (click","miniSpark","chartModal"]),
                     ("sovereign.html",["Settlement Fails","CR_FAILS","fetch_fails","failBars","settlement-fails.json"])]:
    h=get("https://justhodl.ai/"+page)
    if h.startswith("ERR"): print(f"{page}: {h}"); continue
    print(f"{page}: {len(h)} bytes |", {m:(m in h) for m in markers})
# data files reachable + shapes
s3=boto3.client("s3",region_name="us-east-1")
for k in ["data/plumbing-history.json","data/settlement-fails.json"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
        if "indicators" in d: print(f"{k}: {len(d['indicators'])} indicators, {len(d.get('crises',[]))} crises")
        else: print(f"{k}: regime={d['signal']['regime']} ust_combined={d['headline']['combined_bn']} ftd_hist_n={len(next(c for c in d['classes'] if c['key']=='ust_ex_tips')['combined'])}")
    except Exception as e: print(k,"ERR",e)
