import urllib.request, boto3, json
s3=boto3.client("s3","us-east-1")
req=urllib.request.Request("https://justhodl.ai/sector-flow.html",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Cache-Control":"no-cache"})
html=urllib.request.urlopen(req,timeout=30).read().decode("utf-8","ignore")
for m in ["renderDarkPool","renderCycleFlow","Cycle vs Flow","dark-pool","liquidity"]:
    print(f"  {'FOUND' if m in html else 'MISSING':7} {m}")
print("page bytes:",len(html))
print("=== sector-flow-state.json feed ===")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/sector-flow-state.json")["Body"].read())
    print("keys:",list(d.keys()))
    print("generated_at:",d.get("generated_at"),"| liquidity:",d.get("liquidity"),"| phase:",d.get("phase"))
    print("overweight:",d.get("overweight"),"| underweight:",d.get("underweight"))
    secs=d.get("sectors") or d.get("by_sector") or []
    if isinstance(secs,list) and secs: print("sector[0]:",json.dumps(secs[0])[:340])
    elif isinstance(secs,dict): 
        k=list(secs.keys())[:1]; print("sector sample:",json.dumps({k[0]:secs[k[0]]})[:340] if k else "{}")
except Exception as e: print("FEED ERR",str(e)[:80])
print("DONE 2502")
