import json, urllib.request, time
out={}
UID="9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"
# does api.justhodl.ai route to the worker? test GET /brain there
for host in ["api.justhodl.ai","justhodl.ai/api","data.justhodl.ai"]:
    try:
        u="https://"+host+"/brain?uid="+UID+"&t=%d"%int(time.time())
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"}),timeout=10)
        out[host]={"status":r.getcode(),"is_brain":"notes" in r.read().decode()[:200]}
    except urllib.error.HTTPError as e: out[host]={"status":e.code}
    except Exception as e: out[host]={"err":str(e)[:50]}
open("aws/ops/reports/1373_api.json","w").write(json.dumps(out,indent=2,default=str));print("done")
