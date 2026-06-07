import json, urllib.request, time
out={}
for url in ["https://justhodl-data-proxy.raafouis.workers.dev/brain-debug","https://api.justhodl.ai/brain-debug"]:
    try:
        r=urllib.request.urlopen(urllib.request.Request(url+"?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0","Origin":"https://justhodl.ai"}),timeout=20)
        out[url.split("//")[1].split("/")[0]]=json.loads(r.read().decode())
        break
    except urllib.error.HTTPError as e: out[url.split("//")[1].split("/")[0]]={"http":e.code}
    except Exception as e: out[url.split("//")[1].split("/")[0]]=str(e)[:60]
open("aws/ops/reports/1382_bd.json","w").write(json.dumps(out,indent=2,default=str));print("done")
