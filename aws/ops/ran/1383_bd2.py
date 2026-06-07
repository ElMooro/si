import json, urllib.request, time
out={}
try:
    r=urllib.request.urlopen(urllib.request.Request("https://api.justhodl.ai/brain-debug?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0","Origin":"https://justhodl.ai"}),timeout=25)
    out=json.loads(r.read().decode())
except urllib.error.HTTPError as e: out={"http":e.code,"body":e.read().decode()[:100]}
except Exception as e: out={"err":str(e)[:80]}
open("aws/ops/reports/1383_bd.json","w").write(json.dumps(out,indent=2,default=str));print("done")
