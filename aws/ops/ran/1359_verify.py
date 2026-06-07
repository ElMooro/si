import json, urllib.request, time
out={}
req=urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/brain?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
g=json.loads(urllib.request.urlopen(req,timeout=15).read().decode())
out["n_notes"]=len(g.get("notes",[]))
out["pin_set"]=g.get("pin_set")
out["notes_preview"]=[n.get("text","")[:30] for n in g.get("notes",[])]
open("aws/ops/reports/1359_v.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
