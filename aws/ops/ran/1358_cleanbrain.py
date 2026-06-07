import json, urllib.request, time
out={}
BASE="https://justhodl-data-proxy.raafouis.workers.dev/brain"
def get():
    req=urllib.request.Request(BASE+"?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"})
    return json.loads(urllib.request.urlopen(req,timeout=15).read().decode())
g=get()
notes=g.get("notes",[])
out["current_notes"]=[{"id":n.get("id"),"cat":n.get("cat"),"len":len(n.get("text","")),"preview":n.get("text","")[:40]} for n in notes]
# remove the test junk: ids 'big1','nf1','perm1' OR any note that is just repeated 'MACRO NOTE'/'save'/'PERMANENCE'/'no-preflight'
JUNK_IDS={"big1","nf1","perm1","seed1"}
def is_junk(n):
    t=n.get("text","")
    if n.get("id") in JUNK_IDS: return True
    if t.count("MACRO NOTE.")>5: return True
    if "PERMANENCE TEST" in t or "no-preflight save test" in t or "save path test" in t: return True
    return False
clean=[n for n in notes if not is_junk(n)]
out["removed"]=len(notes)-len(clean); out["remaining"]=len(clean)
# write back clean (pin = savetest1234 was the bootstrap pin used in tests)
payload={"notes":clean,"updated_at":time.strftime("%Y-%m-%dT%H:%M:%SZ"),"_pin":"savetest1234"}
try:
    req=urllib.request.Request(BASE,data=json.dumps(payload).encode(),
        headers={"Content-Type":"text/plain","Origin":"https://justhodl.ai","User-Agent":"Mozilla/5.0 Chrome/126"},method="PUT")
    out["write"]=json.loads(urllib.request.urlopen(req,timeout=20).read().decode())
except urllib.error.HTTPError as e: out["write"]={"http":e.code,"body":e.read().decode()[:80]}
open("aws/ops/reports/1358_cb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
