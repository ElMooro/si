import json, urllib.request, time
out={}
BASE="https://justhodl-data-proxy.raafouis.workers.dev/brain"
def get(q=""):
    try:
        req=urllib.request.Request(BASE+"?t=%d%s"%(int(time.time()),q),headers={"User-Agent":"Mozilla/5.0"})
        return json.loads(urllib.request.urlopen(req,timeout=15).read().decode())
    except urllib.error.HTTPError as e: return {"http":e.code}
    except Exception as e: return {"err":str(e)[:60]}
# 1) read owner brain (current state)
g0=get()
out["owner_pin_set"]=g0.get("pin_set"); out["owner_notes"]=len(g0.get("notes",[]))
# 2) write a test note WITH the right pin (only if pin already set; else bootstrap with a test pin we won't keep)
#    To avoid hijacking Khalid's real pin, only test write to a throwaway test user uid.
TEST="testuser_perm_001"
def put(payload,pin,uid):
    try:
        req=urllib.request.Request(BASE+"?uid="+uid,data=json.dumps(payload).encode(),
            headers={"Content-Type":"application/json","X-Brain-Pin":pin},method="PUT")
        return json.loads(urllib.request.urlopen(req,timeout=15).read().decode())
    except urllib.error.HTTPError as e: return {"http":e.code,"body":e.read().decode()[:80]}
    except Exception as e: return {"err":str(e)[:60]}
note={"notes":[{"id":"perm1","cat":"rule","text":"PERMANENCE TEST — this note should persist and be fully visible.","created":int(time.time()*1000),"pinned":True}],"updated_at":"2026-06-06"}
w=put(note,"testpin99",TEST)
out["test_write"]=w
time.sleep(1)
# read it back for that user
gt=get("&uid="+TEST)
out["test_readback_notes"]=len(gt.get("notes",[]))
out["test_note_visible"]=any(n.get("id")=="perm1" for n in gt.get("notes",[]))
out["test_scope"]=gt.get("scope")
# confirm owner brain is UNAFFECTED (isolation)
g1=get()
out["owner_notes_after"]=len(g1.get("notes",[]))
out["isolation_ok"]=(out["owner_notes"]==out["owner_notes_after"])
open("aws/ops/reports/1348_bt.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
