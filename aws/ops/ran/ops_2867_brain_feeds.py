"""ops 2867 — mine data/brain.json for canary notes + capture the 6 early-warning feeds' schemas."""
import os, json, re, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":2867,"ts":datetime.now(timezone.utc).isoformat()}
def gj(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"__err__":str(e)[:80]}
# 1) BRAIN — search notes for canary/leading/early-warning content
brain=gj("data/brain.json")
notes=[]
if isinstance(brain,dict):
    for k in ("notes","entries","items","records","data"):
        if isinstance(brain.get(k),list): notes=brain[k]; break
elif isinstance(brain,list): notes=brain
R["brain_total_notes"]=len(notes)
pat=re.compile(r"canary|canaries|leading indicator|early.?warning|tripwire|crack first|front.?run|recession lead|inversion|divergence|watch (for|list)",re.I)
hits=[]
for n in notes:
    txt=n if isinstance(n,str) else json.dumps(n,ensure_ascii=False) if isinstance(n,dict) else str(n)
    if pat.search(txt):
        # pull a readable snippet
        s=(n.get("text") or n.get("note") or n.get("content") or n.get("body") or txt) if isinstance(n,dict) else txt
        hits.append(str(s)[:240])
R["brain_canary_note_count"]=len(hits)
R["brain_canary_notes"]=hits[:25]
# sample a note's shape
R["brain_note_shape"]=list(notes[0].keys()) if (notes and isinstance(notes[0],dict)) else ("string-notes" if notes else "empty")
# 2) SIX FEEDS — schema capture
feeds={"canary-grid":"data/canary-grid.json","crisis-canaries":"data/crisis-canaries.json",
       "leading-markets":"data/leading-markets.json","dollar-radar":"data/dollar-radar.json",
       "vol-radar":"data/vol-radar.json","alert-sentinel":"data/alert-sentinel.json"}
for name,k in feeds.items():
    d=gj(k)
    if "__err__" in d: R.setdefault("feeds",{})[name]={"err":d["__err__"]}; continue
    top=list(d.keys())[:20] if isinstance(d,dict) else "list"
    # find the primary signal/canary array + its element shape
    arr_key=None; elem=None
    if isinstance(d,dict):
        for kk,vv in d.items():
            if isinstance(vv,list) and vv and isinstance(vv[0],dict):
                arr_key=kk; elem=list(vv[0].keys())[:12]; break
    R.setdefault("feeds",{})[name]={"top_keys":top,"array_key":arr_key,"elem_keys":elem,
        "headline":(d.get("headline") or d.get("verdict") or d.get("summary") or d.get("read") if isinstance(d,dict) else None),
        "as_of":(d.get("generated_at") or d.get("as_of") or d.get("ts") if isinstance(d,dict) else None)}
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2867_brain_feeds.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2867 COMPLETE")
