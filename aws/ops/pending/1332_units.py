import json, urllib.request, urllib.parse
out={}
FRED="2f057499936072679d8843d7fce99989"
def fred(sid,n=3):
    try:
        p={"series_id":sid,"api_key":FRED,"file_type":"json","sort_order":"desc","limit":str(n)}
        u="https://api.stlouisfed.org/fred/series/observations?"+urllib.parse.urlencode(p)
        req=urllib.request.Request(u,headers={"User-Agent":"JustHodl/1.0"})
        d=json.loads(urllib.request.urlopen(req,timeout=15).read().decode())
        return [(o["date"],o["value"]) for o in d.get("observations",[])]
    except urllib.error.HTTPError as e: return f"HTTP {e.code}"
    except Exception as e: return str(e)[:80]
# WALCL (Fed assets) and WRESBAL (reserves) — what units?
out["WALCL"]=fred("WALCL")        # expect $millions, ~6,600,000
out["WRESBAL"]=fred("WRESBAL")    # expect $millions, ~3,000,000
# TGCR variants
for sid in ["TGCR","SOFRVOL","BGCR"]:
    out[sid]=fred(sid,1)
open("aws/ops/reports/1332_units.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
