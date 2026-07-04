"""ops 2823 — confirm keyless BLS v1 works + discover Census EITS codes (time as predicate)."""
import os, json, urllib.request, urllib.parse
from datetime import datetime, timezone
R={"ops":2823,"ts":datetime.now(timezone.utc).isoformat()}
CEN=os.environ.get("CENSUS_API_KEY","")
# BLS v1 keyless
try:
    payload={"seriesid":["LNS14000000","CUUR0000SA0"],"startyear":"2024","endyear":"2026"}
    req=urllib.request.Request("https://api.bls.gov/publicAPI/v1/timeseries/data/",data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"})
    resp=json.loads(urllib.request.urlopen(req,timeout=40).read())
    ser=(resp.get("Results",{}) or {}).get("series",[])
    d0=ser[0]["data"] if ser and ser[0].get("data") else []
    R["BLS_v1_keyless"]={"status":resp.get("status"),"n_series":len(ser),"n_obs_series0":len(d0),
        "latest":(d0[0] if d0 else None)}
except Exception as e:
    R["BLS_v1_keyless"]={"exception":repr(e)[:160]}
# Census marts discovery (time as PREDICATE, not in get)
def discover(prog, tval="2026-04"):
    try:
        u="https://api.census.gov/data/timeseries/eits/%s?"%prog+urllib.parse.urlencode(
            {"get":"cell_value,category_code,data_type_code,seasonally_adj","time":tval,"for":"us","key":CEN})
        rows=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=30).read())
        hdr=rows[0]; ci={h:i for i,h in enumerate(hdr)}
        combos=sorted({(r[ci["category_code"]],r[ci["data_type_code"]],r[ci["seasonally_adj"]]) for r in rows[1:]})
        return {"n":len(rows)-1,"combos":combos[:40]}
    except Exception as e:
        eb=""
        try: eb=e.read().decode()[:160]
        except Exception: pass
        return {"exception":repr(e)[:120],"body":eb}
for prog in ("marts","resconst","advm3","m3"):
    R["CENSUS_"+prog]=discover(prog)
print(json.dumps(R,indent=1,default=str)[:3200])
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2823_probe2.json","w"),indent=1,default=str)
print("OPS 2823 COMPLETE")
