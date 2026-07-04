"""ops 2822 — diagnose BLS (empty), Census (codes), BEA-income (line match). Read-only."""
import os, json, urllib.request, urllib.parse
from datetime import datetime, timezone
R={"ops":2822,"ts":datetime.now(timezone.utc).isoformat()}
BLS=os.environ.get("BLS_API_KEY",""); CEN=os.environ.get("CENSUS_API_KEY",""); BEA=os.environ.get("BEA_API_KEY","")
# ---- BLS: raw status/message ----
try:
    payload={"seriesid":["LNS14000000","CUUR0000SA0","JTS000000000000000JOL"],"startyear":"2024","endyear":"2026","registrationkey":BLS,"calculations":True}
    req=urllib.request.Request("https://api.bls.gov/publicAPI/v2/timeseries/data/",data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"})
    d=json.loads(urllib.request.urlopen(req,timeout=40).read())
    ser=(d.get("Results",{}) or {}).get("series",[])
    R["BLS"]={"status":d.get("status"),"message":d.get("message"),"n_series":len(ser),
              "per_series":[{"id":s.get("seriesID"),"n_data":len(s.get("data",[]))} for s in ser]}
except Exception as e:
    R["BLS"]={"exception":repr(e)[:200]}
# ---- Census: discovery (no filters, one month) to see valid codes ----
try:
    u="https://api.census.gov/data/timeseries/eits/marts?"+urllib.parse.urlencode({"get":"cell_value,category_code,data_type_code,seasonally_adj,time","time":"2026-04","for":"us","key":CEN})
    rows=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=30).read())
    hdr=rows[0]; cats=sorted({r[hdr.index("category_code")] for r in rows[1:]}); dts=sorted({r[hdr.index("data_type_code")] for r in rows[1:]})
    R["CENSUS_marts"]={"http":200,"n_rows":len(rows)-1,"category_codes":cats[:25],"data_type_codes":dts[:25]}
except urllib.error.HTTPError as he:
    R["CENSUS_marts"]={"http":he.code,"body":he.read().decode("utf-8","ignore")[:300]}
except Exception as e:
    R["CENSUS_marts"]={"exception":repr(e)[:200]}
# probe resconst + advm3 program names
for prog in ("resconst","advm3","m3"):
    try:
        u="https://api.census.gov/data/timeseries/eits/%s?"%prog+urllib.parse.urlencode({"get":"cell_value,category_code,data_type_code,time","time":"2026-04","for":"us","key":CEN})
        rows=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=30).read())
        hdr=rows[0]
        R["CENSUS_"+prog]={"http":200,"n":len(rows)-1,"cats":sorted({r[hdr.index("category_code")] for r in rows[1:]})[:20],"dts":sorted({r[hdr.index("data_type_code")] for r in rows[1:]})[:15]}
    except urllib.error.HTTPError as he:
        R["CENSUS_"+prog]={"http":he.code,"body":he.read().decode("utf-8","ignore")[:150]}
    except Exception as e:
        R["CENSUS_"+prog]={"exception":repr(e)[:150]}
# ---- BEA T20600 line descriptions ----
try:
    p={"UserID":BEA,"method":"GetData","datasetname":"NIPA","TableName":"T20600","Frequency":"M","Year":"2026","ResultFormat":"json"}
    d=json.loads(urllib.request.urlopen(urllib.request.Request("https://apps.bea.gov/api/data?"+urllib.parse.urlencode(p),headers={"User-Agent":"jh"}),timeout=40).read())
    data=((d.get("BEAAPI",{}) or {}).get("Results",{}) or {}).get("Data",[])
    lines=[]
    seen=set()
    for row in data:
        ld=row.get("LineDescription")
        if ld and ld not in seen: seen.add(ld); lines.append(ld)
    R["BEA_T20600_lines"]=lines[:30]
except Exception as e:
    R["BEA_T20600_lines"]={"exception":repr(e)[:200]}
print(json.dumps(R,indent=1,default=str)[:3500])
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2822_gov_diag.json","w"),indent=1,default=str)
print("OPS 2822 COMPLETE")
