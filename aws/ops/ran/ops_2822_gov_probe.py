"""ops 2822 — READ-ONLY probe: why BLS returned 0, correct Census EITS codes,
BEA income line descriptions. Uses keys from injected secrets. Truncates output."""
import os, json, urllib.request, urllib.parse
from datetime import datetime, timezone
R={"ops":2822,"ts":datetime.now(timezone.utc).isoformat()}
BLS=os.environ.get("BLS_API_KEY",""); BEA=os.environ.get("BEA_API_KEY",""); CEN=os.environ.get("CENSUS_API_KEY","")
# ---- BLS: raw status + message ----
try:
    payload={"seriesid":["LNS14000000","CUUR0000SA0"],"startyear":"2025","endyear":"2026","registrationkey":BLS,"calculations":True}
    req=urllib.request.Request("https://api.bls.gov/publicAPI/v2/timeseries/data/",data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"})
    resp=json.loads(urllib.request.urlopen(req,timeout=40).read())
    ser=(resp.get("Results",{}) or {}).get("series",[])
    R["BLS"]={"status":resp.get("status"),"message":resp.get("message"),"n_series":len(ser),
              "first_series_has_data":bool(ser and ser[0].get("data")),
              "sample":(ser[0]["data"][0] if ser and ser[0].get("data") else None)}
except Exception as e:
    R["BLS"]={"exception":repr(e)[:200]}
# ---- Census: discover valid category/data_type codes for retail (marts) ----
try:
    u="https://api.census.gov/data/timeseries/eits/marts?"+urllib.parse.urlencode({"get":"cell_value,category_code,data_type_code,seasonally_adj,time","time":"2026-04","for":"us","key":CEN})
    rows=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=30).read())
    hdr=rows[0]; ci={h:i for i,h in enumerate(hdr)}
    cats=sorted({(r[ci["category_code"]],r[ci["data_type_code"]],r[ci["seasonally_adj"]]) for r in rows[1:]})
    R["CENSUS_marts"]={"http":"ok","n_rows":len(rows)-1,"cat_dtype_sa_combos":cats[:25]}
except Exception as e:
    try:
        eb=e.read().decode()[:200] if hasattr(e,"read") else ""
    except Exception: eb=""
    R["CENSUS_marts"]={"exception":repr(e)[:160],"body":eb}
# ---- Census: resconst discovery ----
try:
    u="https://api.census.gov/data/timeseries/eits/resconst?"+urllib.parse.urlencode({"get":"cell_value,category_code,data_type_code,seasonally_adj,time","time":"2026-04","for":"us","key":CEN})
    rows=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=30).read())
    hdr=rows[0]; ci={h:i for i,h in enumerate(hdr)}
    R["CENSUS_resconst"]={"combos":sorted({(r[ci["category_code"]],r[ci["data_type_code"]],r[ci["seasonally_adj"]]) for r in rows[1:]})[:20]}
except Exception as e:
    R["CENSUS_resconst"]={"exception":repr(e)[:160]}
# ---- Census durable goods program name probe (advm3 vs m3) ----
for prog in ("advm3","m3"):
    try:
        u="https://api.census.gov/data/timeseries/eits/%s?"%prog+urllib.parse.urlencode({"get":"cell_value,category_code,data_type_code,time","time":"2026-04","for":"us","key":CEN})
        rows=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=30).read())
        hdr=rows[0]; ci={h:i for i,h in enumerate(hdr)}
        R["CENSUS_%s"%prog]={"combos":sorted({(r[ci["category_code"]],r[ci["data_type_code"]]) for r in rows[1:]})[:20]}
    except Exception as e:
        R["CENSUS_%s"%prog]={"exception":repr(e)[:120]}
# ---- BEA T20600 income line descriptions ----
try:
    yrs="2025,2026"
    u="https://apps.bea.gov/api/data?"+urllib.parse.urlencode({"UserID":BEA,"method":"GetData","datasetname":"NIPA","TableName":"T20600","Frequency":"M","Year":yrs,"ResultFormat":"json"})
    d=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=40).read())
    data=((d.get("BEAAPI",{}) or {}).get("Results",{}) or {}).get("Data",[]) or []
    lines=sorted({(row.get("LineNumber"),row.get("LineDescription")) for row in data},key=lambda x:int(x[0]) if str(x[0]).isdigit() else 999)
    R["BEA_T20600_lines"]=[{"ln":l,"desc":dsc} for l,dsc in lines[:30]]
except Exception as e:
    R["BEA_T20600_lines"]={"exception":repr(e)[:160]}
print(json.dumps(R,indent=1,default=str))
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/2822_gov_probe.json","w"),indent=1,default=str)
print("OPS 2822 COMPLETE")
