"""ops 2833 — discovery probe for supercore (BLS series+weights), retail control
group + core capex (Census), and freight proxy (FRED). Read-only."""
import os, json, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3
R={"ops":2833,"ts":datetime.now(timezone.utc).isoformat()}
BLS=os.environ.get("BLS_API_KEY",""); CEN=os.environ.get("CENSUS_API_KEY","")
lam=boto3.client("lambda",region_name="us-east-1")
try: FRED=lam.get_function_configuration(FunctionName="justhodl-china-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
except Exception: FRED=""
UA={"User-Agent":"Mozilla/5.0 (JustHodl)"}

# 1) BLS supercore candidate series (does a published services-less-shelter exist?)
def bls(ids):
    p={"seriesid":ids,"startyear":"2024","endyear":"2026","registrationkey":BLS,"calculations":True}
    req=urllib.request.Request("https://api.bls.gov/publicAPI/v2/timeseries/data/",data=json.dumps(p).encode(),headers={"Content-Type":"application/json"})
    resp=json.loads(urllib.request.urlopen(req,timeout=45).read())
    out={}
    for s in (resp.get("Results",{}) or {}).get("series",[]):
        d=s.get("data",[])
        if d:
            l=d[0]; calc=(l.get("calculations",{}) or {}).get("pct_changes",{}) or {}
            out[s["seriesID"]]={"val":l.get("value"),"per":l.get("year")+l.get("period"),"yoy":calc.get("12")}
        else: out[s["seriesID"]]={"empty":True}
    return {"status":resp.get("status"),"series":out}
R["bls_supercore_candidates"]=bls([
 "CUSR0000SASL2RS",  # services less rent of shelter
 "CUSR0000SASLE",    # services less energy services (=core services)
 "CUSR0000SAH1",     # shelter
 "CUSR0000SAM2",     # medical care services
 "CUSR0000SAS4",     # transportation services
 "CUSR0000SEHC",     # owners equiv rent
 "CUSR0000SEHA",     # rent of primary residence
])
# 2) BLS relative importance file availability
for url in ["https://www.bls.gov/cpi/tables/relative-importance/2025.xlsx",
            "https://www.bls.gov/cpi/tables/relative-importance/2024.xlsx"]:
    try:
        req=urllib.request.Request(url,headers=UA); raw=urllib.request.urlopen(req,timeout=25).read()
        R.setdefault("bls_ri_file",{})[url]={"bytes":len(raw),"head":raw[:8].hex()}
        break
    except Exception as e:
        R.setdefault("bls_ri_file",{})[url]="err "+str(e)[:60]

# 3) Census marts ALL categories (find control group + gasoline 447 + bldg 444)
try:
    u="https://api.census.gov/data/timeseries/eits/marts?"+urllib.parse.urlencode({"get":"cell_value,category_code,data_type_code,seasonally_adj","time":"2026-05","for":"us","key":CEN})
    rows=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers=UA),timeout=30).read())
    hdr=rows[0]; ci={h:i for i,h in enumerate(hdr)}
    cats=sorted({r[ci["category_code"]] for r in rows[1:]})
    R["census_marts_categories"]=cats
except Exception as e:
    R["census_marts_categories"]={"err":str(e)[:80]}
# 4) Census m3/advm3 with time_slot_id (core capex = nondef ex-air new orders)
for prog in ("advm3","m3"):
    try:
        u="https://api.census.gov/data/timeseries/eits/%s?"%prog+urllib.parse.urlencode({"get":"cell_value,category_code,data_type_code,time_slot_id,seasonally_adj","time":"2026-05","for":"us","key":CEN})
        rows=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers=UA),timeout=30).read())
        hdr=rows[0]; ci={h:i for i,h in enumerate(hdr)}
        combos=sorted({(r[ci["category_code"]],r[ci["data_type_code"]],r[ci.get("time_slot_id",0)] if "time_slot_id" in ci else "") for r in rows[1:]})
        R["census_"+prog]={"n":len(rows)-1,"combos":combos[:30]}
    except Exception as e:
        eb=""
        try: eb=e.read().decode()[:120]
        except Exception: pass
        R["census_"+prog]={"err":str(e)[:80],"body":eb}
# 5) FRED freight proxies
def fred(sid):
    try:
        u="https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=14"%(sid,FRED)
        obs=[o for o in json.loads(urllib.request.urlopen(u,timeout=20).read()).get("observations",[]) if o.get("value") not in(".","",None)]
        if not obs: return {"empty":True}
        cur=float(obs[0]["value"]); yago=float(obs[12]["value"]) if len(obs)>12 else None
        return {"val":cur,"date":obs[0]["date"],"yoy_pct":round((cur-yago)/yago*100,2) if yago else None}
    except Exception as e: return {"err":str(e)[:60]}
R["fred_freight"]={s:fred(s) for s in ["TSIFRGHT","FRGSHPUSM649NCIS","RAILFRTINTERMODAL","TRUCKD11"]}
print(json.dumps(R,indent=1,default=str)[:3500])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2833_probe3.json","w"),indent=1,default=str)
print("OPS 2833 COMPLETE")
