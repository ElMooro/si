import urllib.request, urllib.parse, json
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"; FRED="2f057499936072679d8843d7fce99989"
def fmp(path,params):
    params["apikey"]=FMP
    u=f"https://financialmodelingprep.com/stable/{path}?"+urllib.parse.urlencode(params)
    return json.loads(urllib.request.urlopen(u,timeout=25).read())
def fred(sid):
    u="https://api.stlouisfed.org/fred/series/observations?"+urllib.parse.urlencode({"series_id":sid,"api_key":FRED,"file_type":"json","observation_start":"2025-09-01","limit":100000})
    j=json.loads(urllib.request.urlopen(u,timeout=25).read())
    return [(o["date"],float(o["value"])) for o in j.get("observations",[]) if o.get("value") not in (".","",None)]
for etf in ["USO","UNG"]:
    try:
        h=fmp("historical-price-eod/full",{"symbol":etf})
        rows=h if isinstance(h,list) else (h.get("historical") if isinstance(h,dict) else None)
        print(etf,"type",type(h).__name__,"n",len(rows) if rows else 0)
        if rows: print("   sample:",json.dumps(rows[0])[:140])
    except Exception as e: print(etf,"ERR",str(e)[:80])
for s in ["DCOILWTICO","DHHNGSP","DGASUSGULF"]:
    try:
        o=fred(s); print(s,"n",len(o),"last",o[-1] if o else None)
    except Exception as e: print(s,"ERR",str(e)[:70])
print("DONE 2480")
