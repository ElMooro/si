"""1271 — probe FMP shares-float endpoints to find the working one + fields."""
import json, urllib.request, boto3
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(url):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"JustHodl/1.0"})
        with urllib.request.urlopen(req,timeout=15) as r: return json.loads(r.read().decode())
    except Exception as e: return {"_err":str(e)[:120]}
out={}
base="https://financialmodelingprep.com/stable"
tries={
 "historical-shares-float":f"{base}/historical-shares-float?symbol=SPY&apikey={FMP}",
 "shares-float":f"{base}/shares-float?symbol=SPY&apikey={FMP}",
 "shares-float-all":f"{base}/shares-float?apikey={FMP}&limit=1",
 "etf-info":f"{base}/etf-info?symbol=SPY&apikey={FMP}",
 "etf-holdings-dates":f"{base}/etf-holdings-dates?symbol=SPY&apikey={FMP}",
 "key-metrics-spy":f"{base}/key-metrics-ttm?symbol=SPY&apikey={FMP}",
}
for name,url in tries.items():
    r=get(url)
    if isinstance(r,list):
        out[name]={"type":"list","len":len(r),"sample":r[0] if r else None}
    elif isinstance(r,dict):
        out[name]={"type":"dict","keys":list(r.keys())[:10],"sample":r}
    else:
        out[name]=str(r)[:120]
boto3.client("s3",region_name="us-east-1").put_object(Bucket="justhodl-dashboard-live",
    Key="data/_probe_shares.json",Body=json.dumps(out,default=str).encode())
open("aws/ops/reports/1271_shares_probe.json","w").write(json.dumps(out,indent=2,default=str)[:4000])
print("done")
