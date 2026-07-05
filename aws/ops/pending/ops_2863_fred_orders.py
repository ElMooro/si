"""ops 2863 — probe FRED for clean industrial-ORDERS leads (Japan machinery, Germany, others)
as robust alternatives to fragile JMTBA/SIA scrapes."""
import os, json, urllib.request, boto3
from datetime import datetime, timezone
R={"ops":2863,"ts":datetime.now(timezone.utc).isoformat()}
lam=boto3.client("lambda",region_name="us-east-1")
FRED=lam.get_function_configuration(FunctionName="justhodl-china-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
def fred(sid):
    try:
        u="https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=14"%(sid,FRED)
        d=json.loads(urllib.request.urlopen(u,timeout=15).read())
        o=[x for x in d.get("observations",[]) if x.get("value") not in(".","",None)]
        if not o: return {"empty":True}
        cur=float(o[0]["value"]); ya=float(o[12]["value"]) if len(o)>12 else None
        return {"latest":cur,"date":o[0]["date"],"yoy_pct":round((cur-ya)/abs(ya)*100,1) if ya else None}
    except Exception as e: return {"err":str(e)[:60]}
# candidate clean ORDERS series on FRED (OECD MEI / national)
cands={
 "Japan_mfg_orders_MEI":"JPNPRMNTO01IXOBSAM",
 "Japan_mfg_orders_alt":"JPNPRMNTO01GYSAM",
 "Germany_mfg_orders":"DEUPRMNTO01IXOBM",
 "Germany_mfg_orders_yoy":"DEUPRMNTO01GYSAM",
 "US_durable_new_orders":"DGORDER",
 "US_core_capex_orders":"NEWORDER",
 "Korea_mfg_orders":"KORPRMNTO01IXOBM",
 "OECD_mfg_orders":"OECDPRMNTO01IXOBSAM",
 "Japan_machinery_tool":"JPNPRINTO01GYSAM",
}
for name,sid in cands.items():
    R.setdefault("fred",{})[name]={"sid":sid, **fred(sid)}
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2863_fred_orders.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2863 COMPLETE")
