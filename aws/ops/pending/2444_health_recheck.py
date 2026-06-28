import boto3, json, time
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1"); sch=boto3.client("scheduler","us-east-1")
now=datetime.now(timezone.utc)
def age_h(k):
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=k)
        return round((now-h["LastModified"]).total_seconds()/3600,1)
    except Exception as e: return None
print("=== (1) 3 new crypto engines: freshness + schedule ===")
for fn,key,sched in [("crypto-etf-flows","data/crypto-etf-flows.json","crypto-etf-flows-sched"),
                     ("hyperliquid-perps","data/hyperliquid-perps.json","hyperliquid-perps-sched"),
                     ("crypto-gex","data/crypto-gex.json","crypto-gex-sched")]:
    a=age_h(key)
    try: st=sch.get_schedule(Name=sched); se=st["State"]; ex=st["ScheduleExpression"]
    except Exception as e: se="MISSING"; ex=str(e)[:30]
    print("  %-20s out=%sh | sched=%s %s"%(fn,a,se,ex))
print("=== (2) Hyperliquid regime maturation ===")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hyperliquid-perps.json")["Body"].read())
    print("  hist_n=%s | regime=%s | total_oi_chg_24h=%s | btc_oi_chg_24h=%s | liq=%s"%(
        d.get("history_n"),d.get("leverage_regime"),d.get("total_oi_chg_24h_pct"),d.get("btc_oi_chg_24h_pct"),d.get("liq_pressure_proxy")))
except Exception as e: print("  err",str(e)[:60])
print("=== (3) fleet freshness manifest (authoritative stale view) ===")
try:
    fm=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="_freshness-manifest.json")["Body"].read())
    gen=fm.get("generated_at") or fm.get("generated"); 
    items=fm.get("engines") or fm.get("files") or fm.get("items") or fm
    print("  manifest gen:",gen,"| keys:",list(fm.keys())[:8])
    # try to surface stale entries generically
    stale=[]
    if isinstance(items,list):
        for it in items:
            ah=it.get("age_hours") or it.get("age_h") or it.get("age")
            nm=it.get("name") or it.get("key") or it.get("engine")
            fresh=it.get("fresh") if "fresh" in it else (None if ah is None else ah<49)
            if fresh is False or (isinstance(ah,(int,float)) and ah>49): stale.append((nm,ah))
        print("  total tracked:",len(items),"| stale(>49h):",len(stale))
        for nm,ah in sorted(stale,key=lambda x:-(x[1] or 0))[:20]: print("    STALE %sh  %s"%(ah,nm))
    else:
        print("  manifest shape:",type(items).__name__, str(items)[:200])
except Exception as e: print("  manifest err:",str(e)[:80])
print("DONE 2444")
