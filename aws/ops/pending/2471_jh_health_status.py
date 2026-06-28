import boto3, json
from datetime import datetime, timezone
def p(*a): print(*a, flush=True)
s3=boto3.client("s3","us-east-1"); sch=boto3.client("scheduler","us-east-1")
now=datetime.now(timezone.utc)
def age_h(k):
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=k); return round((now-h["LastModified"]).total_seconds()/3600,1)
    except Exception: return None
p("=== new crypto engines: out-age + schedule ===")
for fn,key,scd in [("crypto-etf-flows","data/crypto-etf-flows.json","crypto-etf-flows-sched"),
                   ("hyperliquid-perps","data/hyperliquid-perps.json","hyperliquid-perps-sched"),
                   ("crypto-gex","data/crypto-gex.json","crypto-gex-sched")]:
    a=age_h(key)
    try: st=sch.get_schedule(Name=scd); se="%s %s"%(st["State"],st["ScheduleExpression"])
    except Exception as e: se="MISSING"
    p("  %-20s out=%sh sched=%s"%(fn,a,se))
p("=== Hyperliquid maturation ===")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hyperliquid-perps.json")["Body"].read())
    p("  hist_n=%s regime=%s oi_chg24h=%s liq=%s"%(d.get("history_n"),d.get("leverage_regime"),d.get("total_oi_chg_24h_pct"),d.get("liq_pressure_proxy")))
except Exception as e: p("  err",str(e)[:60])
p("=== fleet freshness manifest ===")
try:
    raw=s3.get_object(Bucket="justhodl-dashboard-live",Key="_freshness-manifest.json")["Body"].read()
    fm=json.loads(raw); p("  bytes=%d keys=%s"%(len(raw),list(fm.keys())[:10]))
    items=fm.get("engines") or fm.get("files") or fm.get("stale") or fm.get("entries") or []
    if isinstance(items,list) and items:
        def ah(it): return it.get("age_hours") or it.get("age_h") or it.get("age")
        stale=[(it.get("name") or it.get("key"),ah(it)) for it in items if (it.get("fresh") is False) or (isinstance(ah(it),(int,float)) and ah(it)>49)]
        p("  tracked=%d stale=%d"%(len(items),len(stale)))
        for nm,a in sorted(stale,key=lambda x:-(x[1] or 0))[:18]: p("    STALE %sh %s"%(a,nm))
    else: p("  counts:",{k:fm.get(k) for k in ("stale_count","fresh_count","total","n_stale") if k in fm},"sample",json.dumps(fm)[:220])
except Exception as e: p("  manifest err:",str(e)[:80])
p("DONE 2471")
