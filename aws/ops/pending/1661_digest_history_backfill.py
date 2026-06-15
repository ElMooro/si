import json, time, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
B="justhodl-dashboard-live"
def compact(ctx, date):
    pf=ctx.get("portfolio",{}); pr=ctx.get("predictions",{})
    return {"date":date,"realized_pnl_usd":pf.get("realized_pnl_usd",0),"win_rate_pct":pf.get("win_rate_pct",0),
            "n_open":pf.get("n_open",0),"n_closed":pf.get("n_closed",0),"n_predictions":pr.get("n_today",0),
            "n_calibration":(ctx.get("calibration_state") or {}).get("n_data_points",0),
            "n_validated":(ctx.get("validation") or {}).get("n_validated",0),
            "total_alerts":sum((pr.get("alert_distribution") or {}).values()),
            "top_retail_pct":max([abs(r.get("velocity_pct") or 0) for r in ctx.get("retail_sentiment_surges",[])] or [0])}
# 1) backfill from dated history
paginator=s3.get_paginator("list_objects_v2"); series={}
n_files=0
for pg in paginator.paginate(Bucket=B, Prefix="data/digest-trends-ai-history/"):
    for o in pg.get("Contents",[]):
        k=o["Key"]
        if not k.endswith(".json"): continue
        date=k.split("/")[-1].replace(".json","")
        try:
            d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
            ctx=d.get("system_state_snapshot") or {}
            if ctx: series[date]=compact(ctx, date); n_files+=1
        except Exception as e: print("skip",k,str(e)[:40])
print(f"backfilled {n_files} dated files -> {len(series)} unique days")
# 2) invoke engine to append today + write rolling file
n0=datetime.now(timezone.utc)
lam.invoke(FunctionName="justhodl-digest-trends-ai", InvocationType="Event"); print("invoked digest engine")
# if engine hasn't written rolling file yet, seed it from backfill now (engine will merge today on its run)
if series:
    ser=sorted(series.values(), key=lambda r:r["date"])[-120:]
    s3.put_object(Bucket=B,Key="data/digest-trends-history.json",
        Body=json.dumps({"generated_at":n0.isoformat(),"series":ser},default=str).encode(),
        ContentType="application/json",CacheControl="public, max-age=600")
    print(f"seeded rolling history with {len(ser)} days; sample last:", json.dumps(ser[-1]))
# 3) verify engine run merged (poll)
for i in range(12):
    time.sleep(20)
    try:
        h=json.loads(s3.get_object(Bucket=B,Key="data/digest-trends-history.json")["Body"].read())
        gen=datetime.fromisoformat(h.get("generated_at"))
        if gen>=n0.replace(microsecond=0):
            ser=h.get("series",[])
            print(f"\nVERIFIED engine merged: {len(ser)} days, latest {ser[-1]['date'] if ser else '—'}")
            print("fields:", list(ser[-1].keys()) if ser else [])
            break
    except Exception: pass
    print(f"  ...{(i+1)*20}s waiting for engine merge")
