import boto3, datetime, json
s3=boto3.client("s3","us-east-1"); logs=boto3.client("logs","us-east-1")
now=datetime.datetime.now(datetime.timezone.utc); B="justhodl-dashboard-live"
checks={
 "justhodl-dealer-gex":"data/dealer-gex.json","justhodl-options-gamma":"data/options-gamma.json",
 "justhodl-etf-flows":"data/etf-flows.json","justhodl-polygon-options-flow":"data/polygon-options-flow.json",
 "justhodl-vol-regime":"data/vol-regime.json","justhodl-rotation-chain":"data/rotation-chains.json",
 "justhodl-exchange-flows":"data/exchange-flows.json","justhodl-event-flow-monitor":"data/event-flow-health.json",
 "justhodl-bond-vol":"data/bond-vol.json","justhodl-vol-target-unwind":"data/vol-target-unwind.json"}
def last_log(fn):
    try:
        lg="/aws/lambda/"+fn
        st=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        if not st: return "no logs"
        ev=logs.get_log_events(logGroupName=lg,logStreamName=st[0]["logStreamName"],limit=10,startFromHead=False)["events"]
        msgs=[e["message"].strip().replace("\n"," ")[:110] for e in ev if e["message"].strip()]
        return " || ".join(msgs[-3:])
    except Exception as e: return "log err "+str(e)[:50]
print("=== REVIVED ENGINE HEALTH (output freshness) ===")
stale=[]
for fn,key in checks.items():
    try:
        h=s3.head_object(Bucket=B,Key=key); age=(now-h["LastModified"]).total_seconds()/60
        tag="FRESH ✓" if age<20 else "stale %.0fmin"%age
        print("  %-30s %s"%(fn,tag))
        if age>=20: stale.append(fn)
    except Exception:
        print("  %-30s NO OUTPUT"%fn); stale.append(fn)
print("\n=== DIAGNOSTICS for not-yet-fresh (CloudWatch tail) ===")
for fn in stale:
    print("  %s:\n     %s"%(fn,last_log(fn)))
print("\n=== ETF FUND-FLOW OUTPUT (the $99 data) ===")
for o in s3.list_objects_v2(Bucket=B,Prefix="data/").get("Contents",[]):
    k=o["Key"].lower()
    if "etf" in k or "fund-flow" in k:
        age=(now-o["LastModified"]).total_seconds()/3600
        print("  %-40s %.1fh old"%(o["Key"],age))
print("\n=== SAMPLES ===")
for key,label in [("data/dealer-gex.json","dealer-gex (GEX)"),("data/options-gamma.json","options-gamma")]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
        print("  %s keys: %s"%(label,list(d.keys())[:10]))
    except Exception as e: print("  %s: %s"%(label,str(e)[:50]))
