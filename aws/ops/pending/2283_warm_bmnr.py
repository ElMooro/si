import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); logs=boto3.client("logs","us-east-1")
def doc(t):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"equity-research/{t}.json")["Body"].read())
    except Exception: return None
exists=doc("BMNR")
print("BMNR cached before:", bool(exists), "gen=" , (exists or {}).get("generated_at"))
before=(exists or {}).get("generated_at")
t0=time.time()
lam.invoke(FunctionName="justhodl-equity-research",InvocationType="Event",Payload=json.dumps({"ticker":"BMNR","force_refresh":True,"_internal":"1"}).encode())
print("invoked BMNR; polling up to ~280s...")
d=None
for i in range(22):
    time.sleep(13); cur=doc("BMNR")
    if cur and cur.get("generated_at")!=before: d=cur; print(f"t+{(i+1)*13}s WROTE in {time.time()-t0:.0f}s"); break
    print(f"t+{(i+1)*13}s...")
if d:
    print("\n=== BMNR doc summary ===")
    co=d.get("company") or {}; q=d.get("quote") or {}
    print("name:", co.get("name"), "| sector:", co.get("sector"), "| industry:", co.get("industry"))
    print("price:", q.get("price"), "| mkt_cap:", co.get("market_cap"))
    print("data_sources:", (d.get("metadata") or {}).get("data_sources_ok"))
    for k in ["valuation","peer_comparison","industry_comparison","financial_health","growth","analyst_ratings","options_expectations","business_mix","price_history","earnings_track_record","margins"]:
        v=d.get(k)
        if isinstance(v,dict): print(f"  {k}: dict keys={len(v)}", "EMPTY" if not v else "")
        elif isinstance(v,list): print(f"  {k}: list len={len(v)}")
        else: print(f"  {k}: {('null' if v is None else type(v).__name__)}")
    oe=d.get("options_expectations"); ar=d.get("analyst_ratings") or {}
    print("  options:", "none (no chain)" if not oe else f"move ±{oe.get('implied_move_pct')}%")
    print("  analyst dist total:", (ar.get('distribution') or {}).get('total'))
    print("  exec_summary head:", str(d.get("executive_summary"))[:90])
else:
    print("NOT written in window — pulling recent ERROR logs")
    start=int((time.time()-300)*1000)
    for st in logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-equity-research",orderBy="LastEventTime",descending=True,limit=3)["logStreams"]:
        for e in logs.get_log_events(logGroupName="/aws/lambda/justhodl-equity-research",logStreamName=st["logStreamName"],startTime=start,limit=200,startFromHead=False)["events"]:
            m=e["message"].rstrip()
            if any(x in m for x in ("BMNR","Error","Traceback","Task timed out","[options]","[fmp_get]","[polygon")): print("  LOG:",m[:200])
print("DONE 2283")
