import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-earnings-tracker",InvocationType="RequestResponse")
print("invoke:",r.get("StatusCode"),r.get("FunctionError"),"|",r["Payload"].read()[:160])
time.sleep(3)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/earnings-tracker.json")["Body"].read())
print("version:",j.get("version"),"| n_recent:",j.get("n_recent"),"| n_forward_calendar:",j.get("n_forward_calendar"))
print("data_sources:",j.get("data_sources"))
cal=j.get("forward_calendar",[])
print(f"\n★ MARKET-WIDE FORWARD CALENDAR ({len(cal)} names imp>=2, next 14d):")
for x in cal[:12]:
    print(f"  {x['date']} {x.get('session'):<4} {x['ticker']:<6} imp{x.get('importance')} estEPS={x.get('estimated_eps')} {esc if False else (x.get('company') or '')[:22]}")
print("\n★ PEAD now with revenue confirmation (label | eps% | rev% | score):")
rec=[r for r in j.get("recent_results_30d",[]) if r.get("eps_surprise_pct") is not None]
for x in sorted(rec,key=lambda z:z.get('pead_score',50),reverse=True)[:8]:
    print(f"  {x['ticker']:<6} {x.get('pead_label'):<22} eps={x.get('eps_surprise_pct')!s:<8} rev={x.get('revenue_surprise_pct')!s:<8} score={x.get('pead_score')}")
dbd=[r for r in rec if r.get('pead_label')=='DOUBLE_BEAT_DRIFT']
print(f"\nDOUBLE_BEAT_DRIFT names (beat both lines): {[r['ticker'] for r in dbd]}")
print("DONE 1976")
