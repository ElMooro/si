import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
print("Lambda timeout now:", c.get("Timeout"))
# fire-and-forget background generation (force fresh)
lam.invoke(FunctionName="justhodl-equity-research", InvocationType="Event",
           Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("kicked off background LDOS regeneration; polling S3...")
prev=None
for i in range(20):  # up to ~260s
    time.sleep(13)
    try:
        o=s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")
        d=json.loads(o["Body"].read()); gen=d.get("generated_at")
        es=d.get("executive_summary") or ""; v=d.get("verdict") or {}; da=d.get("devils_advocate") or {}
        done = es and not es.startswith("AI synthesis failed") and v.get("rating") and bool(da)
        print(f"  t+{(i+1)*13}s gen={gen} exec_len={len(es)} verdict={v.get('rating')} devils={bool(da)} done={bool(done)}")
        if done:
            rf=d.get("risk_factors") or {}
            print("\n=== EXEC SUMMARY ===\n", es[:260])
            print("\n=== VERDICT ===", {k:v.get(k) for k in ("rating","conviction_grade","price_target_12m","upside_pct","verdict_rationale")})
            print("\n=== RISKS ===", rf.get("title"), "|", [r.get("risk") for r in (rf.get("key_risks") or [])[:4]])
            print("\n=== DEVIL'S ADVOCATE ===", da.get("title"))
            print("  short_thesis:", str(da.get("short_thesis"))[:320])
            print("  kill_points:", [(k.get("point"),k.get("evidence")) for k in (da.get("kill_points") or [])[:4]])
            print("  bulls underestimate:", str(da.get("what_bulls_underestimate"))[:160])
            break
    except Exception as e:
        print(f"  t+{(i+1)*13}s poll: {str(e)[:50]}")
print("DONE 2251")
