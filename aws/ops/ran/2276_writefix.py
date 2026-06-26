import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
def doc(t): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"equity-research/{t}.json")["Body"].read())
before=doc("LDOS").get("generated_at")
lam.invoke(FunctionName="justhodl-equity-research",InvocationType="Event",Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("regen LDOS; waiting up to ~280s for the write (non-blocking fallback)...")
d=None
for i in range(22):
    time.sleep(13); cur=doc("LDOS")
    if cur.get("generated_at")!=before: d=cur; print(f"t+{(i+1)*13}s WROTE gen={cur.get('generated_at')}"); break
    print(f"t+{(i+1)*13}s...")
if d:
    ar=d.get("analyst_ratings") or {}
    print("\nANALYST RATINGS:")
    print("  distribution:", ar.get("distribution"))
    print("  pt_momentum:", {k:ar.get('pt_momentum',{}).get(k) for k in ('last_month_avg','last_quarter_avg','last_year_avg','momentum_pct')})
    print("  recent_actions:", [(a.get('date'),a.get('firm'),a.get('action')) for a in (ar.get('recent_actions') or [])[:4]])
    print("  ratings_trend pts:", len(ar.get("ratings_trend") or []))
    print("  AI exec ok:", not str(d.get('executive_summary') or '').startswith('AI synthesis failed'))
    # also confirm the other quant data still intact (no regression)
    print("  business_mix segs present:", bool((d.get('business_mix') or {}).get('segments')), "| price pts:", len(d.get('price_history') or []))
else: print("STILL not written — deeper issue")
print("DONE 2276")
