import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-equity-research", InvocationType="Event",
           Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("bg regen kicked; before=",before)
d=None
for i in range(16):
    time.sleep(13)
    cur=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
    if cur.get("generated_at")!=before: d=cur; print(f"t+{(i+1)*13}s NEW doc gen={cur.get('generated_at')}"); break
    print(f"t+{(i+1)*13}s not yet")
if d:
    bm=d.get("business_mix") or {}
    print("\nBUSINESS MIX segments:", bm.get("segments"))
    print("geography:", bm.get("geography"), "| trend periods:", len(bm.get("segment_trend") or []))
    print("price_history pts:", len(d.get("price_history") or []), "| sample:", (d.get("price_history") or [None])[0], (d.get("price_history") or [None])[-1])
    ot=[x for x in ((d.get("margins") or {}).get("operating_trend") or []) if x.get("value") is not None]
    print("margins operating non-null:", len(ot), "latest:", ((d.get("margins") or {}).get("operating_trend") or [{}])[0])
    print("AI exec_summary (may be degraded during outage):", str(d.get("executive_summary"))[:90])
    print("business_mix_assessment:", str(d.get("business_mix_assessment"))[:160])
else:
    print("doc still not regenerated within window")
print("DONE 2265")
