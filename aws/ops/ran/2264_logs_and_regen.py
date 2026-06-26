import boto3, json, time
logs=boto3.client("logs","us-east-1"); lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
grp="/aws/lambda/justhodl-equity-research"
# fire a background regeneration FIRST so logs capture it
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-equity-research", InvocationType="Event",
           Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("kicked off bg regen; before gen=",before)
newdoc=None
for i in range(22):
    time.sleep(13)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
    if d.get("generated_at")!=before:
        newdoc=d; print(f"t+{(i+1)*13}s NEW doc gen={d.get('generated_at')}"); break
    print(f"t+{(i+1)*13}s not yet")
# pull recent error logs regardless
print("\n--- recent ERROR/Traceback logs ---")
start=int((time.time()-600)*1000)
for st in logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=3)["logStreams"]:
    for e in logs.get_log_events(logGroupName=grp,logStreamName=st["logStreamName"],startTime=start,limit=120,startFromHead=False)["events"]:
        m=e["message"].rstrip()
        if any(w in m for w in ("Traceback","Error","error","crash","[business","[claude] ","segmentation","KeyError","TypeError","NoneType")):
            print(m[:240])
if newdoc:
    bm=newdoc.get("business_mix") or {}
    print("\nBUSINESS MIX segments:", bm.get("segments"))
    print("geography:", bm.get("geography"), "| trend periods:", len(bm.get("segment_trend") or []))
    print("price_history pts:", len(newdoc.get("price_history") or []))
    ot=[x for x in ((newdoc.get("margins") or {}).get("operating_trend") or []) if x.get("value") is not None]
    print("margins operating non-null:", len(ot), "latest:", ((newdoc.get("margins") or {}).get("operating_trend") or [{}])[0])
    print("business_mix_assessment:", str(newdoc.get("business_mix_assessment"))[:220])
print("DONE 2264")
