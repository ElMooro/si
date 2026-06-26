import urllib.request, json, time, boto3
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
s3=boto3.client("s3","us-east-1")
def s3doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
before=s3doc().get("generated_at")
# up to 2 sync attempts (AI is intermittent); the resilience fix means it writes either way
for attempt in range(2):
    try:
        req=urllib.request.Request(f"{URL}?ticker=LDOS&refresh=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"})
        t=time.time()
        with urllib.request.urlopen(req,timeout=200) as r: r.read()
        print(f"attempt {attempt+1}: sync ok in {time.time()-t:.0f}s")
        break
    except Exception as e:
        print(f"attempt {attempt+1}: {type(e).__name__} {str(e)[:50]} (checking S3 anyway)")
    # check if S3 updated despite connection
    if s3doc().get("generated_at")!=before: break
# poll S3 a bit more for the write to land
d=s3doc()
for _ in range(8):
    if d.get("generated_at")!=before: break
    time.sleep(12); d=s3doc()
bm=d.get("business_mix") or {}; pm=(d.get("forward_model") or {}).get("price_model") or {}
print("\nLDOS gen now:", d.get("generated_at"), "(was", before, ")")
print("business_mix segments:", bm.get("segments"))
print("business_mix geography:", bm.get("geography"))
print("price_history pts:", len(d.get("price_history") or []))
print("margins op latest:", ((d.get('margins') or {}).get('operating_trend') or [{}])[0])
print("forward_model price_model:", {k:pm.get(k) for k in ("forward_pe_applied","fair_value_base","target_eps_year")} if pm else None)
ka=(d.get("forward_model") or {}).get("key_assumptions") or []
print("key_assumptions[0] type:", type(ka[0]).__name__ if ka else None)
print("AI exec ok:", not str(d.get('executive_summary') or '').startswith('AI synthesis failed'), "| model:", (d.get('metadata') or {}).get('claude_model'))
print("DONE 2268")
