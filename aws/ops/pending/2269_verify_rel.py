import urllib.request, json, time, boto3
def get(u,t=30):
    req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Origin":"https://justhodl.ai"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","replace")
s,html=get("https://justhodl.ai/why.html")
print("why.html ->",s,"| renderRelationships:", "renderRelationships" in html, "| Customer & Partner Map:", "Customer &amp; Partner Map" in html)
# regenerate LDOS to populate relationships (AI intermittent — try sync, resilient either way)
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
s3=boto3.client("s3","us-east-1")
def s3doc(): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
before=s3doc().get("generated_at")
for attempt in range(2):
    try:
        t=time.time()
        with urllib.request.urlopen(urllib.request.Request(f"{URL}?ticker=LDOS&refresh=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=200) as r: r.read()
        print(f"attempt {attempt+1}: sync ok {time.time()-t:.0f}s"); break
    except Exception as e:
        print(f"attempt {attempt+1}: {type(e).__name__} (checking S3)")
    if s3doc().get("generated_at")!=before: break
d=s3doc()
for _ in range(8):
    if d.get("generated_at")!=before: break
    time.sleep(12); d=s3doc()
rel=d.get("relationships") or {}
print("\nLDOS gen:", d.get("generated_at"), "| AI ok:", not str(d.get('executive_summary') or '').startswith('AI synthesis failed'))
print("relationships present:", bool(rel))
print("  summary:", str(rel.get("summary"))[:200])
for kind in ("customers","partners","suppliers"):
    arr=rel.get(kind) or []
    print(f"  {kind} ({len(arr)}):", [ (x.get('name'), x.get('concentration') or x.get('source')) for x in arr[:5]])
print("DONE 2269")
