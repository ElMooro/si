import urllib.request, json, time, boto3
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
s3=boto3.client("s3","us-east-1")
def doc(): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
before=doc().get("generated_at")
for attempt in range(2):
    try:
        t=time.time()
        with urllib.request.urlopen(urllib.request.Request(f"{URL}?ticker=LDOS&refresh=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=200) as r: r.read()
        print(f"attempt {attempt+1}: ok {time.time()-t:.0f}s"); break
    except Exception as e:
        print(f"attempt {attempt+1}: {type(e).__name__} (check S3)")
    if doc().get("generated_at")!=before: break
d=doc()
for _ in range(8):
    if d.get("generated_at")!=before: break
    time.sleep(12); d=doc()
ar=d.get("analyst_ratings") or {}
print("\nLDOS gen:", d.get("generated_at"))
print("distribution:", ar.get("distribution"))
print("pt_momentum:", ar.get("pt_momentum"))
print("recent_actions:", [(a.get('date'),a.get('firm'),(a.get('from'),a.get('to')),a.get('action')) for a in (ar.get('recent_actions') or [])[:5]])
print("ratings_trend pts:", len(ar.get("ratings_trend") or []), "| first/last:", (ar.get('ratings_trend') or [{}])[0], (ar.get('ratings_trend') or [{}])[-1])
print("DONE 2274")
