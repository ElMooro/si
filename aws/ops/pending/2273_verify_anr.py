import urllib.request, json, time, boto3
def get(u,t=30):
    with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Origin":"https://justhodl.ai"}),timeout=t) as r: return r.status,r.read().decode("utf-8","replace")
s,html=get("https://justhodl.ai/why.html")
print("page ->",s,"| renderAnalystRatings:", "renderAnalystRatings" in html, "| ANR header:", "Analyst Ratings &amp; PT Momentum" in html)
# ANR is data-only; bg-invoke writes it even during AI outage
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc(t): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"equity-research/{t}.json")["Body"].read())
before=doc("LDOS").get("generated_at")
lam.invoke(FunctionName="justhodl-equity-research",InvocationType="Event",Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("regen LDOS (data-only ANR writes regardless of AI)...")
d=None
for i in range(14):
    time.sleep(13); cur=doc("LDOS")
    if cur.get("generated_at")!=before: d=cur; print(f"t+{(i+1)*13}s updated"); break
    print(f"t+{(i+1)*13}s not yet")
if d:
    ar=d.get("analyst_ratings") or {}
    print("distribution:", ar.get("distribution"))
    print("pt_momentum:", ar.get("pt_momentum"))
    print("recent_actions:", [(a.get('date'),a.get('firm'),a.get('action')) for a in (ar.get('recent_actions') or [])[:4]])
    print("ratings_trend points:", len(ar.get("ratings_trend") or []))
print("DONE 2273")
