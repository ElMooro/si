import urllib.request, json, time, boto3
def get(u,t=30):
    req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Origin":"https://justhodl.ai"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","replace")
s,html=get("https://justhodl.ai/why.html")
print("why.html ->",s,
      "| renderPriceChart:", "renderPriceChart" in html,
      "| renderBusinessMix:", "renderBusinessMix" in html,
      "| renderSensitivity:", "renderSensitivity" in html,
      "| key-assum fix:", "Object.values(a)[0]" in html)
# trigger LDOS regen so the user's page gets the new quant data (resilient even if AI degraded)
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-equity-research", InvocationType="Event",
           Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("LDOS regen kicked (before=%s)"%before)
for i in range(14):
    time.sleep(13)
    cur=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
    if cur.get("generated_at")!=before:
        bm=cur.get("business_mix") or {}
        print(f"t+{(i+1)*13}s LDOS updated | segs:",bm.get("segments"),"| price pts:",len(cur.get("price_history") or []),
              "| margins op latest:",((cur.get('margins') or {}).get('operating_trend') or [{}])[0].get('value'),
              "| forward_model:",bool((cur.get('forward_model') or {}).get('price_model')),
              "| AI exec ok:", not str(cur.get('executive_summary') or '').startswith('AI synthesis failed'))
        break
    print(f"t+{(i+1)*13}s not yet")
print("DONE 2267")
