import urllib.request, time, boto3, json
def get(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15); return r.getcode(),r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,"code",0),str(e)[:50]
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/attention-signals.json")["Body"].read())
print("attention-signals: tickers=%s themes=%s (Wikipedia)"%(d.get("n_tickers"),len(d.get("theme_pulse",[]))))
print("THEME PULSE (Wikipedia pageview momentum):")
for t in (d.get("theme_pulse") or [])[:10]:
    print("   %-26s recent=%-8s trend=%s%%"%(t["theme"],t.get("views_recent"),t.get("attention_trend_pct")))
print()
for label,url,marker in [("attention page","https://justhodl.ai/attention.html","Accumulation Meets Attention"),("index nav","https://justhodl.ai/index.html","attention.html")]:
    for i in range(8):
        c,b=get(url+("?t=%d"%time.time()))
        if c==200 and marker in b: print("OK  %-14s live (200), marker present"%label); break
        print("  %s try %d code=%s"%(label,i+1,c)); time.sleep(20)
    else: print("PENDING %s code=%s"%(label,c))
