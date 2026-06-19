import urllib.request, time
def get(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,"code",0), str(e)[:60]
for i in range(8):
    c,b=get("https://justhodl.ai/deal-scanner.html?t=%d"%time.time())
    if c==200 and "badge flow" in b and "flowBadge" in b:
        print("OK deal-scanner.html live with 🌊 sector-tailwind badge (200)"); break
    print("  attempt %d code=%s badge_present=%s"%(i+1,c,"badge flow" in (b or ""))); time.sleep(20)
else: print("PENDING code=%s"%c)
