import urllib.request, time
def get(u,t=15):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=t); return r.getcode(), r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,"code",0), str(e)[:80]
for label,url,marker in [("deals page","https://justhodl.ai/deal-scanner.html","AI Deals"),("index nav","https://justhodl.ai/index.html","deal-scanner.html")]:
    for i in range(8):
        c,b=get(url)
        if c==200 and marker in b: print("OK  %-10s %s (200) marker '%s' present"%(label,url,marker)); break
        print("  %s attempt %d code=%s"%(label,i+1,c)); time.sleep(20)
    else: print("PENDING %s code=%s (Pages deploy lag)"%(label,c))
