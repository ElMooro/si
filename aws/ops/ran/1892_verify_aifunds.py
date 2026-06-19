import urllib.request, time
def get(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15); return r.getcode(),r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,"code",0),str(e)[:50]
for label,url,marker in [("ai-funds page","https://justhodl.ai/smart-money-13f.html","Investors Who Have The Edge"),("index nav","https://justhodl.ai/index.html","smart-money-13f.html")]:
    for i in range(8):
        c,b=get(url+("?t=%d"%time.time()))
        if c==200 and marker in b: print("OK  %-14s live (200), marker present"%label); break
        print("  %s try %d code=%s"%(label,i+1,c)); time.sleep(20)
    else: print("PENDING %s code=%s"%(label,c))
