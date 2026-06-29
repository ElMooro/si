import urllib.request, time
url="https://justhodl.ai/bottleneck-boom.html?cb=%d"%int(time.time())
try:
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (JustHodl verify)","Cache-Control":"no-cache"})
    h=urllib.request.urlopen(req,timeout=25).read().decode("utf-8","replace")
    print("len",len(h))
    for m in ["Industries & companies at the bottleneck","Inventory pre-shortage","Futures curve","COT commercials","Pricing power (PPI)","Capital (Marks)"]:
        print("  %-42s %s"%(m,"FOUND" if m in h else "MISSING"))
except Exception as e: print("ERR",str(e)[:90])
print("DONE 2490")
