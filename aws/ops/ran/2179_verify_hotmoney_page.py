import urllib.request, time
time.sleep(135)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/hot-money.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
    b=r.read().decode("utf-8","ignore")
    print("hot-money.html ->",r.getcode(),"renders=",("Hot Money Radar" in b and "conviction" in b))
except Exception as e: print("hot-money.html ->",str(e)[:60])
print("DONE 2179")
