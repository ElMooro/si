import urllib.request, time
time.sleep(140)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/hot-money.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
    b=r.read().decode("utf-8","ignore")
    print("hot-money.html ->",r.getcode(),"| EM-debt section:", "EM-debt channel" in b, "| renders:", "Hot Money Radar" in b)
except Exception as e: print("hot-money.html ->",str(e)[:60])
print("DONE 2182")
# retrigger 1782409822
