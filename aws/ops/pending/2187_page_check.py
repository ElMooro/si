import urllib.request, time
time.sleep(135)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/accumulation.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
    b=r.read().decode("utf-8","ignore")
    print("accumulation.html ->",r.getcode(),"| renders:", ("Accumulation / Distribution Radar" in b and "Likely TOPS" in b))
except Exception as e: print("accumulation.html ->",str(e)[:60])
print("DONE 2187")
