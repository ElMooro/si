import urllib.request, time
time.sleep(5)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/ma200-radar.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
    body=r.read(600).decode("utf-8","ignore")
    print("ma200-radar.html ->",r.getcode(),"renders=",("Retest" in body or "200-DMA" in body))
except Exception as e: print("ma200-radar.html ->",str(e)[:60])
print("DONE 2163")
