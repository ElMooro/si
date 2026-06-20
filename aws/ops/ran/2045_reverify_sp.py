import urllib.request, time
u="https://justhodl.ai/strategy-portfolio.html?t="+str(int(time.time()))
try:
    with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh-verify"}),timeout=20) as r:
        b=r.read().decode("utf-8","replace")
        print("HTTP",r.getcode(),"bytes",len(b),"| reads json:",'strategy-portfolio.json' in b,"| heatmap:",'correlation' in b)
except Exception as e: print("ERR",str(e)[:100])
print("DONE 2045")
