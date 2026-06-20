import urllib.request, time
u="https://justhodl.ai/strategy-portfolio.html?t="+str(int(time.time()))
ok=False
for _ in range(5):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh-verify"}),timeout=20) as r:
            b=r.read().decode("utf-8","replace")
            print("HTTP",r.getcode(),"bytes",len(b),"| reads json:",'strategy-portfolio.json' in b,"| heatmap:",'correlation' in b,"| curves:",'equity_curves' in b)
            ok=True;break
    except Exception as e:
        print("retry…",str(e)[:60]); time.sleep(20)
print("PAGE_LIVE" if ok else "STILL_DOWN")
print("DONE 2045")
