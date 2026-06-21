import urllib.request, time
def get(u):
    for _ in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r:
                return r.getcode(), r.read().decode("utf-8","replace")
        except Exception: time.sleep(15)
    return None,""
c,b=get("https://justhodl.ai/index.html?t="+str(int(time.time())))
print("index.html:",c,"| emergence-card present:",'emergence-card' in b,"| reads crypto-emergence:",'data/crypto-emergence.json' in b,"| reads sector-emergence:",'data/sector-emergence.json' in b)
c2,b2=get("https://justhodl.ai/crypto-emergence.html?t="+str(int(time.time())))
print("crypto-emergence.html:",c2,"| MVRV chip:",'MVRV' in b2)
print("DONE 2074")
