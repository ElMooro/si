import urllib.request, time
def get(u):
    for _ in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r:
                return r.getcode(), r.read().decode("utf-8","replace")
        except Exception: time.sleep(14)
    return None,""
ts=str(int(time.time()))
c,idx=get(f"https://justhodl.ai/index.html?t={ts}")
print("index.html:",c,"| cycle-clock-card:",'cycle-clock-card' in idx,"| reads feed:",'data/cycle-clock.json' in idx,"| CC pill:",'/cycle-clock.html' in idx)
c2,pg=get(f"https://justhodl.ai/cycle-clock.html?t={ts}")
print("cycle-clock.html:",c2,"| reads feed:",'data/cycle-clock.json' in pg,"| has clock svg fn:",'function clock' in pg)
c3,dr=get(f"https://justhodl.ai/directory.html?t={ts}")
print("directory: cycle-clock listed:",'/cycle-clock.html' in dr)
print("DONE 2089")
