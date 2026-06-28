import urllib.request, time
def fetch(url):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"})
    with urllib.request.urlopen(req,timeout=30) as r: return r.read().decode("utf-8","ignore")
markers=['data-tab="flows"','Flows · COT','Exchange Flows','Institutional COT (CME)','Coinbase Premium','Stablecoin Peg','Realized Price','Flows · COT · Realized','exchange_flows','D.cot']
for attempt in range(3):
    cb=int(time.time())
    html=fetch("https://justhodl.ai/crypto/?cb=%d"%cb)
    hit=sum(1 for m in markers if m in html)
    print(f"attempt {attempt+1}: {hit}/{len(markers)} markers | size {len(html)}")
    if hit==len(markers):
        print("ALL MARKERS PRESENT ✓"); break
    miss=[m for m in markers if m not in html]
    print("  missing:",miss)
    time.sleep(20)
print("DONE 2405")
