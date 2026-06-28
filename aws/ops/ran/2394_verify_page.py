import urllib.request, time
def fetch(url):
    req=urllib.request.Request(url,headers={"User-Agent":"justhodl/1.0"})
    with urllib.request.urlopen(req,timeout=30) as r: return r.read().decode("utf-8","ignore")
markers=['data-tab="volcomplex"','Vol Complex','pane-volcomplex','Implied Vol · DVOL','Miner Economics','Futures Basis · Carry','25&#916; risk reversal']
for attempt in range(3):
    try:
        html=fetch("https://justhodl.ai/crypto/?cb=%d"%int(time.time()))
        present=[m for m in markers if m in html]
        print(f"fetch {attempt+1}: {len(present)}/{len(markers)} markers present")
        if len(present)==len(markers):
            print("  ALL markers live:",present[:3],"...")
            break
        else:
            missing=[m for m in markers if m not in html]
            print("  missing:",missing)
    except Exception as e:
        print(f"fetch {attempt+1} err:",str(e)[:80])
    time.sleep(20)
# also confirm main page links to /crypto/
idx=fetch("https://justhodl.ai/?cb=%d"%int(time.time()))
print('main page links to /crypto/:', 'href="/crypto/"' in idx)
print("DONE 2394")
