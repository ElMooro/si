import urllib.request, time
def get(u):
    for _ in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r:
                return r.getcode(), r.read().decode("utf-8","replace")
        except Exception: time.sleep(15)
    return None,""
ts=str(int(time.time()))
c,idx=get(f"https://justhodl.ai/index.html?t={ts}")
print("index.html:",c,"| Command Center band:",'COMMAND CENTER' in idx)
flagships=["strategist.html","regime-map.html","sector-emergence.html","crypto-emergence.html",
           "strategy-portfolio.html","paper-book.html","edge-discovery.html","interpretation-scorecard.html",
           "risk-regime.html","directory.html"]
print("\nflagship links present in band + page resolves 200:")
allok=True
for pg in flagships:
    inband = f'/{pg}"' in idx
    code,body=get(f"https://justhodl.ai/{pg}?t={ts}")
    ok = inband and code==200 and len(body)>500
    allok = allok and ok
    print(f"  {'✓' if ok else '✗'} {pg:<32} in-band={inband} http={code} bytes={len(body)}")
# the 3 new pages actually wire their feeds
for pg,feed in [("paper-book.html","data/paper-book.json"),("edge-discovery.html","data/edge-discovery.json"),("interpretation-scorecard.html","data/interpretation-scorecard.json")]:
    _,b=get(f"https://justhodl.ai/{pg}?t={ts}")
    print(f"  {pg} reads {feed}:", feed in b)
print("\nALL_FLAGSHIPS_OK" if allok else "SOME_ISSUE")
print("DONE 2075")
