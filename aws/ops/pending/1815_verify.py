import urllib.request
def get(u,t=20):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 JustHodl"}),timeout=t) as r: return r.status,r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code','ERR'),str(e)
st,body=get("https://justhodl.ai/jh-enhance.js")
print(f"jh-enhance.js: HTTP {st} | {len(body)}b | has render funcs: {'renderBars' in body and 'renderLine' in body and 'data-metrics' in str(body)}")
sample=["carry.html","forensic.html","regime.html","global-tide.html","auctions.html","canaries.html","dislocations.html","13f.html"]
for p in sample:
    st,b=get("https://justhodl.ai/"+p)
    print(f"  {p}: HTTP {st} | jh-enhance wired: {'/jh-enhance.js' in b}")
