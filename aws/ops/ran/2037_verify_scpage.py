"""ops 2037: confirm supply-chain.html live (runner reaches justhodl.ai)."""
import urllib.request, time
for u in ("https://justhodl.ai/supply-chain.html",):
    try:
        with urllib.request.urlopen(urllib.request.Request(u+f"?t={int(time.time())}",headers={"User-Agent":"v"}),timeout=20) as r:
            b=r.read().decode("utf-8","replace")
            print(u,"HTTP",r.getcode(),"bytes",len(b),"| has d3:",'d3.min.js' in b,"| reads graph json:",'supply-chain-graph.json' in b)
    except Exception as e: print(u,"->",str(e)[:90])
print("DONE 2037")
