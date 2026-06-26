import urllib.request, json
def fetch(u):
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh-verify"})
        with urllib.request.urlopen(req,timeout=25) as r: return r.status, r.read().decode("utf-8","replace")
    except Exception as e: return None, str(e)[:80]
st,body=fetch("https://justhodl.ai/inventory-drawdown.html")
print("PAGE justhodl.ai/inventory-drawdown.html ->",st)
if body and st==200:
    print("  has title:", "Inventory Drawdown" in body)
    print("  reads json key:", "data/inventory-drawdown.json" in body)
    print("  has sector bars:", "Which sectors are drawing down" in body)
    print("  has boom split:", "Demand-confirmed boom board" in body)
    print("  has destock strip:", "Destocking" in body)
st2,body2=fetch("https://justhodl-data-proxy.raafouis.workers.dev/data/inventory-drawdown.json")
print("PROXY data/inventory-drawdown.json ->",st2)
if st2==200:
    try:
        d=json.loads(body2); print("  valid json · sectors:",len(d.get('sector_drawdown') or []),"· boom:",len(d.get('boom_setups') or []))
    except Exception as e: print("  json parse err",str(e)[:50])
print("DONE 2243")
