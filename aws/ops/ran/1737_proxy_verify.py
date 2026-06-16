import urllib.request, json
def get(url, t=30):
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=t) as r: return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',None), str(e)[:60]
PX="https://justhodl-data-proxy.raafouis.workers.dev"
print("Confirming the page's actual fetch path (gj: justhodl.ai first, then PX proxy):\n")
for key in ["data/edgar-authority.json","data/stock-valuations.json"]:
    s1,_=get(f"https://justhodl.ai/{key}?t=v")
    s2,body=get(f"{PX}/{key}?t=v")
    ok="—"
    if s2==200:
        try: d=json.loads(body); ok=f"OK keys={len(d)}"+(f" net-nets={d.get('n_net_nets')} cc={d.get('crosscheck',{}).get('n_checked')}" if 'edgar' in key else "")
        except: ok="200 but parse-fail"
    print(f"  {key}")
    print(f"     justhodl.ai -> {s1}   PX proxy -> {s2}  [{ok}]")
print("\nVERDICT: page renders if PX proxy serves edgar-authority.json (200).")
