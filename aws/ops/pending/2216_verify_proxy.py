import urllib.request, json
UA={"User-Agent":"Mozilla/5.0"}
u="https://justhodl-data-proxy.raafouis.workers.dev/data/crypto-confluence.json"
try:
    r=urllib.request.urlopen(urllib.request.Request(u,headers=UA),timeout=20)
    d=json.loads(r.read())
    print("proxy status:", r.status)
    print("  engine:", d.get("engine"), "v"+str(d.get("version")))
    print("  regime:", d.get("market_context",{}).get("regime"), "| bullish:", d.get("counts",{}).get("bullish_any"), "| coins in book:", len(d.get("confluence_book") or []))
    print("PAGE+DATA FULLY LIVE")
except Exception as e:
    print("proxy ERR:", str(e)[:90])
print("DONE 2216")
