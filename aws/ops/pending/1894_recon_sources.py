import urllib.request, json, time
FK="d8qlt5pr01qrf6e278d0d8qlt5pr01qrf6e278dg"  # not echoed
def g(u,hdr=None):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers=hdr or {"User-Agent":"jh-recon"}),timeout=15)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8","ignore")[:160]
    except Exception as e:
        return 0, str(e)[:120]
print("=== FINNHUB (new key) ===")
fh={"quote":"quote?symbol=NVDA","insider-transactions":"stock/insider-transactions?symbol=NVDA&from=2026-01-01&to=2026-06-19",
 "insider-sentiment":"stock/insider-sentiment?symbol=NVDA&from=2025-06-01&to=2026-06-19","recommendation":"stock/recommendation?symbol=NVDA",
 "earnings-surprise":"stock/earnings?symbol=NVDA","company-news":"company-news?symbol=NVDA&from=2026-06-12&to=2026-06-19",
 "social-sentiment":"stock/social-sentiment?symbol=NVDA","price-target":"stock/price-target?symbol=NVDA"}
for n,p in fh.items():
    c,b=g("https://finnhub.io/api/v1/"+p+"&token="+FK)
    tag="OK " if c==200 and b not in("[]","{}") and "Invalid" not in b else "EMPTY" if c==200 else "PREMIUM/DENIED" if c in(401,403) else "LIMIT" if c==429 else "ERR"
    print("  [%-14s] %-20s HTTP %s  %s"%(tag,n,c,b.replace(chr(10)," ")[:90]))
    time.sleep(1.1)
print("\n=== STOCKTWITS (no key) ===")
for n,u in [("trending","https://api.stocktwits.com/api/2/trending/symbols.json"),("symbol-stream","https://api.stocktwits.com/api/2/streams/symbol/NVDA.json")]:
    c,b=g(u)
    print("  [%s] %-14s HTTP %s  %s"%("OK " if c==200 else "BLOCKED",n,c,b.replace(chr(10)," ")[:110]))
    time.sleep(1)
print("\n=== GDELT (no key) ===")
for n,u in [("tone-timeline","https://api.gdeltproject.org/api/v2/doc/doc?query=%22AI%20data%20center%22&mode=timelinetone&format=json&timespan=3months"),
            ("artlist","https://api.gdeltproject.org/api/v2/doc/doc?query=%22HBM%20memory%22&mode=artlist&maxrecords=3&format=json&timespan=1week")]:
    c,b=g(u)
    print("  [%s] %-14s HTTP %s  %s"%("OK " if c==200 and b.strip().startswith("{") else "CHK",n,c,b.replace(chr(10)," ")[:110]))
    time.sleep(1)
