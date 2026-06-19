import urllib.request, json, time
FK="d8qlt5pr01qrf6e278d0d8qlt5pr01qrf6e278dg"  # finnhub key (not echoed)
def get(url,hdr=None):
    try:
        r=urllib.request.urlopen(urllib.request.Request(url,headers=hdr or {"User-Agent":"Mozilla/5.0 jh-verify"}),timeout=20)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8","ignore")[:200]
    except Exception as e:
        return 0, str(e)[:140]
def fh(path):
    return get("https://finnhub.io/api/v1/"+path+("&" if "?" in path else "?")+"token="+FK)

print("===== FINNHUB (new key) =====")
for name,path in [("quote","quote?symbol=NVDA"),
                  ("insider-sentiment","stock/insider-sentiment?symbol=NVDA&from=2025-06-01&to=2026-06-19"),
                  ("insider-transactions","stock/insider-transactions?symbol=NVDA&from=2026-01-01&to=2026-06-19"),
                  ("recommendation","stock/recommendation?symbol=NVDA"),
                  ("earnings-surprise","stock/earnings?symbol=NVDA"),
                  ("company-news","company-news?symbol=NVDA&from=2026-06-12&to=2026-06-19")]:
    c,b=fh(path); print("  [%s] %-22s %s"%("OK" if c==200 else c,name,b.replace(chr(10)," ")[:140])); time.sleep(1.1)

print("\n===== GDELT DOC 2.0 (no key) =====")
for name,url in [("timelinevol AI-datacenter","https://api.gdeltproject.org/api/v2/doc/doc?query=%22AI%20datacenter%22&mode=timelinevol&timespan=14d&format=json"),
                 ("tonechart Micron","https://api.gdeltproject.org/api/v2/doc/doc?query=Micron%20HBM&mode=tonechart&timespan=14d&format=json"),
                 ("artlist SanDisk","https://api.gdeltproject.org/api/v2/doc/doc?query=SanDisk&mode=artlist&maxrecords=3&timespan=7d&format=json")]:
    c,b=get(url); print("  [%s] %-26s %s"%("OK" if c==200 else c,name,b.replace(chr(10)," ")[:160])); time.sleep(1.5)

print("\n===== STOCKTWITS (no key) =====")
for name,url in [("trending","https://api.stocktwits.com/api/2/trending/symbols.json"),
                 ("symbol-stream NVDA","https://api.stocktwits.com/api/2/streams/symbol/NVDA.json")]:
    c,b=get(url); print("  [%s] %-20s %s"%("OK" if c==200 else c,name,b.replace(chr(10)," ")[:160])); time.sleep(1.5)
