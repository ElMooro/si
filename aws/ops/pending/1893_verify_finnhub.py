import urllib.request, json, time
K="afcbdce692af048c29dee367c1c53c631e64a395"   # not echoed below
def get(path):
    u="https://finnhub.io/api/v1/"+path+("&" if "?" in path else "?")+"token="+K
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh-verify"}),timeout=15)
        return r.getcode(), r.read().decode("utf-8","ignore")[:600]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8","ignore")[:200]
    except Exception as e:
        return 0, str(e)[:120]
tests={
 "quote (price)":"quote?symbol=NVDA",
 "insider-sentiment":"stock/insider-sentiment?symbol=NVDA&from=2025-06-01&to=2026-06-19",
 "insider-transactions":"stock/insider-transactions?symbol=NVDA&from=2026-01-01&to=2026-06-19",
 "recommendation-trends":"stock/recommendation?symbol=NVDA",
 "earnings-surprise":"stock/earnings?symbol=NVDA",
 "company-news":"company-news?symbol=NVDA&from=2026-06-12&to=2026-06-19",
 "general-news":"news?category=general",
 "social-sentiment":"stock/social-sentiment?symbol=NVDA",
 "price-target":"stock/price-target?symbol=NVDA",
}
print("FINNHUB KEY CHECK (key valid if quote returns 200 with a 'c' price field)\n")
for name,path in tests.items():
    code,body=get(path)
    ok="OK " if code==200 else "DENIED" if code in (401,403) else "LIMIT" if code==429 else "ERR"
    # show a compact preview, never the key
    prev=body.replace("\n"," ")[:120]
    print("  [%s] %-22s HTTP %s  %s"%(ok,name,code,prev))
    time.sleep(1.1)   # stay under free 60/min
