import urllib.request, urllib.parse, json, time
FK="d8qlt5pr01qrf6e278d0d8qlt5pr01qrf6e278dg"; FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def g(u,hdr=None):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers=hdr or {"User-Agent":"JustHodl/1.0 (research@justhodl.ai)"}),timeout=18)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e: return e.code, e.read().decode("utf-8","ignore")[:160]
    except Exception as e: return 0,str(e)[:120]
print("=== SEC EDGAR full-text search (13D/13G) ===")
c,b=g("https://efts.sec.gov/LATEST/search-index?q=%22&forms=SC%2013D")
print("  search-index forms=SC 13D: HTTP %s head=%s"%(c,b[:120].replace(chr(10)," ")))
c,b=g("https://efts.sec.gov/LATEST/search-index?forms=SC%2013D&dateRange=custom&startdt=2026-06-01&enddt=2026-06-19")
print("  search-index dated: HTTP %s head=%s"%(c,b[:120].replace(chr(10)," ")))
c,b=g("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=SC+13D&output=atom&count=10")
print("  browse-edgar getcurrent SC 13D atom: HTTP %s head=%s"%(c,b[:130].replace(chr(10)," ")))
c,b=g("https://data.sec.gov/submissions/CIK0000320193.json")  # company_tickers sanity via AAPL
print("  data.sec.gov submissions AAPL: HTTP %s (recent forms present=%s)"%(c, '\"form\"' in b))
c,b=g("https://www.sec.gov/files/company_tickers.json")
print("  company_tickers.json (CIK<->ticker map): HTTP %s len=%d"%(c,len(b)))
time.sleep(1)
print("\n=== FINNHUB calendars (free?) ===")
for n,p in [("earnings-calendar","calendar/earnings?from=2026-06-19&to=2026-07-10"),("ipo-calendar","calendar/ipo?from=2026-05-01&to=2026-06-19")]:
    c,b=g("https://finnhub.io/api/v1/"+p+"&token="+FK); print("  %-18s HTTP %s head=%s"%(n,c,b[:110].replace(chr(10)," "))); time.sleep(1.1)
print("\n=== FMP calendars (/stable/) ===")
for n,p in [("earnings-cal","earnings-calendar?from=2026-06-19&to=2026-07-05"),("ipo-cal","ipos-calendar?from=2026-01-01&to=2026-06-19"),("econ-cal","economic-calendar?from=2026-06-19&to=2026-06-26")]:
    c,b=g("https://financialmodelingprep.com/stable/"+p+"&apikey="+FMP); print("  %-14s HTTP %s head=%s"%(n,c,b[:110].replace(chr(10)," "))); time.sleep(0.5)
print("\n=== GOOGLE TRENDS (unofficial) ===")
c,b=g("https://trends.google.com/trends/api/dailytrends?hl=en-US&tz=-300&geo=US")
print("  dailytrends: HTTP %s head=%s"%(c,b[:90].replace(chr(10)," ")))
