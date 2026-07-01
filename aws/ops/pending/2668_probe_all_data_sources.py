"""ops 2668 — verify real, accessible data sources for blocks 2/3/4: ERCOT interconnection
queue, a state WARN open-data API, SEC EDGAR full-text search (8-K 5.02 + Form 10/S-1),
FMP IPO calendar. No fabricated sources — only what's actually verified reachable."""
import urllib.request, json, gzip, io

def get(url, headers=None, timeout=20):
    h = {"User-Agent": "JustHodl.AI research contact@justhodl.ai"}
    if headers: h.update(headers)
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout)
        raw = r.read()
        if r.info().get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return r.status, raw
    except Exception as e:
        return None, str(e)[:200]

print("="*60)
print("1) ERCOT MIS report catalog — stable large-load queue endpoint")
s, b = get("https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=13424")
print("  reportTypeId=13424 (Large Flexible/Large Load queue guess):", s, str(b)[:300] if s else b)
s2, b2 = get("https://www.ercot.com/mp/data-products/data-product-details?id=NP3-987-ER")
print("  NP3-987-ER product page:", s2, str(b2)[:200] if s2 else b2)

print("\n" + "="*60)
print("2) SEC EDGAR full-text search — 8-K Item 5.02 (executive departures)")
s3, b3 = get('https://efts.sec.gov/LATEST/search-index?q=%22departure%20of%22&forms=8-K&dateRange=custom&startdt=2026-06-25&enddt=2026-06-30')
print("  efts search-index:", s3, str(b3)[:200] if s3 else b3)
s4, b4 = get('https://efts.sec.gov/LATEST/search-index?q=%225.02%22&forms=8-K')
print("  efts alt:", s4, str(b4)[:200] if s4 else b4)
s5, b5 = get('https://www.sec.gov/cgi-bin/srqsb?text=&first=1&last=40')
s6, b6 = get('https://data.sec.gov/submissions/CIK0000320193.json')
print("  data.sec.gov submissions (AAPL CIK) reachable:", s6, len(b6) if s6 else b6)

print("\n" + "="*60)
print("3) SEC EDGAR daily full-text search (the real endpoint)")
s7, b7 = get('https://efts.sec.gov/LATEST/search-index?q=%22Item+5.02%22&forms=8-K&startdt=2026-06-28&enddt=2026-06-30')
print("  status:", s7)
if s7:
    try:
        j = json.loads(b7)
        print("  keys:", list(j.keys())[:10], "| hits:", j.get("hits",{}).get("total",{}))
    except Exception as e:
        print("  parse err:", e, str(b7)[:300])

print("\n" + "="*60)
print("4) SEC EDGAR full text search UI endpoint (alt path)")
s8, b8 = get('https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=10-12G&dateb=&owner=include&count=40&output=atom')
print("  new-registrant Form 10 browse:", s8, str(b8)[:300] if s8 else b8)

print("\n" + "="*60)
print("5) California EDD WARN open data")
s9, b9 = get('https://data.edd.ca.gov/resource/oaxg-nezb.json?$limit=5')
print("  CA EDD Socrata WARN:", s9, str(b9)[:400] if s9 else b9)

print("\n" + "="*60)
print("6) FMP IPO calendar")
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s10, b10 = get(f'https://financialmodelingprep.com/stable/ipos-calendar?from=2026-06-01&to=2026-07-15&apikey={FMP}')
print("  status:", s10)
if s10:
    try:
        j = json.loads(b10)
        print("  count:", len(j) if isinstance(j,list) else j)
        if isinstance(j,list) and j: print("  sample:", json.dumps(j[0])[:300])
    except Exception as e: print("parse err", e)

print("DONE 2668")
