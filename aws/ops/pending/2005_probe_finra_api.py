"""ops 2005: nail FINRA consolidatedShortInterest query shape + SEC FTD w/ proper UA + Polygon desc snapshot."""
import json, urllib.request, urllib.error, io, zipfile
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"; POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

def req(url,data=None,headers=None,timeout=40,raw=False,method=None):
    h={"User-Agent":"JustHodl Research raafouis@gmail.com"}
    if headers:h.update(headers)
    try:
        r=urllib.request.Request(url,data=(json.dumps(data).encode() if data else None),headers=h,method=method or ("POST" if data else "GET"))
        with urllib.request.urlopen(r,timeout=timeout) as resp:
            b=resp.read(); return resp.getcode(),(b if raw else b.decode("utf-8","replace"))
    except urllib.error.HTTPError as e:
        return e.code,(e.read()[:300] if hasattr(e,'read') else str(e))
    except Exception as e: return None,str(e)[:200]

print("="*64);print("A) FINRA consolidatedShortInterest — POST filter by symbol+latest");print("="*64)
url="https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
# POST query: most recent settlement for AAPL
body={"limit":3,"compareFilters":[{"fieldName":"symbolCode","compareType":"EQUAL","fieldValue":"AAPL"}],
      "sortFields":["-settlementDate"]}
code,resp=req(url,data=body,headers={"Content-Type":"application/json","Accept":"application/json"})
print("POST AAPL:",code)
try:
    j=json.loads(resp); rows=j if isinstance(j,list) else j.get("data") or j.get("results") or []
    print("  rows:",len(rows))
    if rows: 
        print("  FIELDS:",sorted(rows[0].keys()))
        r0=rows[0]
        for k in ("settlementDate","currentShortPositionQuantity","previousShortPositionQuantity","daysToCoverQuantity","averageDailyVolumeQuantity","changePercent","symbolCode","issueName"):
            print(f"    {k} = {r0.get(k)}")
except Exception as e: print("  parse:",str(e)[:200],"| raw:",resp[:200])

# latest settlement date across all (1 row) to learn current period
code,resp=req(url,data={"limit":1,"sortFields":["-settlementDate"]},headers={"Content-Type":"application/json"})
try:
    j=json.loads(resp); rows=j if isinstance(j,list) else j.get("data") or []
    print("  latest settlementDate overall:",rows[0].get("settlementDate") if rows else None)
except Exception as e: print("  latest err",str(e)[:120])

# how many rows for a full latest-settlement pull? (count via large limit, one settlement)
code,resp=req(url,data={"limit":10,"compareFilters":[{"fieldName":"symbolCode","compareType":"EQUAL","fieldValue":"GME"}],"sortFields":["-settlementDate"]},headers={"Content-Type":"application/json"})
try:
    j=json.loads(resp); rows=j if isinstance(j,list) else j.get("data") or []
    print("  GME latest:",rows[0].get("settlementDate"),"SI=",rows[0].get("currentShortPositionQuantity"),"DTC=",rows[0].get("daysToCoverQuantity") if rows else None)
except Exception as e: print("  GME err",str(e)[:120])

print("\n"+"="*64);print("B) SEC FTD with proper UA");print("="*64)
for ym,half in [("202605","b"),("202605","a"),("202604","b")]:
    u=f"https://www.sec.gov/files/data/frequently-requested-foia-document-fails-deliver-data/cnsfails{ym}{half}.zip"
    code,body=req(u,timeout=50,raw=True)
    print(f"  cnsfails{ym}{half}.zip: HTTP {code} bytes={len(body) if isinstance(body,bytes) else body}")
    if code==200 and isinstance(body,bytes):
        zf=zipfile.ZipFile(io.BytesIO(body)); nm=zf.namelist()[0]
        lines=zf.read(nm).decode("utf-8","replace").splitlines()
        print(f"    {nm}: {len(lines)} lines | header: {lines[0][:80]}")
        for ln in lines[1:3]: print("     ",ln[:100])
        # AAPL total fails in this half (max balance)
        aapl=[ln for ln in lines if len(ln.split("|"))>2 and ln.split("|")[2]=="AAPL"]
        print(f"    AAPL rows this half: {len(aapl)}", aapl[-1][:100] if aapl else "")
        break

print("\n"+"="*64);print("C) Polygon SI with order=desc (current snapshot)");print("="*64)
code,resp=req(f"https://api.polygon.io/stocks/v1/short-interest?ticker=AAPL&order=desc&sort=settlement_date&limit=3&apiKey={POLY}")
try:
    j=json.loads(resp); res=j.get("results") or []
    print("  AAPL recent:",[(r.get('settlement_date'),r.get('short_interest'),r.get('days_to_cover')) for r in res])
except Exception as e: print("  poly err",str(e)[:120],resp[:160])
print("DONE 2005")
