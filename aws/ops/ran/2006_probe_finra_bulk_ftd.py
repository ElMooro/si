"""ops 2006: FINRA bulk latest-settlement pull (no sort) + real SEC FTD url from index page."""
import json, urllib.request, urllib.error, io, zipfile, re
from datetime import date, timedelta
def req(url,data=None,headers=None,timeout=50,raw=False):
    h={"User-Agent":"JustHodl Research raafouis@gmail.com"}
    if headers:h.update(headers)
    try:
        r=urllib.request.Request(url,data=(json.dumps(data).encode() if data else None),headers=h,method="POST" if data else "GET")
        with urllib.request.urlopen(r,timeout=timeout) as resp:
            b=resp.read(); return resp.getcode(),(b if raw else b.decode("utf-8","replace"))
    except urllib.error.HTTPError as e: return e.code,(e.read()[:300] if hasattr(e,'read') else str(e))
    except Exception as e: return None,str(e)[:200]

print("="*64);print("A) FINRA bulk latest-settlement via dateRangeFilter (NO sort)");print("="*64)
url="https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest"
end=date.today(); start=end-timedelta(days=28)
body={"limit":5000,"offset":0,
      "dateRangeFilters":[{"fieldName":"settlementDate","startDate":start.isoformat(),"endDate":end.isoformat()}]}
code,resp=req(url,data=body,headers={"Content-Type":"application/json","Accept":"application/json"})
print("HTTP",code,"bytes",len(resp))
try:
    j=json.loads(resp); rows=j if isinstance(j,list) else (j.get("data") or j.get("results") or [])
    print("rows:",len(rows))
    if rows:
        print("FIELDS:",sorted(rows[0].keys()))
        # distinct settlement dates present
        sd={}
        for r in rows: sd[r.get("settlementDate")]=sd.get(r.get("settlementDate"),0)+1
        print("settlementDates:",dict(sorted(sd.items())[-4:]))
        # sample a big name
        for r in rows:
            if r.get("symbolCode")=="AAPL":
                print("AAPL:",{k:r.get(k) for k in ("settlementDate","currentShortPositionQuantity","previousShortPositionQuantity","daysToCoverQuantity","averageDailyVolumeQuantity")}); break
        print("sample row:",{k:rows[0].get(k) for k in sorted(rows[0].keys())})
except Exception as e: print("parse err",str(e)[:200],"raw head:",resp[:200])

print("\n"+"="*64);print("B) SEC FTD — real url from index page");print("="*64)
for idx in ("https://www.sec.gov/data/foiadocsfailsdatahtm","https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"):
    code,html=req(idx,timeout=40)
    print(f" index {idx} -> HTTP {code}")
    if code==200:
        hrefs=re.findall(r'href=["\']([^"\']*cnsfails[^"\']*\.zip)["\']',html)
        print("   cnsfails links found:",len(hrefs))
        for h in hrefs[:3]: print("    ",h)
        if hrefs:
            u=hrefs[0]
            if u.startswith("/"): u="https://www.sec.gov"+u
            code2,body=req(u,timeout=60,raw=True)
            print("   fetch",u.split("/")[-1],"HTTP",code2,"bytes",len(body) if isinstance(body,bytes) else body)
            if code2==200 and isinstance(body,bytes):
                zf=zipfile.ZipFile(io.BytesIO(body)); nm=zf.namelist()[0]
                lines=zf.read(nm).decode("utf-8","replace").splitlines()
                print("   ",nm,len(lines),"lines | header:",lines[0][:70])
                for ln in lines[1:3]: print("     ",ln[:90])
            break
print("DONE 2006")
