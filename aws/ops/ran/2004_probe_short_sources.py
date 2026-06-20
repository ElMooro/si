"""ops 2004: PROBE free short-interest + FTD sources before building squeeze-fuel.
Tests SEC CNS fails zip, official bi-monthly SI (Nasdaq/FMP), confirms finra-short.json live + Polygon SI dead."""
import boto3, json, time, io, zipfile, urllib.request, urllib.error, os
from datetime import date, timedelta
s3=boto3.client("s3","us-east-1")
B="justhodl-dashboard-live"
FMP=os.environ.get("FMP_KEY","wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
UA={"User-Agent":"Mozilla/5.0 (compatible; JustHodl-research/1.0)"}

def get(url,headers=None,timeout=30,raw=False):
    try:
        req=urllib.request.Request(url,headers=headers or UA)
        with urllib.request.urlopen(req,timeout=timeout) as r:
            b=r.read()
            return r.getcode(), (b if raw else b.decode("utf-8","replace"))
    except urllib.error.HTTPError as e:
        return e.code, (e.read()[:200] if hasattr(e,'read') else str(e))
    except Exception as e:
        return None, str(e)[:160]

print("="*64); print("1) SEC CNS FAILS-TO-DELIVER (per-name FTD)"); print("="*64)
# try last few semi-monthly halves
cands=[]
y=2026
for ym,half in [("202605","b"),("202605","a"),("202604","b"),("202604","a")]:
    cands.append(f"https://www.sec.gov/files/data/frequently-requested-foia-document-fails-deliver-data/cnsfails{ym}{half}.zip")
got_ftd=None
for url in cands:
    code,body=get(url,timeout=40,raw=True)
    print(f"  {url.split('/')[-1]}: HTTP {code}  bytes={len(body) if isinstance(body,bytes) else body}")
    if code==200 and isinstance(body,bytes):
        try:
            zf=zipfile.ZipFile(io.BytesIO(body)); nm=zf.namelist()[0]
            txt=zf.read(nm).decode("utf-8","replace")
            lines=txt.splitlines()
            print(f"    parsed {nm}: {len(lines)} lines. header: {lines[0][:90]}")
            # sample a known ticker
            for ln in lines[1:200000]:
                if "|AAPL|" in ln or ln.split("|")[2:3]==["AAPL"]:
                    print("    AAPL row:",ln[:120]); break
            print("    sample rows:")
            for ln in lines[1:4]: print("     ",ln[:110])
            got_ftd=url; break
        except Exception as e:
            print("    zip parse err:",str(e)[:120])
print("  -> FTD usable:",bool(got_ftd))

print("\n"+"="*64); print("2) OFFICIAL BI-MONTHLY SHORT INTEREST — free candidates"); print("="*64)
# 2a Nasdaq unofficial per-symbol
for t in ["AAPL","GME"]:
    code,body=get(f"https://api.nasdaq.com/api/quote/{t}/short-interest?assetclass=stocks",
                  headers={**UA,"Accept":"application/json"})
    ok = code==200 and isinstance(body,str) and "shortInterest" in body
    print(f"  Nasdaq {t}: HTTP {code} usable={ok}", (body[:160] if isinstance(body,str) and not ok else ""))
# 2b FMP /stable/ candidates
for path in ["short-interest?symbol=AAPL","shares-float?symbol=AAPL","stock-short-interest?symbol=AAPL"]:
    code,body=get(f"https://financialmodelingprep.com/stable/{path}&apikey={FMP}")
    print(f"  FMP /stable/{path.split('?')[0]}: HTTP {code} {(body[:160] if isinstance(body,str) else body)}")
# 2c Polygon SI (confirm dead)
code,body=get(f"https://api.polygon.io/stocks/v1/short-interest?ticker=AAPL&limit=1&apiKey={POLY}")
print(f"  Polygon short-interest: HTTP {code} {(body[:160] if isinstance(body,str) else body)}")
# 2d FINRA consolidated short interest API (likely auth-gated)
code,body=get("https://api.finra.org/data/group/otcMarket/name/consolidatedShortInterest?limit=1")
print(f"  FINRA api consolidatedShortInterest: HTTP {code} {(body[:120] if isinstance(body,str) else body)}")

print("\n"+"="*64); print("3) EXISTING finra-short.json freshness + shape"); print("="*64)
try:
    h=s3.head_object(Bucket=B,Key="data/finra-short.json")
    age=(time.time()-h["LastModified"].timestamp())/3600
    fs=json.loads(s3.get_object(Bucket=B,Key="data/finra-short.json")["Body"].read())
    print(f"  age={age:.1f}h  top keys={sorted(fs.keys())[:12]}")
    # find per-ticker container
    for k in ("tickers","by_ticker","names","squeeze_candidates","universe"):
        v=fs.get(k)
        if isinstance(v,(list,dict)): print(f"    {k}: {len(v)}")
except Exception as e:
    print("  finra-short.json:",e)
try:
    h=s3.head_object(Bucket=B,Key="data/short-interest.json")
    age=(time.time()-h["LastModified"].timestamp())/3600
    si=json.loads(s3.get_object(Bucket=B,Key="data/short-interest.json")["Body"].read())
    print(f"  short-interest.json age={age:.1f}h keys={sorted(si.keys())[:12]}")
except Exception as e:
    print("  short-interest.json:",e)
print("DONE 2004")
