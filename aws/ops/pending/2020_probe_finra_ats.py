"""ops 2020: probe FINRA ATS / off-exchange weekly transparency dataset (dark-pool)."""
import json, urllib.request, urllib.error
from datetime import date, timedelta
URL_BASE="https://api.finra.org/data/group/otcMarket/name/"
def post(name, body):
    try:
        req=urllib.request.Request(URL_BASE+name, data=json.dumps(body).encode(),
            headers={"User-Agent":"JustHodl Research raafouis@gmail.com","Content-Type":"application/json","Accept":"application/json"}, method="POST")
        with urllib.request.urlopen(req,timeout=40) as r: return r.getcode(), json.loads(r.read())
    except urllib.error.HTTPError as e:
        try: return e.code, e.read().decode()[:200]
        except: return e.code,""
    except Exception as e: return None,str(e)[:150]

print("="*64);print("A) weeklySummary dataset — field discovery (no filter)");print("="*64)
c,j=post("weeklySummary",{"limit":2})
print("HTTP",c)
if isinstance(j,list) and j:
    print("FIELDS:",sorted(j[0].keys()))
    for r in j[:2]: print("  row:",{k:r.get(k) for k in list(r.keys())[:10]})
elif isinstance(j,dict):
    rows=j.get("data") or j.get("results") or []
    print("rows:",len(rows));
    if rows: print("FIELDS:",sorted(rows[0].keys()))
else: print("resp:",str(j)[:200])

print("\n"+"="*64);print("B) distinct summaryTypeCode values present");print("="*64)
c,j=post("weeklySummary",{"limit":50})
rows=j if isinstance(j,list) else (j.get("data") if isinstance(j,dict) else []) or []
codes={}
for r in rows:
    k=r.get("summaryTypeCode"); codes[k]=codes.get(k,0)+1
print("summaryTypeCode seen:",codes)
# latest weekStartDate
wks=sorted({r.get("weekStartDate") for r in rows if r.get("weekStartDate")})
print("weekStartDates seen:",wks[-4:])

print("\n"+"="*64);print("C) per-symbol ATS + OTC for AAPL latest weeks");print("="*64)
end=date.today(); start=end-timedelta(days=45)
for code in ["ATS_W_SMBL","OTC_W_SMBL"]:
    body={"limit":5,
          "compareFilters":[{"fieldName":"summaryTypeCode","compareType":"EQUAL","fieldValue":code},
                            {"fieldName":"issueSymbolIdentifier","compareType":"EQUAL","fieldValue":"AAPL"}],
          "dateRangeFilters":[{"fieldName":"weekStartDate","startDate":start.isoformat(),"endDate":end.isoformat()}]}
    c,j=post("weeklySummary",body)
    rows=j if isinstance(j,list) else (j.get("data") if isinstance(j,dict) else []) or []
    print(f"\n {code} AAPL: HTTP {c} rows={len(rows) if isinstance(rows,list) else rows}")
    if isinstance(rows,list):
        for r in rows[:5]:
            print("   ",{k:r.get(k) for k in ("weekStartDate","issueSymbolIdentifier","totalWeeklyShareQuantity","totalWeeklyTradeCount","summaryTypeCode")})
print("DONE 2020")
