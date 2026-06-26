import json, urllib.request, boto3
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(u):
    try:
        return json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=25).read())
    except urllib.error.HTTPError as e: return {"_http":e.code,"_body":e.read(150).decode("utf-8","replace")}
    except Exception as e: return {"_err":str(e)[:80]}
print("===== INDUSTRY / SECTOR P/E endpoints (/stable) =====")
cands=[
 "sector-pe-snapshot?date=2026-06-25&exchange=NYSE",
 "industry-pe-snapshot?date=2026-06-25&exchange=NYSE",
 "sector-pe-snapshot?date=2026-06-25",
 "industry-pe-snapshot?date=2026-06-25",
 "historical-sector-pe?sector=Technology",
 "historical-industry-pe?industry=Information%20Technology%20Services",
]
for c in cands:
    u=f"https://financialmodelingprep.com/stable/{c}&apikey={FMP}" if "?" in c else f"https://financialmodelingprep.com/stable/{c}?apikey={FMP}"
    r=get(u)
    if isinstance(r,list) and r:
        print(f"OK  {c[:40]:42} -> {len(r)} rows; sample:", json.dumps(r[0])[:160])
    else:
        print(f"--  {c[:40]:42} ->", json.dumps(r)[:120])
print("\n===== ANALYST ESTIMATES (forward projection raw material) =====")
r=get(f"https://financialmodelingprep.com/stable/analyst-estimates?symbol=LDOS&period=annual&limit=5&apikey={FMP}")
if isinstance(r,list) and r:
    print(f"{len(r)} rows; fields:", list(r[0].keys()))
    for row in r[:4]:
        print("  ", {k:row.get(k) for k in ("date","revenueAvg","revenueLow","revenueHigh","epsAvg","ebitdaAvg","netIncomeAvg","numAnalystsRevenue") if k in row})
else: print("analyst-estimates ->", json.dumps(r)[:150])
print("\n===== what the LDOS doc already stores for analyst_estimates + peer_comparison =====")
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
print("analyst_estimates:", json.dumps(d.get("analyst_estimates"))[:400])
pc=d.get("peer_comparison") or {}
print("peer_comparison keys:", list(pc.keys()), "| median:", json.dumps(pc.get("median") or pc.get("peer_median"))[:200])
print("DONE 2257")
