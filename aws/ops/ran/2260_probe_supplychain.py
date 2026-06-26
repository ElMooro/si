import json, urllib.request
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(c):
    u=f"https://financialmodelingprep.com/stable/{c}{'&' if '?' in c else '?'}apikey={FMP}"
    try:
        r=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=25).read())
        return r
    except urllib.error.HTTPError as e: return {"_http":e.code}
    except Exception as e: return {"_err":str(e)[:60]}
print("=== Revenue composition (the 'who pays them' / customer-mix visual) ===")
for c in ["revenue-product-segmentation?symbol=LDOS","revenue-geographic-segmentation?symbol=LDOS"]:
    r=get(c)
    if isinstance(r,list) and r:
        print(f"OK  {c.split('?')[0]:38} {len(r)} periods; latest sample:", json.dumps(r[0])[:260])
    else: print(f"--  {c.split('?')[0]:38}", json.dumps(r)[:90])
print("\n=== Supplier/customer graph endpoints (Bloomberg SPLC equivalent) ===")
for c in ["supply-chain?symbol=LDOS","stock-peers?symbol=LDOS","company-customers?symbol=LDOS","company-suppliers?symbol=LDOS"]:
    r=get(c)
    ok = isinstance(r,list) and r
    print(f"{'OK ' if ok else '-- '} {c.split('?')[0]:30}", (json.dumps(r[0])[:120] if ok else json.dumps(r)[:70]))
print("\n=== Ownership depth (Bloomberg HDS / 13F holders) ===")
for c in ["institutional-ownership/symbol?symbol=LDOS","institutional-ownership-extract-analytics/holder?symbol=LDOS&year=2026&quarter=1"]:
    r=get(c)
    ok=isinstance(r,list) and r
    print(f"{'OK ' if ok else '-- '} {c.split('?')[0]:48}", (list(r[0].keys())[:8] if ok else json.dumps(r)[:70]))
print("\n=== Debt maturity (Bloomberg DDIS) — is it in balance-sheet detail? ===")
r=get("balance-sheet-statement?symbol=LDOS&period=annual&limit=1")
if isinstance(r,list) and r:
    keys=[k for k in r[0] if any(w in k.lower() for w in ("debt","maturit","longterm","short","lease"))]
    print("debt-related BS fields:", keys)
print("DONE 2260")
