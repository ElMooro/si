"""ops 2658 — probe FMP /stable/ bulk screener capabilities: does it return sector/industry/
marketCap/growth in ONE call across a broad universe (needed to scale beyond 220 names without
blowing the Lambda timeout on N individual income-statement calls)? Also check ai-infra-stack.json
schema and whether a GICS-sector grouping already exists platform-wide."""
import urllib.request, json
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(path):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    try:
        return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh"}),timeout=20).read())
    except Exception as e:
        return {"__err__": str(e)[:150]}

print("=== 1) company-screener bulk fields (sector/industry/marketCap in one call?) ===")
d = get("company-screener?marketCapMoreThan=1000000000&limit=5")
if isinstance(d, list) and d:
    print("  count:", len(d), "| sample keys:", list(d[0].keys()))
    print("  sample row:", json.dumps(d[0], indent=1)[:500])
else:
    print("  RESULT:", str(d)[:300])

print("\n=== 2) does screener support a growth filter directly? ===")
d2 = get("company-screener?marketCapMoreThan=1000000000&sector=Technology&limit=3")
print("  by-sector filter works:", isinstance(d2, list) and len(d2) > 0)

print("\n=== 3) financial-growth endpoint (per-symbol, but check field names) ===")
d3 = get("financial-growth?symbol=MU&limit=2")
if isinstance(d3, list) and d3:
    print("  keys:", list(d3[0].keys())[:20])
    print("  revenueGrowth:", d3[0].get("revenueGrowth"))

print("\n=== 4) key-metrics or ratios bulk-ish endpoint for EV/Sales directly? ===")
d4 = get("key-metrics?symbol=MU&limit=2")
if isinstance(d4, list) and d4:
    print("  key-metrics keys:", list(d4[0].keys())[:25])

print("DONE 2658")
