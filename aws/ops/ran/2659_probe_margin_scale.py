"""ops 2659 — check margin field availability + screener max rows per call, to size the
broad-universe two-stage funnel (bulk sector-tag -> per-sector top-N -> per-symbol enrich)."""
import urllib.request, json
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(path):
    url = f"https://financialmodelingprep.com/stable/{path}{'&' if '?' in path else '?'}apikey={FMP}"
    try:
        return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh"}),timeout=20).read())
    except Exception as e:
        return {"__err__": str(e)[:150]}

print("=== ratios endpoint — operating margin field? ===")
d = get("ratios?symbol=MU&limit=1")
if isinstance(d, list) and d:
    print("  keys:", [k for k in d[0].keys() if "margin" in k.lower() or "Margin" in k])
    print("  operatingProfitMargin:", d[0].get("operatingProfitMargin"))

print("\n=== company-screener max rows in a single call ===")
d2 = get("company-screener?marketCapMoreThan=300000000&limit=3000&isActivelyTrading=true&country=US")
print("  rows returned for limit=3000:", len(d2) if isinstance(d2, list) else d2)

print("\n=== sector breakdown of that broad screen (real 'all sectors' coverage check) ===")
if isinstance(d2, list) and d2:
    from collections import Counter
    secs = Counter(r.get("sector") for r in d2 if r.get("sector") and not r.get("isEtf") and not r.get("isFund"))
    for s, n in secs.most_common(15): print(f"   {s}: {n}")
    print("  total non-ETF/fund:", sum(1 for r in d2 if not r.get('isEtf') and not r.get('isFund')))

print("\n=== ai-infra-stack.json layer list (for reference, to keep as special overlay) ===")
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
stack = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-infra-stack.json")["Body"].read())
layers = [l.get("layer") for l in stack.get("stack", [])]
print("  layers:", layers)
print("DONE 2659")
