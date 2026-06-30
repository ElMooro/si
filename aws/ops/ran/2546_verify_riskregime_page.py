"""ops 2546 — verify the upgraded risk-regime.html deployed + the feeds it needs are live."""
import urllib.request, json, time, boto3
time.sleep(60)
s3 = boto3.client("s3", "us-east-1")
# 1. page deployed with new sections?
url = "https://justhodl.ai/risk-regime.html"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
try:
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
    markers = {
        "money-flow section": "Cross-asset money flow" in html,
        "regime+cycle section": "Regime &amp; cycle context" in html or "Regime & cycle context" in html,
        "stress+plumbing section": "Systemic stress" in html,
        "foreign-capital section": "Foreign capital" in html,
        "vol-regime section": "Volatility regime" in html,
        "loads cross-asset-flow-state": "cross-asset-flow-state.json" in html,
        "loads crisis-composite": "crisis-composite.json" in html,
        "loads cycle-clock": "cycle-clock.json" in html,
        "14-engine footer": "fuses ~14 cross-asset engines" in html,
    }
    for k, v in markers.items():
        print(f"  {'✅' if v else '❌'} {k}")
    print("PAGE OK:", all(markers.values()))
except Exception as e:
    print("page fetch err:", str(e)[:120])

# 2. confirm all feeds the page reads are actually present in S3 (so sections populate)
feeds = ["risk-regime","polygon-fx-regime","cross-asset-flow-state","cross-asset-regime","dollar-radar",
         "gold-equity-rotation","capital-inflows","tic-flows","eurodollar-stress","eurodollar-plumbing",
         "vol-regime","crisis-composite","global-stress","cycle-clock","dark-pool","regime-map",
         "settlement-fails","sovereign-fiscal"]
present = 0
missing = []
for f in feeds:
    try:
        s3.head_object(Bucket="justhodl-dashboard-live", Key=f"data/{f}.json"); present += 1
    except Exception:
        missing.append(f)
print(f"\nfeeds live in S3: {present}/{len(feeds)}")
if missing:
    print("MISSING (sections will gracefully skip):", missing)
print("DONE 2546")
