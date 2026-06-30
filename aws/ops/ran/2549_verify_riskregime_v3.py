"""ops 2549 — verify the deep-expanded risk-regime page + all feeds present."""
import urllib.request, time, boto3
time.sleep(60)
s3 = boto3.client("s3", "us-east-1")
url = "https://justhodl.ai/risk-regime.html"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
try:
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
    secs = {s: (f'id="{s}"' in html) for s in
            ["credit","rates","liquidity","stress","cb","regime","foreign","vol"]}
    for k, v in secs.items():
        print(f"  {'✅' if v else '❌'} section #{k}")
    print("  40-engine footer:", "fuses ~40 cross-asset engines" in html)
    print("  no double-escape:", "&amp;amp;" not in html)
    print("SECTIONS OK:", all(secs.values()))
except Exception as e:
    print("page err:", str(e)[:120])
# feed availability for every key the page loads
feeds = ["risk-regime","polygon-fx-regime","cross-asset-flow-state","cross-asset-regime","dollar-radar",
         "capital-inflows","tic-flows","eurodollar-stress","eurodollar-plumbing","vol-regime","crisis-composite",
         "global-stress","cycle-clock","regime-map","settlement-fails","sovereign-fiscal","credit-stress",
         "credit-equity-divergence","cds-monitor","cds-proxy","yield-curve","move-index","bond-vol","auction-crisis",
         "auction-grades","vix-curve","global-liquidity","funding-plumbing","liquidity-pulse","repo-lending",
         "bank-stress","liquidity-inflection","systemic-stress","tail-risk","tail-hedge","yen-carry","boj-detail",
         "snb-detail","ecb-detail","crypto-stablecoin-peg","fx-decomposition","macro-nowcast","macro-surprise",
         "global-business-cycle","vvix-vov-regime","skew-tail-hedging","flow-confluence","institutional-positions"]
present, missing = 0, []
for f in feeds:
    try:
        s3.head_object(Bucket="justhodl-dashboard-live", Key=f"data/{f}.json"); present += 1
    except Exception:
        missing.append(f)
print(f"\nfeeds live in S3: {present}/{len(feeds)}")
if missing:
    print("MISSING (sections gracefully skip):", missing)
print("DONE 2549")
