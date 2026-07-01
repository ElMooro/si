"""ops 2644 — verify the nav drawer is actually live and wired on real, varied pages."""
import urllib.request, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")

SAMPLE = ["deal-scanner.html","risk-regime.html","inflection.html","buybacks.html",
          "attention.html","intelligence.html","brain.html","screener/","directory.html",
          "system.html","crypto-liquidity.html","index.html"]
for f in SAMPLE:
    try:
        html = get(f"https://justhodl.ai/{f}?cb={int(time.time())}")
        has_tag = 'src="/jh-nav-drawer.js"' in html
        note = " (protected — intentionally excluded)" if f == "screener/" else ""
        print(f"  [{'OK' if (has_tag or f=='screener/') else 'MISS'}] {f:24s} drawer_tag={has_tag}{note}")
    except Exception as e:
        print(f"  [ERR] {f}: {str(e)[:60]}")
print("DONE 2644")
