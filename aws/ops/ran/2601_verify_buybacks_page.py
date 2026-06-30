import urllib.request, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
html=get(f"https://justhodl.ai/buybacks.html?cb={int(time.time())}")
print("live buybacks.html status: 200 bytes:",len(html))
for mk in ["Corporate Buyback Board","High-Conviction Pump","Net Shrinkers","Dilution-Offset Warnings","data/buyback-engine.json","High Shareholder Yield","Cheap Repurchasers","Methodology"]:
    print(f"  [{'OK' if mk in html else 'MISS'}] {mk}")
print("DONE 2601")
