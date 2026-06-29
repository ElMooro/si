import urllib.request, time
time.sleep(150)
req=urllib.request.Request("https://justhodl.ai/sector-flow.html",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Cache-Control":"no-cache","Pragma":"no-cache"})
html=urllib.request.urlopen(req,timeout=30).read().decode("utf-8","ignore")
for m in ["Cross-asset risk regime","Asset-class rotation","Dollar, FX & carry","Foreign money into US assets","DISTRIBUTION (dark volume","risk-regime.json","capital-inflows.json"]:
    print(f"  {'FOUND' if m in html else 'MISSING':7} {m}")
print("bytes:",len(html));print("DONE 2515")
